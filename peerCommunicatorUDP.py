from socket  import *
from constMP import * #-
import threading
import random
import time
import pickle
from requests import get

# --- Lógica do Relógio de Lamport e Fila ---
# Relógio lógico de Lamport
lamport_clock = 0
# Fila para armazenar mensagens recebidas antes de serem entregues (Total Order)
# Formato do item: {'content': (remetente, msg_num), 'timestamp': ts, 'sender_id': id, 'acks': set()}
message_queue = []
# Lock para acesso concorrente ao relógio e à fila de mensagens
queue_lock = threading.Lock()
# --- Fim da Lógica de Lamport ---

# Contador para garantir que recebemos handshakes de todos os outros processos
handShakeCount = 0

PEERS = []

# Sockets UDP para enviar e receber mensagens de dados:
# Criar socket de envio
sendSocket = socket(AF_INET, SOCK_DGRAM)
#Criar e fazer bind do socket de receção
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# Socket TCP para receber sinal de início do servidor de comparação:
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)


def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('O meu endereço IP público é: {}'.format(ipAddr))
  return ipAddr

# Função para registar este peer com o gestor de grupo
def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('A ligar ao gestor de grupo: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  print ('A registar no gestor de grupo: ', req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('A ligar ao gestor de grupo: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  print ('A obter a lista de peers do gestor de grupo: ', req)
  clientSock.send(msg)
  msg = clientSock.recv(2048)
  PEERS = pickle.loads(msg)
  print ('Lista de peers obtida: ', PEERS)
  clientSock.close()
  return PEERS

class MsgHandler(threading.Thread):
  def _init_(self, sock, myself, peers_list):
    threading.Thread._init_(self)
    self.sock = sock
    self.myself = myself
    self.peers = peers_list
    self.logList = []

  def deliver_messages(self):
    """Verifica a fila e entrega mensagens que estão prontas (no topo e com todos os ACKs)."""
    global message_queue
    while True:
        if not message_queue:
            break
        
        # Ordena a fila por (timestamp, sender_id) para garantir a ordem total
        message_queue.sort(key=lambda m: (m['timestamp'], m['sender_id']))
        
        head_message = message_queue[0]
        
        # Verifica se a mensagem no topo da fila recebeu ACK de todos os peers
        if len(head_message['acks']) == N:
            delivered_msg = message_queue.pop(0)
            
            # --- Lógica de Conversa (agora na entrega) ---
            peer_id = delivered_msg['sender_id']
            msg_content = delivered_msg['content'] # Tuplo (frase, número)
            
            # A lógica de conversa pode ser baseada no número da mensagem original
            msg_topic_id = msg_content[1] 

            conversation_replies = {
                0: ["Olá, Peer {peer_id}! Tudo bem por aqui.", "E aí, {peer_id}! Recebido."],
                1: ["Hmm, que pergunta interessante, {peer_id}.", "Boa pergunta, {peer_id}!"],
                2: ["Hahaha, essa foi boa, {peer_id}!", "{peer_id}, sempre com as melhores piadas."],
            }
            default_replies = ["Interessante o que dizes, {peer_id}.", "Ok, {peer_id}, a processar."]
            
            replies_list = conversation_replies.get(msg_topic_id, default_replies)
            selected_reply = random.choice(replies_list).format(peer_id=peer_id)
            
            original_starter_phrase = msg_content[0]

            print(f"[Peer {self.myself} viu] Peer {peer_id} iniciou '{original_starter_phrase}' (T:{delivered_msg['timestamp']})")
            print(f"    -> [Peer {self.myself}] pensa: \"{selected_reply}\"")
            
            self.logList.append(delivered_msg)
        else:
            # Se a mensagem no topo não está pronta, não podemos entregar mais nada
            break


  def run(self):
    print('O handler está pronto. A aguardar pelos handshakes...')
    
    global handShakeCount, lamport_clock
    
    # Esperar até que os handshakes sejam recebidos de todos os outros processos
    while handShakeCount < N:
      msgPack, addr = self.sock.recvfrom(1024)
      msg = pickle.loads(msgPack)
      if msg[0] == 'READY':
        handShakeCount = handShakeCount + 1
        print('--- Handshake recebido de: ', msg[1])

    print('Thread Secundária: Recebidos todos os handshakes. A entrar no loop para receber mensagens.')

    stopCount=0 
    while True:                
      msgPack, addr = self.sock.recvfrom(2048)   # receber dados do cliente
      msg = pickle.loads(msgPack)
      
      with queue_lock:
        # Atualiza o relógio de Lamport ao receber qualquer mensagem com timestamp
        if isinstance(msg, tuple) and len(msg) > 2 and isinstance(msg[2], int):
            lamport_clock = max(lamport_clock, msg[2]) + 1

        if msg[0] == 'DATA': # Formato: ('DATA', (starter_phrase, msgNumber), timestamp, sender_id)
            _, content, timestamp, sender_id = msg
            
            # Adiciona a mensagem à fila
            new_msg_item = {'content': content, 'timestamp': timestamp, 'sender_id': sender_id, 'acks': {self.myself}}
            message_queue.append(new_msg_item)
            
            # Envia ACK para todos
            lamport_clock += 1
            ack_msg = ('ACK', timestamp, sender_id, lamport_clock, self.myself)
            ack_pack = pickle.dumps(ack_msg)
            for peer_addr in self.peers:
                sendSocket.sendto(ack_pack, (peer_addr, PEER_UDP_PORT))

        elif msg[0] == 'ACK': # Formato: ('ACK', orig_ts, orig_sender, ack_ts, acker_id)
            _, orig_ts, orig_sender, _, acker_id = msg
            
            # Encontra a mensagem correspondente na fila e adiciona o ACK
            for m in message_queue:
                if m['timestamp'] == orig_ts and m['sender_id'] == orig_sender:
                    m['acks'].add(acker_id)
                    break

        elif msg[0] == -1:   # contar as mensagens 'stop' dos outros processos
            stopCount = stopCount + 1
            if stopCount == N:
                break
        
        # Tenta entregar mensagens após cada evento
        self.deliver_messages()

    # Escrever ficheiro de log
    logFile = open('logfile'+str(self.myself)+'.log', 'w')
    # O logList já estará ordenado devido à entrega ordenada
    logFile.writelines(str(self.logList))
    logFile.close()
    
    print('A enviar o ficheiro de log ordenado para o servidor...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(self.logList)
    clientSock.send(msgPack)
    clientSock.close()
    
    exit(0)

def waitToStart():
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Processo peer '+str(myself)+' iniciado.'))
  conn.close()
  return (myself,nMsgs)

# A partir daqui, o código é executado quando o programa arranca:
registerWithGroupManager()
while 1:
  print('A aguardar sinal para começar...')
  (myself, nMsgs) = waitToStart()
  print('Estou ativo, e o meu ID é: ', str(myself))

  if nMsgs == 0:
    print('A terminar.')
    exit(0)

  time.sleep(5)

  PEERS = getListOfPeers()
  
  # Criar handler de receção de mensagens
  msgHandler = MsgHandler(recvSocket, myself, PEERS)
  msgHandler.start()
  print('Handler iniciado')
  
  # Enviar handshakes
  for addrToSend in PEERS:
    print('A enviar handshake para ', addrToSend)
    msg = ('READY', myself)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

  print('Thread Principal: Enviei todos os handshakes. handShakeCount=', str(handShakeCount))

  while (handShakeCount < N):
    pass  # Espera ativa pelos handshakes

  # --- MODIFICAÇÃO PARA CONVERSA COM ORDEM TOTAL ---
  conversation_starters = [
    "Olá a todos! Alguém na escuta?",
    "Vou lançar uma pergunta no ar para reflexão.",
    "Alguém aí conhece uma boa piada?",
    "Que tal falarmos sobre o tempo?",
    "A iniciar uma nova ronda de discussões."
  ]

  # Enviar uma sequência de mensagens de dados para todos os outros processos 
  for msgNumber in range(0, nMsgs):
    time.sleep(random.randrange(100,500)/1000)
    
    with queue_lock:
        # Incrementar o relógio ANTES de enviar
        lamport_clock += 1
        
        starter_index = msgNumber % len(conversation_starters)
        starter_phrase = conversation_starters[starter_index]
        
        # A mensagem agora contém o tipo, o conteúdo, o timestamp e o ID do remetente
        content = (starter_phrase, msgNumber)
        msg = ('DATA', content, lamport_clock, myself)
        msgPack = pickle.dumps(msg)
    
    # Envia a mensagem para todos os peers (multicast)
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
      
    print(f"[Peer {myself}] enviou: \"{starter_phrase}\" (Tópico #{msgNumber}) com Timestamp: {msg[2]}")
  # --- FIM DA MODIFICAÇÃO ---


  # Informar todos os processos que não tenho mais mensagens para enviar
  for addrToSend in PEERS:
    msg = (-1,-1)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))