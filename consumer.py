from confluent_kafka import Consumer
import sys

if __name__ == '__main__':

    config = { # Define oa parâmetros de config necessários
        
        'bootstrap.servers': 'localhost:9092', # Endereço onde o kafka está hospedado
        'group.id' : 'eabara' # Config do consumer que identifica a que grupo ele pertence
    }


    consumer = Consumer(config) # Instãncia

    # Se inscerve ao topico = topic
    topic = "mensagem"
    consumer.subscribe([topic])

    # Pesquisa pelas mensagens
    try:
        while True:
            msg = consumer.poll(1.0) # consumet.poll(timeout); consome uma mensagem, chama o callback e retorna um evento
            if msg is None:
                print("Aguardando Mensagem...")
            elif msg.error():
                print("ERRO: %s".format(msg.error()))
            else:
                # Printa a mensagem e a chave
                print("Mensagem recebida do tópico {topic}: Valor = {value:12}".format(
                    topic=msg.topic(), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close() # Fecha o consumer desinscrevendo dos tópicos