#!/usr/bin/env python

from random import choice
from confluent_kafka import Producer

if __name__ == '__main__':

    config = {
        'bootstrap.servers': 'localhost:9092'
        
    }

    
    producer = Producer(config)
    
    def mensagem():
        msg = input("Digite a mensagem que deseja produzir para o consumer -> ")
        return msg

    # Função retirada do doc "confluent-kafka"
    # o callback serve como uma confirmação de envio ou fala no envio da mensagem
    def delivery_callback(err, msg):
        if err:
            print('ERRO: Falha no envio da mensagem: {}'.format(err))
        else:
            print("Mensagem produzida para o topico {topic}: value = {value:12}".format(
                topic=msg.topic(), value=msg.value().decode('utf-8')))

    topic = "mensagem"

    count = 0
    for _ in range(10):
        producer.produce(topic, mensagem(), callback=delivery_callback)
        count += 1

    
    producer.poll(10000) # Verifica e aciona o callback de cada mensagem
    producer.flush() # Aguarda a entrega de todas as mensagens na fila