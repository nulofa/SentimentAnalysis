from kafka import KafkaConsumer

def consumeData(bootstrap_server, topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_server,
                                auto_offset_reset='earliest')

    for msg in consumer:
        # print(msg)
        print(msg.value.decode('utf-8'))

if __name__ == '__main__':
    bootstrap_server = '10.8.12.208'
    topic = 'sentence'
    consumeData(bootstrap_server,topic)