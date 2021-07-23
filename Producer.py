import time
import pandas as pd
from kafka import KafkaProducer

def getSentences(file_path):
    df = pd.read_csv(file_path, sep='\n',encoding='gb18030')

    print(df.columns)
    sentences = df['neg']
    return sentences

def send_data(sentences,bootsrap_server):
    producer = KafkaProducer(bootstrap_servers = bootsrap_server)
    for s in sentences:
        print('send: ', s)
        producer.send('sentence', s.encode('utf-8'))
        time.sleep(5)


if __name__ == '__main__':
    file_path = './clothing/neg.txt'
    sentences = getSentences(file_path)
    bootstrap_server = '10.8.12.208:9092'
    send_data(sentences, bootstrap_server)

