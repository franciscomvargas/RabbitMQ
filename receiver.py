import pika
import ast

class MetaClass(type):
    _instance = {}
    def __call__(cls, *args, **kwargs):
        ''' Singelton Design Pattern '''
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
            return cls._instance[cls]

class RabbitMQ_Configure(metaclass=MetaClass):
    def __init__(self, queue='hello', host='localhost'):
        ''' Configure Rabbit MQ Server '''
        self.queue = queue
        self.host= host

class RabbitMQ_Client():
    def __init__(self, server):
        '''
        :param server: Object of class RabbitMQ_Configure
        '''
        self.server = server
        self._pikaConnection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server.host)
        )
        self._pikaChannel = self._pikaConnection.channel()
        self._tem = self._pikaChannel.queue_declare(queue=self.server.queue)
    
    def callback(self, ch, method, properties, body):
        payload = ast.literal_eval(body.decode("utf-8"))
        print(f' [x] Received {payload}')
    
    def startClient(self):
        self._pikaChannel.basic_consume(
            queue=self.server.queue,
            on_message_callback=self.callback,
            auto_ack=True
        )
        print(' [*] Waiting for messages. To exit press CTRL+C')

        self._pikaChannel.start_consuming()



if __name__ == '__main__':
    serverconfigure = RabbitMQ_Configure(
        queue='hello', 
        host='localhost'
    )
    client = RabbitMQ_Client(server=serverconfigure)
    client.startClient()