import pika

class MetaClass(type):
    _instance = {}
    def __call__(cls, *args, **kwargs):
        ''' Singelton Design Pattern '''
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
            return cls._instance[cls]

class RabbitMQ_Configure(metaclass=MetaClass):
    def __init__(self, queue='hello', host='localhost', routingKey='hello', exchange=''):
        ''' Configure Rabbit MQ Server '''
        self.queue = queue
        self.host= host
        self.routingKey = routingKey
        self.exchange = exchange

class RabbitMQ_Server():

    __slots__ = ["server", "_pikaConnection", "_pikaChannel"]

    def __init__(self, server):
        '''
        :param server: Object of class RabbitMQ_Configure
        '''
        self.server = server
        self._pikaConnection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server.host)
        )
        self._pikaChannel = self._pikaConnection.channel()
        self._pikaChannel.queue_declare(queue=self.server.queue)

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_val, exc_tb):
        self._pikaConnection.close()

    def publish(self, payload = {}):
        '''
        :param payload: JSON payload
        :return: None
        '''
        self._pikaChannel.basic_publish(
            exchange=self.server.exchange,
            routing_key=self.server.routingKey,
            body=str(payload)
        )
        print(f"Published message: {payload}")

if __name__ == "__main__":
    server = RabbitMQ_Configure(
        queue='hello', 
        host='localhost', 
        routingKey='hello', 
        exchange=''
    )
    with RabbitMQ_Server(server) as rabbitmq:
        rabbitmq.publish(payload={'Data':'Hello World!'})
