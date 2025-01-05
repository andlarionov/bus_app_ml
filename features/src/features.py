import pika
import numpy as np
import json
import time
from datetime import datetime
from sklearn.datasets import load_diabetes
import pandas as pd

# Загружаем датасет о диабете
X, y = load_diabetes(return_X_y=True)

# Инициализация DataFrame для хранения метрик
metrics_df = pd.DataFrame(columns=['id', 'y_true', 'y_pred', 'absolute_error'])

def connect_to_rabbitmq():
    """Подключение к RabbitMQ с повторами в случае ошибки."""
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='rabbitmq', port=5672)
            )
            channel = connection.channel()
            print("Подключение к RabbitMQ успешно установлено.")
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Не удалось подключиться к очереди: {e}. Повтор через 5 секунд.")
            time.sleep(5)

# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Формируем случайный индекс строки
        random_row = np.random.randint(0, X.shape[0]-1)

        # Генерируем уникальный идентификатор на основе текущего времени
        message_id = datetime.timestamp(datetime.now())

        # Создаём подключение по адресу rabbitmq:
        connection, channel = connect_to_rabbitmq()

        # Создаём очередь y_true и features
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='features')

        # Публикуем сообщение в очередь y_true с уникальным ID
        message_y_true = {
            'id': message_id,
            'body': y[random_row]
        }
        channel.basic_publish(exchange='',
                              routing_key='y_true',
                              body=json.dumps(message_y_true))
        print('Сообщение с правильным ответом отправлено в очередь')

        # Публикуем сообщение в очередь features с уникальным ID
        message_features = {
            'id': message_id,
            'body': list(X[random_row])
        }
        channel.basic_publish(exchange='',
                              routing_key='features',
                              body=json.dumps(message_features))
        print('Сообщение с вектором признаков отправлено в очередь')

        # Закрываем подключение
        connection.close()

        # Задержка перед следующей итерацией
        time.sleep(10)
    except Exception as e:
        print(f'Не удалось подключиться к очереди: {e}')