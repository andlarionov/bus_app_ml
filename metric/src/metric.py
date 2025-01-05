import pika
import json
import pandas as pd
import time
import os

# Создаем директорию logs, если она не существует
if not os.path.exists('./logs'):
    os.makedirs('./logs')

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

def calculate_absolute_error(message_id):
    """Вычисляет абсолютную ошибку для заданного ID."""
    global metrics_df
    row = metrics_df[metrics_df['id'] == message_id]
    
    if not row.empty and row['y_true'].notnull().any() and row['y_pred'].notnull().any():
        absolute_error = abs(row['y_true'].values[0] - row['y_pred'].values[0])
        metrics_df.loc[metrics_df['id'] == message_id, 'absolute_error'] = absolute_error
        
        # Записываем данные в CSV файл
        metrics_df.to_csv('./logs/metric_log.csv', index=False)

def callback_y_true(ch, method, properties, body):
    """Обработка сообщений из очереди y_true."""
    global metrics_df
    message = json.loads(body)
    message_id = message['id']
    y_true = message['body']
    
    metrics_df.loc[metrics_df.shape[0]] = [message_id, y_true, None, None]
    
    if not metrics_df[metrics_df['id'] == message_id]['y_pred'].isnull().all():
        calculate_absolute_error(message_id)

def callback_y_pred(ch, method, properties, body):
    """Обработка сообщений из очереди y_pred."""
    global metrics_df
    message = json.loads(body)
    message_id = message['id']
    y_pred = message['body']
    
    metrics_df.loc[metrics_df.shape[0]] = [message_id, None, y_pred, None]
    
    if not metrics_df[metrics_df['id'] == message_id]['y_true'].isnull().all():
        calculate_absolute_error(message_id)

# Подключение к RabbitMQ
connection, channel = connect_to_rabbitmq()

# Объявление очередей
channel.queue_declare(queue='y_true')
channel.queue_declare(queue='y_pred')

# Настройка обработки очередей
channel.basic_consume(queue='y_true', on_message_callback=callback_y_true, auto_ack=True)
channel.basic_consume(queue='y_pred', on_message_callback=callback_y_pred, auto_ack=True)

print('...Ожидание сообщений')
channel.start_consuming()