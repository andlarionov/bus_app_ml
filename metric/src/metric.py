import pika
import json
import pandas as pd
import os

# Создание папки logs, если она не существует
os.makedirs('logs', exist_ok=True)

# Запись заголовков в файл metric_log.csv
log_file_path = 'logs/metric_log.csv'
with open(log_file_path, 'w') as f:
    f.write('id,y_true,y_pred,absolute_error\n')

# Инициализация DataFrame для хранения метрик
metrics_df = pd.DataFrame(columns=['id', 'y_true', 'y_pred'])

try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
   
    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')

    # Функция для обработки сообщений из очереди y_true
    def process_y_true(ch, method, properties, body):
        global metrics_df
        message = json.loads(body)
        metrics_df = metrics_df.append({'id': message['id'], 'y_true': message['body']}, ignore_index=True)
        calculate_absolute_error(message['id'])

    # Функция для обработки сообщений из очереди y_pred
    def process_y_pred(ch, method, properties, body):
        global metrics_df
        message = json.loads(body)
        metrics_df.loc[metrics_df['id'] == message['id'], 'y_pred'] = message['body']
        calculate_absolute_error(message['id'])

    # Функция для вычисления абсолютной ошибки и записи в CSV
    def calculate_absolute_error(message_id):
        global metrics_df

        if message_id in metrics_df['id'].values:
            row = metrics_df[metrics_df['id'] == message_id]
            if not row['y_true'].isnull().any() and not row['y_pred'].isnull().any():
                y_true = row['y_true'].values[0]
                y_pred = row['y_pred'].values[0]
                absolute_error = abs(y_true - y_pred)

                # Запись данных в CSV-файл
                with open(log_file_path, 'a') as f:
                    f.write(f'{message_id},{y_true},{y_pred},{absolute_error}\n')

                # Удаление строки из DataFrame после записи (по желанию)
                metrics_df.drop(metrics_df[metrics_df['id'] == message_id].index, inplace=True)

    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=process_y_true,
        auto_ack=True
    )
    
    # Извлекаем сообщение из очереди y_pred
    channel.basic_consume(
        queue='y_pred',
        on_message_callback=process_y_pred,
        auto_ack=True
    )

    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
except Exception as e:
    print(f'Не удалось подключиться к очереди: {e}')