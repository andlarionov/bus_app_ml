import pika
import pickle
import numpy as np
import json
import time

# Читаем файл с сериализованной моделью
with open('myfile.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)

# Попытки подключения к RabbitMQ
while True:
    try:
        # Создаём подключение по адресу rabbitmq с портом 5672
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', port=5672)
        )
        channel = connection.channel()
        break  # Успешное подключение
    except pika.exceptions.AMQPConnectionError:
        print("Не удалось подключиться к очереди. Повтор через 5 секунд.")
        time.sleep(5)

# Объявляем очередь features
channel.queue_declare(queue='features')
# Объявляем очередь y_pred
channel.queue_declare(queue='y_pred')

# Создаём функцию callback для обработки данных из очереди
def callback(ch, method, properties, body):
    print(f'Получен вектор признаков {body}')
    
    # Декодируем сообщение из JSON формата
    features = json.loads(body)
    
    # Извлекаем вектор признаков из словаря
    feature_vector = features['body']  # Предполагается, что 'body' содержит список чисел
    
    # Преобразуем вектор признаков в массив numpy для передачи в predict
    pred = regressor.predict(np.array(feature_vector).reshape(1, -1))
    
    # Формируем сообщение для отправки предсказания
    message_y_pred = {
        'id': features['id'],  # Используем тот же ID, что и у входных данных
        'body': pred[0]  # Предсказание модели
    }
    
    channel.basic_publish(
        exchange='',
        routing_key='y_pred',
        body=json.dumps(message_y_pred)  # Сериализуем предсказание как JSON
    )
    
    print(f'Предсказание {pred[0]} отправлено в очередь y_pred')

# Извлекаем сообщение из очереди features
channel.basic_consume(
    queue='features',
    on_message_callback=callback,
    auto_ack=True
)

print('...Ожидание сообщений')

# Запускаем режим ожидания прихода сообщений
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Завершение работы.")
