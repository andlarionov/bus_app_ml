import pandas as pd
import matplotlib.pyplot as plt
import time
import os

# Путь к файлу metric_log.csv
log_file_path = '../metric/logs/metric_log.csv'  # Указываем путь к файлу

def plot_error_distribution():
    while True:
        # Проверяем, существует ли файл
        if os.path.exists(log_file_path):
            # Читаем данные из CSV-файла
            data = pd.read_csv(log_file_path)

            # Проверяем, есть ли данные для построения графика
            if not data.empty and 'absolute_error' in data.columns:
                # Строим гистограмму абсолютных ошибок
                plt.figure(figsize=(10, 6))
                plt.hist(data['absolute_error'], bins=30, color='blue', alpha=0.7)
                plt.title('Распределение абсолютных ошибок')
                plt.xlabel('Абсолютная ошибка')
                plt.ylabel('Частота')
                
                # Сохраняем график в файл
                plt.savefig('logs/error_distribution.png')
                plt.close()
        
        # Задержка перед следующей итерацией
        time.sleep(10)

if __name__ == "__main__":
    plot_error_distribution()