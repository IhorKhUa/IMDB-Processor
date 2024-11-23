import os
import gzip
import shutil
import pandas as pd
import json

# Папки для хранения данных
raw_dir = "raw"
bronze_dir = "bronze"
silver_dir = "silver"
gold_dir = "gold"

# Создание папок, если их нет
for directory in [raw_dir, bronze_dir, silver_dir, gold_dir]:
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Папка '{directory}' была создана.")
    else:
        print(f"Папка '{directory}' уже существует.")


# Функция для распаковки файлов .gz
def extract_gz_file(source_path, dest_path):
    with gzip.open(source_path, 'rb') as f_in:
        with open(dest_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Файл {source_path} распакован в {dest_path}")


# Распаковка файлов из папки raw в папку bronze
for file_name in os.listdir(raw_dir):
    if file_name.endswith('.gz'):
        source_file = os.path.join(raw_dir, file_name)
        dest_file = os.path.join(bronze_dir, file_name[:-3])  # Убираем .gz из имени файла
        extract_gz_file(source_file, dest_file)

# Пример обработки данных из папки bronze и сохранения в silver и gold
for file_name in os.listdir(bronze_dir):
    if file_name.endswith('.csv'):
        file_path = os.path.join(bronze_dir, file_name)
        print(f"Чтение данных из {file_path}")

        # Чтение данных в DataFrame
        df = pd.read_csv(file_path)

        # Пример простой обработки: добавим столбец с количеством строк
        df['row_count'] = df.shape[0]

        # Сохранение в папку silver как CSV
        silver_file_path = os.path.join(silver_dir, file_name)
        df.to_csv(silver_file_path, index=False)
        print(f"Данные сохранены в {silver_file_path}")

        # Преобразование в JSON и сохранение в gold
        gold_file_path = os.path.join(gold_dir, file_name.replace('.csv', '.json'))
        df.to_json(gold_file_path, orient='records', lines=True)
        print(f"Данные сохранены в {gold_file_path}")
