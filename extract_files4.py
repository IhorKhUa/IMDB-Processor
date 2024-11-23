import os
import pandas as pd

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

# Обработка файлов в папке bronze
for file_name in os.listdir(bronze_dir):
    if file_name.endswith('.tsv'):
        file_path = os.path.join(bronze_dir, file_name)
        print(f"Чтение данных из {file_path}")

        try:
            # Чтение данных в DataFrame
            df = pd.read_csv(file_path, sep='\t', low_memory=False)  # Используем sep='\t' для .tsv файлов

            # Пример обработки данных: добавим столбец с количеством строк
            df['row_count'] = df.shape[0]

            # Приведение типов для возможных смешанных данных (если необходимо)
            if df.shape[1] > 4:  # Проверяем, что есть хотя бы 5 столбцов
                df.iloc[:, 4] = df.iloc[:, 4].astype(str)  # Приводим 4-й столбец к строковому типу

            # Сохранение в папку silver как CSV
            silver_file_path = os.path.join(silver_dir, file_name.replace('.tsv', '.csv'))
            df.to_csv(silver_file_path, index=False)
            print(f"Данные сохранены в {silver_file_path}")

            # Преобразование в JSON и сохранение в gold
            gold_file_path = os.path.join(gold_dir, file_name.replace('.tsv', '.json'))
            df.to_json(gold_file_path, orient='records', lines=True)
            print(f"Данные сохранены в {gold_file_path}")

        except Exception as e:
            print(f"Ошибка при обработке файла {file_name}: {e}")


