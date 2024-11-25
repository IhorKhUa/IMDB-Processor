# Связанная задача: IMDB-1
# Подробнее: https://github.com/<ваш_репозиторий>/issues/8

import os
import pandas as pd

# Папки для хранения данных
raw_dir = "raw"
bronze_dir = "bronze"
silver_dir = "silver"
gold_dir = "Gold_but_empty"  # Папка, где будут оставаться пустыми JSON файлы

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
        print(f"Чтение данных из {file_path}...")

        try:
            # Чтение данных в DataFrame
            df = pd.read_csv(file_path, sep='\t', low_memory=False)  # Используем sep='\t' для .tsv файлов
            print(f"Данные успешно загружены из {file_path}.")

            # Пример обработки данных: добавим столбец с количеством строк
            df['row_count'] = df.shape[0]

            # Приведение типов для возможных смешанных данных (если необходимо)
            if df.shape[1] > 4:  # Проверяем, что есть хотя бы 5 столбцов
                df.iloc[:, 4] = df.iloc[:, 4].astype(str)  # Приводим 4-й столбец к строковому типу
                print("Типы данных в столбце 4 успешно приведены к строковому типу.")

            # Сохранение в папку silver как CSV
            silver_file_path = os.path.join(silver_dir, file_name.replace('.tsv', '.csv'))
            df.to_csv(silver_file_path, index=False)
            print(f"Данные сохранены в {silver_file_path}.")

            # Преобразование в JSON и сохранение в gold (файлы не создаются, папка остается пустой)
            gold_file_path = os.path.join(gold_dir, file_name.replace('.tsv', '.json'))
            # Не сохраняем JSON в 'Gold_but_empty' папку
            print(f"JSON файлы не создаются, так как папка 'Gold_but_empty' остается пустой.")

        except Exception as e:
            print(f"Ошибка при обработке файла {file_name}: {e}")

            # Сообщаем, что обработка завершена
            print(f"Данные из {file_name} успешно обработаны и сохранены в папку 'silver'.")

        except Exception as e:
            print(f"Ошибка при обработке файла {file_name}: {e}")

