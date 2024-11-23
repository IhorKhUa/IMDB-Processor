import gzip
import pandas as pd
import numpy as np

file1 = "/home/ihor/load_base/title.basics.tsv.gz"
file2 = "/home/ihor/load_base/title.ratings.tsv.gz"


# Функция для чтения и распаковки .gz файлов
def extract_gz(file_path):
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            lines = [next(f) for _ in range(5)]
            print(f"Первые строки из файла {file_path}:")
            print("".join(lines))
            print("=" * 50)
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except Exception as e:
        print(f"Ошибка при чтении {file_path}: {e}")


extract_gz(file1)
extract_gz(file2)


# Функция для загрузки TSV файлов в DataFrame
def load_tsv(file_path):
    dtype = {
        'tconst': 'str',
        'titleType': 'str',
        'primaryTitle': 'str',
        'originalTitle': 'str',
        'isAdult': 'str',
        'startYear': 'float',
        'endYear': 'float',
        'runtimeMinutes': 'float',
        'genres': 'str'
    }

    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            # Заменяем '\N' на np.nan
            df = pd.read_csv(f, sep='\t', dtype=dtype, na_values={'\\N': np.nan}, low_memory=False)

            # Преобразуем столбцы в числовой формат, если это возможно
            df['isAdult'] = pd.to_numeric(df['isAdult'], errors='coerce')
            df['runtimeMinutes'] = pd.to_numeric(df['runtimeMinutes'], errors='coerce')
            df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce')
            df['endYear'] = pd.to_numeric(df['endYear'], errors='coerce')

            print(f"Пример данных из {file_path}:")
            # Устанавливаем опции отображения для более читаемого вывода
            pd.set_option('display.max_columns', None)  # Показывать все столбцы
            pd.set_option('display.width', 1000)  # Установить ширину для длинных таблиц
            pd.set_option('display.max_rows', 10)  # Отобразить только первые 10 строк
            print(df.head())
            return df
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except Exception as e:
        print(f"Ошибка при загрузке {file_path}: {e}")


# Загрузка данных
df1 = load_tsv(file1)
df2 = load_tsv(file2)
################################################################################


#############################################
import os
import zipfile
import shutil

# Пути для папок
raw_dir = "raw"
bronze_dir = "bronze"
silver_dir = "silver"
gold_dir = "gold"

# Создание папок
for directory in [raw_dir, bronze_dir, silver_dir, gold_dir]:
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Папка '{directory}' была создана.")
    else:
        print(f"Папка '{directory}' уже существует.")

# Пример распаковки архивов
file1 = "/home/ihor/load_base/title.basics.tsv.gz"
file2 = "/home/ihor/load_base/title.ratings.tsv.gz"

# Функция для распаковки архивов
def extract_file(file_path, destination_dir):
    if file_path.endswith('.gz'):
        # Если архив .gz, распаковываем его
        with open(destination_dir, 'wb') as out_file:
            with open(file_path, 'rb') as f_in:
                shutil.copyfileobj(f_in, out_file)
        print(f"Файл {file_path} был распакован в {destination_dir}")
    else:
        print(f"Файл {file_path} не является архивом .gz")
##################################################################
