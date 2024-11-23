import os
import zipfile
import gzip
import shutil
import pandas as pd
import json

# Путь к исходным архивам
file1 = "/home/ihor/load_base/title.basics.tsv.gz"
file2 = "/home/ihor/load_base/title.ratings.tsv.gz"

# Папки для сохранения данных
raw_dir = "raw"
bronze_dir = "bronze"
silver_dir = "silver"
gold_dir = "gold"

# Функция для распаковки .gz архива
def unpack_gz(file_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, os.path.basename(file_path).replace(".gz", ""))
    with gzip.open(file_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return output_path

# Функция для загрузки данных из .tsv и преобразования в DataFrame
def load_data(file_path):
    return pd.read_csv(file_path, sep='\t')

# Функция для преобразования DataFrame в формат JSON
def save_json(df, output_path):
    df.to_json(output_path, orient="records", lines=True)

# Шаг 1: Загрузка и распаковка архивов в Raw
os.makedirs(raw_dir, exist_ok=True)
shutil.copy(file1, raw_dir)
shutil.copy(file2, raw_dir)

# Шаг 2: Распаковка архивов в Bronze
file1_unpacked = unpack_gz(file1, bronze_dir)
file2_unpacked = unpack_gz(file2, bronze_dir)

# Шаг 3: Загрузка данных в DataFrame и сохранение в JSON (Silver)
df1 = load_data(file1_unpacked)
df2 = load_data(file2_unpacked)

# Очистка данных, например, убираем строки с отсутствующими значениями
df1_cleaned = df1.dropna()
df2_cleaned = df2.dropna()

# Сохраняем очищенные данные в формате JSON в папку Silver
os.makedirs(silver_dir, exist_ok=True)
save_json(df1_cleaned, os.path.join(silver_dir, "title_basics.json"))
save_json(df2_cleaned, os.path.join(silver_dir, "title_ratings.json"))

# Шаг 4: Слияние данных и подготовка финального набора (Gold)
merged_df = pd.merge(df1_cleaned, df2_cleaned, on='tconst', how='inner')

# Преобразуем объединённые данные в JSON формат
os.makedirs(gold_dir, exist_ok=True)
save_json(merged_df, os.path.join(gold_dir, "merged_data.json"))

print("Данные успешно обработаны и сохранены в папки raw, bronze, silver, gold.")
i