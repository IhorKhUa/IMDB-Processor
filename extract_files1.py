import os

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