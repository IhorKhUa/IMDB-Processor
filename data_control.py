import pandas as pd

# Загрузка данных
file_path = "bronze/title.basics.tsv"  # Укажите путь к вашему файлу
df = pd.read_csv(file_path, sep='\t', low_memory=False)

# Вывод уникальных значений в столбце titleType
unique_title_types = df['titleType'].unique()
print("Уникальные значения в столбце titleType:")
print(unique_title_types)