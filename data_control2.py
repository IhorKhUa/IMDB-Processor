import pandas as pd

# Путь к файлу title.ratings.tsv
ratings_file = 'bronze/title.ratings.tsv'

# Чтение файла
try:
    ratings_df = pd.read_csv(ratings_file, sep='\t', low_memory=False)

    # Проверяем наличие столбца 'averageRating'
    if 'averageRating' in ratings_df.columns:
        min_rating = ratings_df['averageRating'].min()
        max_rating = ratings_df['averageRating'].max()
        print(f"Диапазон значений averageRating: от {min_rating} до {max_rating}")
    else:
        print("Столбец 'averageRating' отсутствует в данных.")
except Exception as e:
    print(f"Ошибка при обработке файла: {e}")

