import os
import pandas as pd

# Папки для хранения данных
bronze_dir = "bronze"
silver_dir = "silver"

# Создание папки silver, если её нет
if not os.path.exists(silver_dir):
    os.makedirs(silver_dir)
    print(f"Папка '{silver_dir}' была создана.")
else:
    print(f"Папка '{silver_dir}' уже существует.")

# Пути к файлам
ratings_file = os.path.join(bronze_dir, "title.ratings.tsv")
basics_file = os.path.join(bronze_dir, "title.basics.tsv")

try:
    # Чтение данных
    print(f"Чтение данных из {ratings_file}...")
    ratings_df = pd.read_csv(ratings_file, sep='\t', low_memory=False)
    print("Данные о рейтингах успешно загружены.")

    print(f"Чтение данных из {basics_file}...")
    basics_df = pd.read_csv(basics_file, sep='\t', low_memory=False)
    print("Данные о базовых характеристиках успешно загружены.")

    # Объединение данных по ключу 'tconst'
    print("Объединение данных...")
    merged_df = pd.merge(basics_df, ratings_df, on='tconst', how='inner')

    # Фильтрация фильмов (titleType == 'movie')
    movies_df = merged_df[merged_df['titleType'] == 'movie']
    print(f"Найдено {len(movies_df)} фильмов.")

    # Сортировка по рейтингу и выбор ТОП-30
    top_movies_df = movies_df.sort_values(by='averageRating', ascending=False).head(30)
    print("ТОП-30 фильмов успешно отобраны.")

    # Фильтрация эпизодов (titleType == 'tvEpisode')
    episodes_df = merged_df[merged_df['titleType'] == 'tvEpisode']
    print(f"Найдено {len(episodes_df)} эпизодов.")

    # Сохранение данных
    movies_file = os.path.join(silver_dir, "movies.csv")
    episodes_file = os.path.join(silver_dir, "episodes.csv")
    top_movies_file = os.path.join(silver_dir, "top_movies_30.csv")

    movies_df.to_csv(movies_file, index=False)
    print(f"Все фильмы сохранены в {movies_file}.")

    episodes_df.to_csv(episodes_file, index=False)
    print(f"Эпизоды сохранены в {episodes_file}.")

    top_movies_df.to_csv(top_movies_file, index=False)
    print(f"ТОП-30 фильмов сохранён в {top_movies_file}.")

    # Вывод первых 10 фильмов
    print("\nПервые 10 фильмов:")
    print(movies_df.head(10).to_string(index=False))

    # Вывод первых 10 эпизодов
    print("\nПервые 10 эпизодов:")
    print(episodes_df.head(10).to_string(index=False))

    # Вывод первых 10 фильмов из ТОП-30
    print("\nПервые 10 фильмов из ТОП-30:")
    print(top_movies_df.head(10).to_string(index=False))

except Exception as e:
    print(f"Ошибка при обработке файлов: {e}")
