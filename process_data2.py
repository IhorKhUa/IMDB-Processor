# Связанная задача: IMDB-2
# Подробнее: https://github.com/IhorKhUa/IMDB-Processor/issues/7

import os
import pandas as pd

# Директория для хранения результатов
result_dir = "/home/ihor/PycharmProjects/extract_files/IMDB-Processor/result_transform"

# Папки для хранения данных
bronze_dir = "bronze"
silver_dir = "silver"

# Создание папки result_transform, если её нет
if not os.path.exists(result_dir):
    os.makedirs(result_dir)
    print(f"Папка '{result_dir}' была создана.")
else:
    print(f"Папка '{result_dir}' уже существует.")

# Пути к файлам
#ratings_file = os.path.join(bronze_dir, "title.ratings.tsv")
#basics_file = os.path.join(bronze_dir, "title.basics.tsv")

# Получаем абсолютный путь к директории, где находится текущий скрипт
script_dir = os.path.dirname(os.path.abspath(__file__))

# Формируем абсолютные пути
bronze_dir = os.path.abspath(os.path.join(script_dir, "../bronze"))
ratings_file = os.path.join(bronze_dir, "title.ratings.tsv")
basics_file = os.path.join(bronze_dir, "title.basics.tsv")

#ratings_file = "/home/ihor/PycharmProjects/extract_files/bronze/title.ratings.tsv"
#basics_file = "/home/ihor/PycharmProjects/extract_files/bronze/title.basics.tsv"


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

    # Фильтрация эпизодов (titleType == 'tvEpisode')
    episodes_df = merged_df[merged_df['titleType'] == 'tvEpisode']
    print(f"Найдено {len(episodes_df)} эпизодов.")

    # Сортировка по рейтингу ТОП-30, выбор первых 30 строк
    top_movies_df = movies_df.sort_values(['averageRating', 'numVotes'], ascending=[False, False]).head(30)
    print("ТОП-30 - первые 30 фильмов успешно отобраны.")

    # Сортировка по рутингу ТОП_30, выбор 30 первых рейтинговых значения
    top_movies2_df = movies_df.nlargest(30,'averageRating',keep='all').sort_values(by='originalTitle',ascending=True)
    print("ТОП-30 - 30 фильмов средниму баллу успешно отобраны.")

       # Сохранение данных в result_transform
    movies_file = os.path.join(result_dir, "movies.csv")
    episodes_file = os.path.join(result_dir, "episodes.csv")
    top_movies_file = os.path.join(result_dir, "top_movies_30_first_line.csv")
    top_movies_file2 = os.path.join(result_dir, "top_movies_30_avg_rating.csv")

    movies_df.to_csv(movies_file, index=False)
    print(f"Все фильмы сохранены в {movies_file}.")

    episodes_df.to_csv(episodes_file, index=False)
    print(f"Эпизоды сохранены в {episodes_file}.")

    top_movies_df.to_csv(top_movies_file, index=False)
    print(f"ТОП-30 фильмов по первым строкам сохранён в {top_movies_file}.")

    top_movies2_df.to_csv(top_movies_file2, index=False)
    print(f"ТОП-30 фильмов по рейтингу сохранён в {top_movies_file2}.")

    # Вывод первых 10 фильмов
    print("\nПервые 10 фильмов:")
    print(movies_df.head(10).to_string(index=False))

    # Вывод первых 10 эпизодов
    print("\nПервые 10 эпизодов:")
    print(episodes_df.head(10).to_string(index=False))

    # Вывод первых 10 фильмов из ТОП-30
    print("\nПервые 10 фильмов из ТОП-30 по строкам:")
    print(top_movies_df.head(10).to_string(index=False))

    # Вывод первых 10 фильмов из ТОП-30
    print("\nПервые 10 фильмов из ТОП-30 по рейтингу:")
    print(top_movies2_df.head(10).to_string(index=False))

except Exception as e:
    print(f"Ошибка при обработке файлов: {e}")