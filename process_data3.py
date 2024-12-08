import os
import requests
import gzip
import shutil
import pandas as pd


class FileManager:
    """
    Класс для работы с файлами: проверка, создание, загрузка и распаковка.
    """

    @staticmethod
    def check_or_create_folder(folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    @staticmethod
    def download_file(url, dest_path):
        print(f"Загрузка файла из {url}...")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(dest_path, "wb") as file:
                shutil.copyfileobj(response.raw, file)
            print(f"Файл сохранен: {dest_path}")
        else:
            raise Exception(f"Ошибка загрузки файла: {url}, код ответа {response.status_code}")

    @staticmethod
    def extract_gzip(source_path, dest_path):
        print(f"Распаковка файла {source_path}...")
        with gzip.open(source_path, "rb") as f_in:
            with open(dest_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Файл распакован: {dest_path}")


class DataReader:
    """
    Класс для чтения и объединения данных.
    """

    @staticmethod
    def load_data(raw_folder):
        ratings_file = os.path.join(raw_folder, "title_ratings.tsv")
        basics_file = os.path.join(raw_folder, "title_basics.tsv")

        ratings_df = pd.read_csv(ratings_file, sep='\t', low_memory=False)
        basics_df = pd.read_csv(basics_file, sep='\t', low_memory=False)

        merged_df = pd.merge(basics_df, ratings_df, on='tconst', how='inner')
        return merged_df


class DataProcessor:
    """
    Класс для обработки данных: фильтрация, выборка топов, сохранение в CSV.
    """

    def __init__(self, result_folder):
        self.result_folder = result_folder

    def filter_by_type(self, data, selected_type):
        return data[data['titleType'] == selected_type]

    def get_top_records(self, data, top_level):
        num_top_records = int((len(data) * top_level) / 100)
        return data.nlargest(num_top_records, 'averageRating', keep='all').sort_values(by='averageRating',
                                                                                       ascending=True)

    def save_to_csv(self, data, filename):
        output_file = os.path.join(self.result_folder, filename)
        data.to_csv(output_file, index=False)
        print(f"Результаты сохранены в {output_file}.")


class IMDBDataPipeline:
    """
    Основной класс для управления процессом обработки данных.
    """

    def __init__(self, raw_folder, result_folder, urls):
        self.file_manager = FileManager()
        self.data_reader = DataReader()
        self.data_processor = DataProcessor(result_folder)
        self.raw_folder = raw_folder
        self.result_folder = result_folder
        self.urls = urls

    def update_data(self):
        self.file_manager.check_or_create_folder(self.raw_folder)

        for name, url in self.urls.items():
            compressed_file_path = os.path.join(self.raw_folder, f"{name}.tsv.gz")
            extracted_file_path = os.path.join(self.raw_folder, f"{name}.tsv")

            self.file_manager.download_file(url, compressed_file_path)
            self.file_manager.extract_gzip(compressed_file_path, extracted_file_path)

            if os.path.exists(compressed_file_path):
                os.remove(compressed_file_path)
                print(f"Удален архив: {compressed_file_path}")

    def run(self):
        try:
            self.file_manager.check_or_create_folder(self.result_folder)
            data = self.data_reader.load_data(self.raw_folder)

            unique_types = data['titleType'].unique()
            while True:
                print("\nДоступные типы фильмов:")
                for idx, title_type in enumerate(unique_types, start=1):
                    print(f"{idx}. {title_type}")

                try:
                    choice = int(input("\nВыберите номер типа для фильтрации: "))
                    if 1 <= choice <= len(unique_types):
                        selected_type = unique_types[choice - 1]
                        break
                    else:
                        raise ValueError
                except ValueError:
                    cont = input("Неверный выбор. Вы хотите завершить программу? (Yes/No): ").strip().lower()
                    if cont == "yes":
                        print("Программа завершена.")
                        return

            filtered_data = self.data_processor.filter_by_type(data, selected_type)
            print(f"Найдено {len(filtered_data)} записей для типа {selected_type}.")

            self.data_processor.save_to_csv(filtered_data, f"{selected_type}_filtered.csv")

            while True:
                next_step = input("\nВы хотите выполнить другие действия? (Yes/No): ").strip().lower()
                if next_step == "yes":
                    # Новый выбор: дополнительно сформировать файл или перейти к выбору типа фильма
                    additional_step = input(
                        "1 - Дополнительно сформировать файл ТОП категории? 2 - Сформировать новый файл по типу фильмов: ").strip()
                    if additional_step == "1":
                        # Действие по формированию файла ТОП категории
                        while True:
                            try:
                                all_or_selected = int(
                                    input("1 - Сформировать файл по выбранной категории, 2 - по всему файлу: "))
                                if all_or_selected == 1:
                                    target_data = filtered_data
                                    print(f"Будет выполнена обработка для категории: {selected_type}")
                                elif all_or_selected == 2:
                                    target_data = data
                                    print("Будет выполнена обработка для всех данных.")
                                else:
                                    raise ValueError

                                top_level = float(input("Укажите ТОП уровень, от 0.1 до 99.9: "))
                                if top_level < 0.1 or top_level > 99.9:
                                    print("Неверный уровень. Программа завершена.")
                                    return

                                top_records = self.data_processor.get_top_records(target_data, top_level)
                                print(f"Будет сформирован файл с ТОП-{top_level}% записей.")
                                print(
                                    f"Найдено {len(top_records)} записей ТОП-{top_level}% для типа {'всех данных' if all_or_selected == 2 else selected_type}.")

                                filename = f"top_{top_level}_percent_{'all' if all_or_selected == 2 else selected_type}.csv"
                                self.data_processor.save_to_csv(top_records, filename)
                                break
                            except ValueError:
                                print("Неверный выбор. Повторите ввод.")
                        break
                    elif additional_step == "2":
                        # Перезапуск выбора типа фильма
                        print("\nДоступные типы фильмов:")
                        for idx, title_type in enumerate(unique_types, start=1):
                            print(f"{idx}. {title_type}")

                        try:
                            choice = int(input("\nВыберите номер типа для фильтрации: "))
                            if 1 <= choice <= len(unique_types):
                                selected_type = unique_types[choice - 1]
                                filtered_data = self.data_processor.filter_by_type(data, selected_type)
                                print(f"Найдено {len(filtered_data)} записей для типа {selected_type}.")
                                self.data_processor.save_to_csv(filtered_data, f"{selected_type}_filtered.csv")
                            else:
                                raise ValueError
                        except ValueError:
                            cont = input("Неверный выбор. Вы хотите завершить программу? (Yes/No): ").strip().lower()
                            if cont == "yes":
                                print("Программа завершена.")
                                return
                elif next_step == "no":
                    print("Программа завершена.")
                    break
                else:
                    print("Неверный выбор. Повторите ввод.")

            # Новый блок для удаления директории
            while True:
                save_choice = input(
                    f"Вы хотите удалить директорию: {self.raw_folder} и хранящиеся в ней исходные файлы? (Yes/No): ").strip().lower()

                if save_choice == "yes":
                    for file_name in os.listdir(self.raw_folder):
                        file_path = os.path.join(self.raw_folder, file_name)
                        os.remove(file_path)
                        print(f"Удален исходный файл: {file_path}")
                    os.rmdir(self.raw_folder)
                    print(f"Удалена директория: {self.raw_folder}")
                    print(f"Формирование {len(os.listdir(self.result_folder))} файлов завершено.")
                    break
                elif save_choice == "no":
                    print(f"Формирование {len(os.listdir(self.result_folder))} файлов завершено.")
                    break
                else:
                    print("Неверный выбор. Повторите ввод.")

        except Exception as e:
            print(f"Ошибка: {e}")

if __name__ == "__main__":
    RAW_FOLDER = "Raw"
    RESULT_FOLDER = "Result_ETL"
    URLS = {
        "title_basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
        "title_ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
    }

    pipeline = IMDBDataPipeline(RAW_FOLDER, RESULT_FOLDER, URLS)
    pipeline.update_data()
    pipeline.run()

"""
import os
import requests
import gzip
import shutil
import pandas as pd

class FileManager:
    
    #Класс для работы с файлами: проверка, создание, загрузка и распаковка.
    
    @staticmethod
    def check_or_create_folder(folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    @staticmethod
    def download_file(url, dest_path):
        print(f"Загрузка файла из {url}...")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(dest_path, "wb") as file:
                shutil.copyfileobj(response.raw, file)
            print(f"Файл сохранен: {dest_path}")
        else:
            raise Exception(f"Ошибка загрузки файла: {url}, код ответа {response.status_code}")

    @staticmethod
    def extract_gzip(source_path, dest_path):
        print(f"Распаковка файла {source_path}...")
        with gzip.open(source_path, "rb") as f_in:
            with open(dest_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Файл распакован: {dest_path}")


class DataProcessor:
    
    #Класс для обработки данных: чтение, объединение, фильтрация, сортировка.
    
    def __init__(self, raw_folder, result_folder):
        self.raw_folder = raw_folder
        self.result_folder = result_folder

    def load_data(self):
        ratings_file = os.path.join(self.raw_folder, "title_ratings.tsv")
        basics_file = os.path.join(self.raw_folder, "title_basics.tsv")

        ratings_df = pd.read_csv(ratings_file, sep='\t', low_memory=False)
        basics_df = pd.read_csv(basics_file, sep='\t', low_memory=False)

        merged_df = pd.merge(basics_df, ratings_df, on='tconst', how='inner')
        return merged_df

    def filter_by_type(self, data, selected_type):
        return data[data['titleType'] == selected_type]

    def get_top_records(self, data, top_level):
        num_top_records = int((len(data) * top_level) / 100)
        return data.nlargest(num_top_records, 'averageRating', keep='all').sort_values(by='averageRating', ascending=True)

    def save_to_csv(self, data, filename):
        output_file = os.path.join(self.result_folder, filename)
        data.to_csv(output_file, index=False)
        print(f"Результаты сохранены в {output_file}.")


class IMDBDataPipeline:
    
    #Основной класс для управления процессом обработки данных.
    
    def __init__(self, raw_folder, result_folder, urls):
        self.file_manager = FileManager()
        self.data_processor = DataProcessor(raw_folder, result_folder)
        self.raw_folder = raw_folder
        self.result_folder = result_folder
        self.urls = urls

    def update_data(self):
        self.file_manager.check_or_create_folder(self.raw_folder)

        for name, url in self.urls.items():
            compressed_file_path = os.path.join(self.raw_folder, f"{name}.tsv.gz")
            extracted_file_path = os.path.join(self.raw_folder, f"{name}.tsv")

            self.file_manager.download_file(url, compressed_file_path)
            self.file_manager.extract_gzip(compressed_file_path, extracted_file_path)

            if os.path.exists(compressed_file_path):
                os.remove(compressed_file_path)
                print(f"Удален архив: {compressed_file_path}")

    def run(self):
        try:
            self.file_manager.check_or_create_folder(self.result_folder)
            data = self.data_processor.load_data()

            unique_types = data['titleType'].unique()
            while True:
                print("\nДоступные типы фильмов:")
                for idx, title_type in enumerate(unique_types, start=1):
                    print(f"{idx}. {title_type}")

                try:
                    choice = int(input("\nВыберите номер типа для фильтрации: "))
                    if 1 <= choice <= len(unique_types):
                        selected_type = unique_types[choice - 1]
                        break
                    else:
                        raise ValueError
                except ValueError:
                    cont = input("Неверный выбор. Вы хотите завершить программу? (Yes/No): ").strip().lower()
                    if cont == "yes":
                        print("Программа завершена.")
                        return

            filtered_data = self.data_processor.filter_by_type(data, selected_type)
            print(f"Найдено {len(filtered_data)} записей для типа {selected_type}.")

            self.data_processor.save_to_csv(filtered_data, f"{selected_type}_filtered.csv")

            while True:
                top_choice = input("Сформировать файл по ТОП категории? (Yes/No): ").strip().lower()
                if top_choice == 'yes':
                    while True:
                        try:
                            all_or_selected = int(input("1 - Сформировать файл по выбранной категории, 2 - по всему файлу: "))
                            if all_or_selected == 1:
                                target_data = filtered_data
                                print(f"Будет выполнена обработка для категории: {selected_type}")
                            elif all_or_selected == 2:
                                target_data = data
                                print("Будет выполнена обработка для всех данных.")
                            else:
                                raise ValueError

                            top_level = float(input("Укажите ТОП уровень, от 0.1 до 99.9: "))
                            if top_level < 0.1 or top_level > 99.9:
                                print("Неверный уровень. Программа завершена.")
                                return

                            top_records = self.data_processor.get_top_records(target_data, top_level)
                            print(f"Будет сформирован файл с ТОП-{top_level}% записей.")
                            print(f"Найдено {len(top_records)} записей ТОП-{top_level}% для типа {'всех данных' if all_or_selected == 2 else selected_type}.")

                            filename = f"top_{top_level}_percent_{'all' if all_or_selected == 2 else selected_type}.csv"
                            self.data_processor.save_to_csv(top_records, filename)
                            break
                        except ValueError:
                            print("Неверный выбор. Повторите ввод.")
                    break
                elif top_choice == 'no':
                    cont = input("Вы хотите завершить программу? (Yes/No): ").strip().lower()
                    if cont == "yes":
                        print("Программа завершена.")
                        return
                    elif cont == "no":
                        print("Возврат к запросу.")
                    else:
                        print("Неверный выбор. Возврат к запросу.")
                else:
                    print("Неверный выбор. Возврат к запросу.")

            while True:
                save_choice = input(f"Вы хотите удалить директорию: {self.raw_folder} и хранящиеся в ней исходные файлы? (Yes/No): ").strip().lower()

                if save_choice == "yes":
                    for file_name in os.listdir(self.raw_folder):
                        file_path = os.path.join(self.raw_folder, file_name)
                        os.remove(file_path)
                        print(f"Удален исходный файл: {file_path}")
                    os.rmdir(self.raw_folder)
                    print(f"Удалена директория: {self.raw_folder}")
                    print(f"Формирование {len(os.listdir(self.result_folder))} файлов завершено.")
                    break
                elif save_choice == "no":
                    print(f"Формирование {len(os.listdir(self.result_folder))} файлов завершено.")
                    break
                else:
                    print("Неверный выбор. Повторите ввод.")

        except Exception as e:
            print(f"Ошибка: {e}")


if __name__ == "__main__":
    RAW_FOLDER = "Raw"
    RESULT_FOLDER = "Result_ETL"
    URLS = {
        "title_basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
        "title_ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
    }

    pipeline = IMDBDataPipeline(RAW_FOLDER, RESULT_FOLDER, URLS)
    pipeline.update_data()
    pipeline.run()




import os
import requests
import gzip
import shutil
import pandas as pd

class FileManager:
    
    #Класс для работы с файлами: проверка, создание, загрузка и распаковка.
    
    @staticmethod
    def check_or_create_folder(folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    @staticmethod
    def download_file(url, dest_path):
        print(f"Загрузка файла из {url}...")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(dest_path, "wb") as file:
                shutil.copyfileobj(response.raw, file)
            print(f"Файл сохранен: {dest_path}")
        else:
            raise Exception(f"Ошибка загрузки файла: {url}, код ответа {response.status_code}")

    @staticmethod
    def extract_gzip(source_path, dest_path):
        print(f"Распаковка файла {source_path}...")
        with gzip.open(source_path, "rb") as f_in:
            with open(dest_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Файл распакован: {dest_path}")


class DataProcessor:
    
    #Класс для обработки данных: чтение, объединение, фильтрация, сортировка.
    
    def __init__(self, raw_folder, result_folder):
        self.raw_folder = raw_folder
        self.result_folder = result_folder

    def load_data(self):
        ratings_file = os.path.join(self.raw_folder, "title_ratings.tsv")
        basics_file = os.path.join(self.raw_folder, "title_basics.tsv")

        ratings_df = pd.read_csv(ratings_file, sep='\t', low_memory=False)
        basics_df = pd.read_csv(basics_file, sep='\t', low_memory=False)

        merged_df = pd.merge(basics_df, ratings_df, on='tconst', how='inner')
        return merged_df

    def filter_by_type(self, data, selected_type):
        return data[data['titleType'] == selected_type]

    def get_top_records(self, data, top_level):
        num_top_records = int((len(data) * top_level) / 100)
        return data.nlargest(num_top_records, 'averageRating', keep='all').sort_values(by='averageRating', ascending=True)

    def save_to_csv(self, data, filename):
        output_file = os.path.join(self.result_folder, filename)
        data.to_csv(output_file, index=False)
        print(f"Результаты сохранены в {output_file}.")


class IMDBDataPipeline:
    
    #Основной класс для управления процессом обработки данных.
    
    def __init__(self, raw_folder, result_folder, urls):
        self.file_manager = FileManager()
        self.data_processor = DataProcessor(raw_folder, result_folder)
        self.raw_folder = raw_folder
        self.result_folder = result_folder
        self.urls = urls

    def update_data(self):
        self.file_manager.check_or_create_folder(self.raw_folder)

        for name, url in self.urls.items():
            compressed_file_path = os.path.join(self.raw_folder, f"{name}.tsv.gz")
            extracted_file_path = os.path.join(self.raw_folder, f"{name}.tsv")

            self.file_manager.download_file(url, compressed_file_path)
            self.file_manager.extract_gzip(compressed_file_path, extracted_file_path)

            if os.path.exists(compressed_file_path):
                os.remove(compressed_file_path)
                print(f"Удален архив: {compressed_file_path}")

    def run(self):
        try:
            self.file_manager.check_or_create_folder(self.result_folder)
            data = self.data_processor.load_data()

            unique_types = data['titleType'].unique()
            print("Доступные типы фильмов:")
            for idx, title_type in enumerate(unique_types, start=1):
                print(f"{idx}. {title_type}")

            choice = int(input("Выберите номер типа для фильтрации: "))
            if choice < 1 or choice > len(unique_types):
                print("Неверный выбор. Программа завершена.")
                return

            selected_type = unique_types[choice - 1]
            filtered_data = self.data_processor.filter_by_type(data, selected_type)
            print(f"Найдено {len(filtered_data)} записей для типа {selected_type}.")

            self.data_processor.save_to_csv(filtered_data, f"{selected_type}_filtered.csv")

            top_choice = input("Сформировать файл по ТОП категории? (да/нет): ").strip().lower()
            if top_choice == 'да':
                all_or_selected = int(input("1 - Сформировать файл по выбранной категории, 2 - по всему файлу: "))
                if all_or_selected == 1:
                    target_data = filtered_data
                    print(f"Будет выполнена обработка для категории: {selected_type}")
                elif all_or_selected == 2:
                    target_data = data
                    print("Будет выполнена обработка для всех данных.")
                else:
                    print("Неверный выбор. Программа завершена.")
                    return

                top_level = float(input("Укажите ТОП уровень, от 0.1 до 99.9: "))
                if top_level < 0.1 or top_level > 99.9:
                    print("Неверный уровень. Программа завершена.")
                    return

                top_records = self.data_processor.get_top_records(target_data, top_level)
                print(f"Будет сформирован файл с ТОП-{top_level}% записей.")
                print(f"Найдено {len(top_records)} записей ТОП-{top_level}% для типа {'всех данных' if all_or_selected == 2 else selected_type}.")

                filename = f"top_{top_level}_percent_{'all' if all_or_selected == 2 else selected_type}.csv"
                self.data_processor.save_to_csv(top_records, filename)

        except Exception as e:
            print(f"Ошибка: {e}")


if __name__ == "__main__":
    RAW_FOLDER = "Raw"
    RESULT_FOLDER = "Result_ETL"
    URLS = {
        "title_basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
        "title_ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
    }

    pipeline = IMDBDataPipeline(RAW_FOLDER, RESULT_FOLDER, URLS)
    pipeline.update_data()
    pipeline.run()



import os
import requests
import gzip
import shutil
import os
import pandas as pd
from abc import ABC, abstractmethod
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext


# Папка для хранения данных
RAW_FOLDER = "Raw"
#RAW_FOLDER = os.path.join(os.getcwd(), "Raw")
#RAW_FOLDER = "/home/ihor/PycharmProjects/extract_files/IMDB-Processor/Raw"
result_dir = "Result_ETL"

# URL-адреса для загрузки данных
URLS = {
    "title_basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "title_ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
}

def check_or_create_folder(folder_path):
    #Создает папку, если она не существует.
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def download_file(url, dest_path):
    #Загружает файл по URL и сохраняет его по указанному пути.
    print(f"Загрузка файла из {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(dest_path, "wb") as file:
            shutil.copyfileobj(response.raw, file)
        print(f"Файл сохранен: {dest_path}")
    else:
        print(f"Ошибка загрузки файла: {url}, код ответа {response.status_code}")


def extract_gzip(source_path, dest_path):
    #Распаковывает .gz файл.
    print(f"Распаковка файла {source_path}...")
    with gzip.open(source_path, "rb") as f_in:
        with open(dest_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Файл распакован: {dest_path}")


def update_data():
    #Основная логика обновления данных.
    check_or_create_folder(RAW_FOLDER)

    for name, url in URLS.items():
        compressed_file_path = os.path.join(RAW_FOLDER, f"{name}.tsv.gz")
        extracted_file_path = os.path.join(RAW_FOLDER, f"{name}.tsv")

        # Загружаем файл, если его нет или он устарел
        download_file(url, compressed_file_path)

        # Распаковываем файл
        extract_gzip(compressed_file_path, extracted_file_path)

        # Удаляем архив после распаковки
        if os.path.exists(compressed_file_path):
            os.remove(compressed_file_path)
            print(f"Удален архив: {compressed_file_path}")

if __name__ == "__main__":

    update_data()

def update_data_with_filter():
    #
    #Основная логика обновления данных с выбором фильтрации по типу.
    
    ratings_file = os.path.join(RAW_FOLDER, "title_ratings.tsv")
    basics_file = os.path.join(RAW_FOLDER, "title_basics.tsv")

    try:
        # Чтение данных
        ratings_df = pd.read_csv(ratings_file, sep='\t', low_memory=False)
        basics_df = pd.read_csv(basics_file, sep='\t', low_memory=False)

        # Объединение данных по ключу 'tconst'
        merged_df = pd.merge(basics_df, ratings_df, on='tconst', how='inner')

        # Определение всех уникальных типов из titleType
        unique_types = merged_df['titleType'].unique()
        print("Доступные типы фильмов:")
        for idx, title_type in enumerate(unique_types, start=1):
            print(f"{idx}. {title_type}")

        # Запрос у пользователя выбора типа фильтрации
        choice = int(input("Выберите номер типа для фильтрации: "))
        if choice < 1 or choice > len(unique_types):
            print("Неверный выбор. Программа завершена.")
            return

        selected_type = unique_types[choice - 1]
        print(f"Выбранный тип: {selected_type}")

        # Фильтрация по выбранному типу
        filtered_df = merged_df[merged_df['titleType'] == selected_type]
        print(f"Найдено {len(filtered_df)} записей для типа {selected_type}.")

        # Проверка и создание директории для сохранения
        check_or_create_folder(result_dir)

        # Сохранение результата
        output_file = os.path.join(result_dir, f"{selected_type}_filtered.csv")
        filtered_df.to_csv(output_file, index=False)
        print(f"Результаты сохранены в {output_file}.")

        # Дополнительная обработка для ТОП категории
        top_choice = input("Сформировать файл по ТОП категории? (да/нет): ").strip().lower()
        if top_choice == 'да':
            all_or_selected = int(input("1 - Сформировать файл по выбранной категории, 2 - по всему файлу: "))

            if all_or_selected == 1:
                target_df = filtered_df
                print(f"Будет выполнена обработка для категории: {selected_type}")
            elif all_or_selected == 2:
                target_df = merged_df
                print("Будет выполнена обработка для всех данных.")
            else:
                print("Неверный выбор. Программа завершена.")
                return

            top_level = float(input("Укажите ТОП уровень, от 0.1 до 99.9: "))
            if top_level < 0.1 or top_level > 99.9:
                print("Неверный уровень. Программа завершена.")
                return

            num_top_records = int((len(target_df) * top_level) / 100)

            top_movies_df = target_df.nlargest(num_top_records, 'averageRating', keep='all').sort_values(by='originalTitle', ascending=True)
            print(f"Будет сформирован файл с ТОП-{top_level}% записей.")
            print(f"Найдено {len(top_movies_df)} записей ТОП-{top_level}% для типа {'всех данных' if all_or_selected == 2 else selected_type}.")

            top_output_file = os.path.join(result_dir, f"top_{top_level}_percent_{'all' if all_or_selected == 2 else selected_type}.csv")
            top_movies_df.to_csv(top_output_file, index=False)
            print(f"ТОП-{top_level}% записи сохранены в {top_output_file}.")

    except Exception as e:
        print(f"Ошибка при обработке файлов: {e}")

# Вызов функции обновления данных с фильтрацией
update_data_with_filter()


___________________________________________________________________________________________________
def update_data_with_filter():
    
    #Основная логика обновления данных с выбором фильтрации по типу.
    
    ratings_file = os.path.join(RAW_FOLDER, "title_ratings.tsv")
    basics_file = os.path.join(RAW_FOLDER, "title_basics.tsv")

    try:
        # Чтение данных
        ratings_df = pd.read_csv(ratings_file, sep='\t', low_memory=False)
        basics_df = pd.read_csv(basics_file, sep='\t', low_memory=False)

        # Объединение данных по ключу 'tconst'
        merged_df = pd.merge(basics_df, ratings_df, on='tconst', how='inner')

        # Определение всех уникальных типов из titleType
        unique_types = merged_df['titleType'].unique()
        print("Доступные типы фильмов:")
        for idx, title_type in enumerate(unique_types, start=1):
            print(f"{idx}. {title_type}")

        # Запрос у пользователя выбора типа фильтрации
        choice = int(input("Выберите номер типа для фильтрации: "))
        if choice < 1 or choice > len(unique_types):
            print("Неверный выбор. Программа завершена.")
            return

        selected_type = unique_types[choice - 1]
        print(f"Выбранный тип: {selected_type}")

        # Фильтрация по выбранному типу
        filtered_df = merged_df[merged_df['titleType'] == selected_type]
        print(f"Найдено {len(filtered_df)} записей для типа {selected_type}.")

        # Проверка и создание директории для сохранения
        check_or_create_folder(result_dir)

        # Сохранение результата
        output_file = os.path.join(result_dir, f"{selected_type}_filtered.csv")
        filtered_df.to_csv(output_file, index=False)
        print(f"Результаты сохранены в {output_file}.")

    except Exception as e:
        print(f"Ошибка при обработке файлов: {e}")

# Вызов функции обновления данных с фильтрацией
update_data_with_filter()

 
ratings_file = os.path.join(RAW_FOLDER, "title_ratings.tsv")
basics_file = os.path.join(RAW_FOLDER, "title_basics.tsv")

try:
    # Чтение данных
    #print(f"Чтение данных из {ratings_file}...")
    ratings_df = pd.read_csv(ratings_file, sep='\t', low_memory=False)
    #print("Данные о рейтингах успешно загружены.")

    #print(f"Чтение данных из {basics_file}...")
    basics_df = pd.read_csv(basics_file, sep='\t', low_memory=False)
   # print("Данные о базовых характеристиках успешно загружены.")

    # Объединение данных по ключу 'tconst'
    #print("Объединение данных...")
    merged_df = pd.merge(basics_df, ratings_df, on='tconst', how='inner')

    # Фильтрация фильмов (titleType == 'movie')
    movies_df = merged_df[merged_df['titleType'] == 'movie']
    print(f"Найдено {len(movies_df)} фильмов.")

    # Фильтрация эпизодов (titleType == 'tvEpisode')
    episodes_df = merged_df[merged_df['titleType'] == 'tvEpisode']
    print(f"Найдено {len(episodes_df)} эпизодов.")

    # Сортировка по рейтингу ТОП-30, выбор первых 30 строк
    top_movies_df = movies_df.sort_values(['averageRating', 'numVotes'], ascending=[False, False]).head(30)
    #print("ТОП-30 - первые 30 фильмов успешно отобраны.")

    # Сортировка по рутингу ТОП_30, выбор 30 первых рейтинговых значения
    top_movies2_df = movies_df.nlargest(30,'averageRating',keep='all').sort_values(by='originalTitle',ascending=True)
    #print("ТОП-30 - 30 фильмов средниму баллу успешно отобраны.")

    #result_dir = "Result_ETL"
    # Проверка и создание директории
    check_or_create_folder(result_dir)

       # Сохранение данных в result_transform
    movies_file = os.path.join(result_dir, "movies.csv")
    episodes_file = os.path.join(result_dir, "episodes.csv")
    top_movies_file = os.path.join(result_dir, "top_30_movies_first_30_line.csv")
    top_movies_file2 = os.path.join(result_dir, "top_30_movies_avg_rating.csv")

    movies_df.to_csv(movies_file, index=False)
    print(f"Фильмы сохранены в {movies_file}.")

    episodes_df.to_csv(episodes_file, index=False)
    print(f"Эпизоды сохранены в {episodes_file}.")

    top_movies_df.to_csv(top_movies_file, index=False)
    print(f"ТОП-30 фильмов по первым строкам сохранён в {top_movies_file}.")

    top_movies2_df.to_csv(top_movies_file2, index=False)
    print(f"ТОП-30 фильмов по рейтингу сохранён в {top_movies_file2}.")

    # Вывод первых 10 фильмов
    #print("\nПервые 10 фильмов:")
    #print(movies_df.head(10).to_string(index=False))

    # Вывод первых 10 эпизодов
    #print("\nПервые 10 эпизодов:")
    #print(episodes_df.head(10).to_string(index=False))

    # Вывод первых 10 фильмов из ТОП-30
    #print("\nПервые 10 фильмов из ТОП-30 по строкам:")
    #print(top_movies_df.head(10).to_string(index=False))

    # Вывод первых 10 фильмов из ТОП-30
    #print("\nПервые 10 фильмов из ТОП-30 по рейтингу:")
    #print(top_movies2_df.head(10).to_string(index=False))

except Exception as e:
    print(f"Ошибка при обработке файлов: {e}") 
 """