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
        return data.nlargest(num_top_records, 'averageRating', keep='all').sort_values(by='averageRating', ascending=True)

    def save_to_csv(self, data, filename):
        output_file = os.path.join(self.result_folder, filename)
        data.to_csv(output_file, index=False)
        print(f"Результаты сохранены")
        #print(f"Результаты сохранены в {output_file}.")


    def get_top_records(self, data, top_level):
        num_top_records = int((len(data) * top_level) / 100)
        return data.nlargest(num_top_records, 'averageRating', keep='all').sort_values(by='averageRating',
                                                                                       ascending=True)


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
        if os.path.exists(self.raw_folder):
            choice = input("Предыдущие исходные файлы существуют. Обновить (Yes - 1/No - 0): ").strip().lower()
            if choice == "0":
                return

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
            selected_type = None
            filtered_data = None

            # Проверка существования папки с результатами
            if os.path.exists(self.result_folder):
                choice = input(
                    "Сохранить пользовательские файлы предыдущего формирования (Yes - 1/No - 0): ").strip().lower()
                if choice in ["no", "0"]:
                    shutil.rmtree(self.result_folder)
                    print(f"Очищена папка: {self.result_folder}")

            # Создаем папку результатов, если её нет
            self.result_folder = os.path.abspath(self.result_folder)  # Преобразуем путь в абсолютный
            self.file_manager.check_or_create_folder(self.result_folder)
            #print(f"Папка для результатов успешно создана: {self.result_folder}")

            # Загрузка данных
            data = self.data_reader.load_data(self.raw_folder)
            unique_types = data['titleType'].unique()

            # Первичный запрос о формировании файла без разделения по типам
            while True:
                choice = input(
                    "Вы хотите сформировать файл без разделения по типам? (Yes - 1/No - 0): ").strip().lower()
                if choice in ["yes", "1"]:
                    #print("Формирование файла all_types_filtered.csv...")
                    all_file_path = os.path.join(self.result_folder, "all_types_filtered.csv")
                    if os.path.exists(all_file_path):
                        print(f"Файл {all_file_path} уже существует, пропуск сохранения.")
                    else:
                        self.data_processor.save_to_csv(data, all_file_path)
                        #print(f"Результат сохранены")
                        #print(f"Результаты сохранены в {all_file_path}.")
                    break
                elif choice in ["no", "0"]:
                    break
                else:
                    print("Неверный выбор. Пожалуйста, введите 'Yes' или 'No'.")

            # Основной цикл для дополнительных действий
            while True:
                choice = input("Вы хотите выполнить другие действия? (Yes - 1/No - 0): ").strip().lower()
                if choice in ["yes", "1"]:
                    print("Доступные действия:")
                    print("1 - Дополнительно сформировать файл по ТОП категориям")
                    print("2 - Дополнительно сформировать новый файл по типу фильмов")
                    action = input("Выберите действие (1/2): ").strip()

                    if action == "1":
                        # Вызов метода create_top_file для формирования ТОП файла
                        self.create_top_file(data, filtered_data)

                    elif action == "2":
                        # Фильтрация данных по типу фильмов
                        while True:
                            print("\nДоступные типы фильмов:")
                            for idx, title_type in enumerate(unique_types, start=1):
                                print(f"{idx}. {title_type}")

                            try:
                                selected_choice = int(input("\nВыберите номер типа для фильтрации: "))
                                if 1 <= selected_choice <= len(unique_types):
                                    selected_type = unique_types[selected_choice - 1]
                                    filtered_data = self.data_processor.filter_by_type(data, selected_type)
                                    print(f"Фильтр данных: {selected_type}, записей: {len(filtered_data)}")
                                    filtered_file_path = os.path.join(self.result_folder,
                                                                      f"{selected_type}_filtered.csv")

                                    # Проверка существования файла перед сохранением
                                    if os.path.exists(filtered_file_path):
                                        print(f"Файл {filtered_file_path} уже существует, пропуск сохранения.")
                                    else:
                                        self.data_processor.save_to_csv(filtered_data, filtered_file_path)
                                        #print(f"Результаты сохранены 2")
                                        #print(f"Результаты сохранены в {filtered_file_path}.")
                                    break
                                else:
                                    raise ValueError("Выбран некорректный номер.")
                            except ValueError as e:
                                print(f"Неверный выбор: {e}. Попробуйте снова.")

                    else:
                        print("Неверный выбор. Пожалуйста, выберите 1 или 2.")
                elif choice in ["no", "0"]:
                    print("Завершение программы.")
                    self.cleanup()
                    return
                else:
                    print("Неверный выбор. Пожалуйста, введите 'Yes' или 'No'.")

        except Exception as e:
            print(f"Ошибка: {e}")

    def cleanup(self):
        while True:
            save_choice = input(
                f"Вы хотите удалить директорию: {self.raw_folder} и хранящиеся в ней исходные файлы? (Yes - 1/No - 0): ").strip().lower()
            if save_choice == "1":
                for file_name in os.listdir(self.raw_folder):
                    file_path = os.path.join(self.raw_folder, file_name)
                    os.remove(file_path)
                    print(f"Удален исходный файл: {file_path}")
                os.rmdir(self.raw_folder)
                print(f"Удалена директория: {self.raw_folder}")
                print(f"Формирование {len(os.listdir(self.result_folder))} файлов завершено.")
                break
            elif save_choice == "0":
                print(f"Формирование {len(os.listdir(self.result_folder))} файлов завершено.")
                break
            else:
                print("Неверный выбор. Повторите ввод.")

    def perform_additional_actions(self, data, filtered_data):
        while True:
            next_action = input("Вы хотите выполнить другие действия? (Yes - 1/No - 0): ").strip().lower()
            if next_action in ["yes", "1"]:
                print("\nДоступные действия:")
                print("1 - Дополнительно сформировать файл ТОП категории")
                print("2 - Сформировать новый файл по типу фильмов")
                action_choice = input("Выберите действие (1/2): ").strip()

                if action_choice == "1":
                    self.create_top_file(data, filtered_data)
                elif action_choice == "2":
                    return  # Выход из метода для завершения программы
                else:
                    print("Неверный выбор. Попробуйте снова.")
            elif next_action in ["no", "0"]:
                print("Завершение программы.")
                self.cleanup()  # Вызов метода cleanup()
                return  # Выход из метода
            else:
                print("Неверный ввод. Пожалуйста, введите 'Yes' или 'No'.")

    def create_top_file(self, data, filtered_data):
        while True:
            try:
                all_or_selected = int(input("1 - Сформировать файл по выбранной категории, 2 - по всему файлу: "))
                if all_or_selected == 1 and filtered_data is not None:
                    target_data = filtered_data
                    print(f"Будет выполнена обработка для категории: {filtered_data['titleType'].iloc[0]}")
                elif all_or_selected == 2:
                    target_data = data
                    print("Будет выполнена обработка для всех данных.")
                else:
                    print("Некорректный выбор. Попробуйте снова.")
                    continue

                top_level = float(input("Укажите ТОП уровень, от 0.1 до 99.9: "))
                if top_level < 0.1 or top_level > 99.9:
                    print("Неверный уровень. Программа завершена.")
                    return

                top_records = self.data_processor.get_top_records(target_data, top_level)
                print(f"Будет сформирован файл с ТОП-{top_level} записей.")
                print(
                    f"Найдено {len(top_records)} записей ТОП-{top_level} для типа {'all_types' if all_or_selected == 2 else filtered_data['titleType'].iloc[0]}.")

                filename = f"top_{top_level}_percent_{'all_types' if all_or_selected == 2 else filtered_data['titleType'].iloc[0]}.csv"
                self.data_processor.save_to_csv(top_records, filename)
                break
            except ValueError:
                print("Неверный ввод. Попробуйте снова.")

    def cleanup(self):
        while True:
            save_choice = input(
                f"Вы хотите удалить директорию: {self.raw_folder} и хранящиеся в ней исходные файлы? (Yes - 1/No - 0): ").strip().lower()
            if save_choice == "1":
                for file_name in os.listdir(self.raw_folder):
                    file_path = os.path.join(self.raw_folder, file_name)
                    os.remove(file_path)
                    print(f"Удален исходный файл: {file_path}")
                os.rmdir(self.raw_folder)
                print(f"Удалена директория: {self.raw_folder}")
                print(f"Формирование {len(os.listdir(self.result_folder))} файлов завершено.")
                break
            elif save_choice == "0":
                print(f"Формирование {len(os.listdir(self.result_folder))} файлов завершено.")
                break
            else:
                print("Неверный выбор. Повторите ввод.")


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

