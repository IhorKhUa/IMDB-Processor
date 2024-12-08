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
        if os.path.exists(self.raw_folder):
            choice = input("Предыдущие исходные файлы существуют. Обновить (Yes/No): ").strip().lower()
            if choice == "no":
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
            selected_type = None  # Инициализация переменной перед основным блоком кода
            filtered_data = None  # Инициализация переменной для предотвращения ошибки

            if os.path.exists(self.result_folder):
                choice = input("Сохранить пользовательские файлы предыдущего формирования (Yes/No): ").strip().lower()
                if choice == "no":
                    shutil.rmtree(self.result_folder)
                    print(f"Удалена папка: {self.result_folder}")

            self.file_manager.check_or_create_folder(self.result_folder)
            data = self.data_reader.load_data(self.raw_folder)

            unique_types = data['titleType'].unique()

            while True:
                choice = input(
                    "Вы хотите сформировать файлы по: 1 - типам фильмов, 2 - без разделения на типы: ").strip()
                if choice == "1":
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
                                self.data_processor.save_to_csv(filtered_data, f"{selected_type}_filtered.csv")
                                break
                            else:
                                raise ValueError("Выбран некорректный номер.")
                        except ValueError as e:
                            print(f"Неверный выбор: {e}. Попробуйте снова.")

                    if not selected_type:
                        print("Вы не выбрали тип фильма. Программа завершена.")
                        return  # Завершение программы

                    print(f"Генерация файла для типа {selected_type} завершена.")
                    break

                elif choice == "2":
                    # Обработка данных без фильтрации по типам (используем merged_df напрямую)
                    print("Формирование файла all_types_filtered.csv уже выполнено.")
                    self.data_processor.save_to_csv(data,
                                                    "all_types_filtered.csv")  # Сохраняем весь объединенный DataFrame

                    # Запрос о дополнительных действиях
                    while True:
                        next_step = input("\nВы хотите выполнить другие действия? (Yes/No): ").strip().lower()
                        if next_step == "yes":
                            # Запрос о ТОП уровне после выбора "2"
                            top_level = float(input("Укажите ТОП уровень, от 0.1 до 99.9: "))
                            if top_level < 0.1 or top_level > 99.9:
                                print("Неверный уровень. Программа завершена.")
                                return

                            top_records = self.data_processor.get_top_records(data, top_level)
                            print(f"Будет сформирован файл с ТОП-{top_level}% записей.")
                            print(f"Найдено {len(top_records)} записей ТОП-{top_level}% для всех данных.")
                            self.data_processor.save_to_csv(top_records, f"top_{top_level}_percent_all.csv")
                            break
                        elif next_step == "no":
                            print("Программа завершена.")
                            break
                        else:
                            print("Неверный выбор. Повторите ввод.")
                    break

                else:
                    print("Неверный выбор. Попробуйте снова.")

            while True:
                next_step = input("\nВы хотите выполнить другие действия? (Yes/No): ").strip().lower()
                if next_step == "yes":
                    # Новый выбор: дополнительно сформировать файл или перейти к выбору типа фильма
                    additional_step = input(
                        "1 - Дополнительно сформировать файл ТОП категории? 2 - Сформировать новый файл по типу фильмов: ").strip()
                    if additional_step == "1":
                        # Действие по формированию файла ТОП категории
                        if not selected_type:
                            print("Ошибка: не выбран тип фильма для формирования файла ТОП.")
                            return  # Завершение программы

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
