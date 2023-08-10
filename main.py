from database import DBManager
from engine_classes import HH
from utils import formatting_vacancy, open_file, get_data_from_env
import pandas as pd

# Получение параметров для создания БД из файла .env
params = get_data_from_env()

# Создание базы данных
db = DBManager('hh', params)
db.create_database_and_tables()

# Чтение файла с перечнем работодателей
content = open_file('employer.json')

# Заполнение таблицы с перечнем работодателей
db.insert_to_table_companies(content)

# Получение вакансий по id работодателей в цикле
for key in content:
    hh = HH(key)
    vacancy_list = hh.get_request()

    # Форматирование полученных данных и получение списка кортежей для записи в БД
    data_list = formatting_vacancy(vacancy_list)

    # Заполнение таблицы с перечнем вакансий
    db.insert_to_table_vacancies(data_list)

# Параметры для вывода таблицы (имеется ограничение по количеству символов в столбце таблицы)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)
pd.set_option('display.max_rows', None)

while True:
    print('Меню:\n'
          '1. Получить список всех компаний и количество вакансий у каждой компании.\n'
          '2. Получить список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на '
          'вакансию.\n'
          '3. Получить среднюю зарплату по вакансиям.\n'
          '4. Получить список всех вакансий, у которых зарплата выше средней по всем вакансиям.\n'
          '5. Получить список всех вакансий, в названии которых содержится переданное слово.\n'
          '6. Выйти из меню)\n')
    user_input = int(input('Выберите нужную цифру:'))
    if user_input == 1:
        print(db.get_companies_and_vacancies_count(), '\n')
    elif user_input == 2:
        print(db.get_all_vacancies(), '\n')
    elif user_input == 3:
        print(db.get_avg_salary(), '\n')
    elif user_input == 4:
        print(db.get_vacancies_with_higher_salary(), '\n')
    elif user_input == 5:
        word_input = input('Введите искомое слово:')
        print(db.get_vacancies_with_keyword(word_input), '\n')
    elif user_input == 6:
        print("Вы вышли из меню")
        break
    else:
        print('Цифра введена некорректно\n')
