import json
import os
from dotenv import load_dotenv


def open_file(file_name: json) -> dict:
    """Читает файл и возвращает содержимое json файла"""
    with open(file_name, encoding='UTF-8') as file:
        content = json.load(file)
    return content


def get_data_from_env() -> dict:
    """Читает файл .env и возвращает словарь с данными для подключения к БД"""
    load_dotenv()
    db_config = {
                    'host': os.getenv('host'),
                    'user': os.getenv('user'),
                    'password': os.getenv('password'),
                    'port': os.getenv('port')
    }
    return db_config


def formatting_vacancy(vacancy_list: list) -> list[tuple]:
    """Возваращет приведенный к общему виду список данных, полученных из API для hh.ru"""
    vacancy_hh = []
    for i in vacancy_list[0]['items']:
        emp_id = i['employer']['id']
        name = i['name']
        url = i['apply_alternate_url']
        description = i['snippet']['responsibility']
        city = i['area']['name']
        publication_date = i['published_at']
        salary_from = i['salary']['from'] if i['salary'] and i['salary']['from'] is not None else 0
        salary_to = i['salary']['to'] if i['salary'] and i['salary']['to'] is not None else 0
        data_tuple = (
                        name,
                        int(emp_id),
                        url,
                        description,
                        city,
                        get_publication_date(publication_date),
                        salary_from,
                        salary_to
                      )
        vacancy_hh.append(data_tuple)
    return vacancy_hh


def get_publication_date(time: str) -> str:
    """Возвращает дату публикации в форматированном виде"""
    index = time.index('T')
    publication_date = time[:index]
    return publication_date
