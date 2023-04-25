import requests


class HH:
    def __init__(self, employer_id: str):
        """Получение вакансий с hh.ru по API"""
        self.employer_id = employer_id
        self.vacancy_list = []
        self.data = requests.get(f"https://api.hh.ru/vacancies?",
                                 params={'employer_id': employer_id}).json()
        self.vacancy_list.append(self.data)

    def get_request(self) -> list:
        """Возвращает список вакансий с hh.ru"""
        return self.vacancy_list
