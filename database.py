import psycopg2
import pandas as pd


class DBManager:
    """Класс для работы с базой данных"""

    def __init__(self, database_name: str, params: dict):
        """Инициализация класса"""
        self.database_name = database_name
        self.params = params

    def create_database_and_tables(self) -> None:
        """Создание базы данных и таблиц для сохранения данных о работодателе и вакансии"""

        conn_postgres = psycopg2.connect(database='postgres', **self.params)
        conn_postgres.autocommit = True

        with conn_postgres.cursor() as cur:
            try:
                cur.execute(f"DROP DATABASE {self.database_name}")
            except Exception:
                print("База данных создана")
            finally:
                cur.execute(f"CREATE DATABASE {self.database_name}")

        conn_postgres.close()

        with psycopg2.connect(database=self.database_name, **self.params) as conn_hh:
            with conn_hh.cursor() as cur:
                cur.execute("""
                        CREATE TABLE companies (
                                employer_id int PRIMARY KEY,
                                company_name varchar(100) NOT NULL
                                )
                            """)

                cur.execute("""
                        CREATE TABLE vacancies (
                                vacancy_id SERIAL PRIMARY KEY,
                                title VARCHAR(100) NOT NULL,
                                employer_id INTEGER,
                                url text,
                                description text,
                                city VARCHAR(100),
                                publication_date date,
                                salary_from INTEGER,
                                salary_to INTEGER
                                )
                            """)

    def insert_to_table_companies(self, content: dict) -> None:
        """Добавление значений в таблицу companies"""
        with psycopg2.connect(database=self.database_name, **self.params) as conn_hh:
            with conn_hh.cursor() as cur:
                cur.execute("TRUNCATE TABLE companies RESTART IDENTITY ")
                for key, value in content.items():
                    tuple_data = int(key), value
                    cur.execute("INSERT INTO companies (employer_id, company_name) "
                                "VALUES (%s, %s)", tuple_data)

    def insert_to_table_vacancies(self, data_list: list) -> None:
        """ Добавление значений в таблицу companies """

        with psycopg2.connect(database=self.database_name, **self.params) as conn_hh:
            with conn_hh.cursor() as cur:
                for i in data_list:
                    cur.execute("INSERT INTO vacancies "
                                "(title, employer_id, url, description, city, publication_date, salary_from, salary_to) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", i)

    def get_companies_and_vacancies_count(self) -> pd.DataFrame:
        """ Получает список всех компаний и количество вакансий у каждой компании"""
        with psycopg2.connect(database=self.database_name, **self.params) as conn_hh:
            with conn_hh.cursor() as cur:
                cur.execute(
                    """
                            SELECT company_name, COUNT(*) AS count_vacancies
                            FROM companies
                            JOIN vacancies USING(employer_id)
                            GROUP BY company_name
                            ORDER BY count_vacancies DESC  
                        """
                )
                rows = cur.fetchall()
        db_table = pd.DataFrame(rows, columns=['company_name', 'count_vacancies'], index=range(1, len(rows) + 1))

        return db_table

    def get_all_vacancies(self) -> pd.DataFrame:
        """ Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию. """
        with psycopg2.connect(database=self.database_name, **self.params) as conn_hh:
            with conn_hh.cursor() as cur:
                cur.execute(
                    """
                    SELECT company_name, title, salary_from, salary_to, url
                    FROM companies
                    JOIN vacancies USING(employer_id)
                    ORDER BY salary_from DESC
                    """
                )
                rows = cur.fetchall()
        db_table = pd.DataFrame(rows, columns=['company_name', 'title', 'salary_From', 'salary_to', 'url'],
                                index=range(1, len(rows) + 1))

        return db_table

    def get_avg_salary(self) -> pd.DataFrame:
        """ Получает среднюю зарплату по вакансиям. """
        with psycopg2.connect(database=self.database_name, **self.params) as conn_hh:
            with conn_hh.cursor() as cur:
                cur.execute(
                    """
                    SELECT AVG((salary_from + salary_to)/2) AS avg_salary 
                    FROM vacancies
                    WHERE salary_from != 0 OR salary_to != 0
                    """
                )
                rows = cur.fetchall()
        db_table = pd.DataFrame(rows, columns=['avg_salary'], index=range(1, len(rows) + 1))

        return db_table

    def get_vacancies_with_higher_salary(self) -> pd.DataFrame:
        """ Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        with psycopg2.connect(database=self.database_name, **self.params) as conn_hh:
            with conn_hh.cursor() as cur:
                cur.execute(
                    """
                    SELECT company_name, title, url, salary_from, salary_to
                    FROM companies
                    JOIN vacancies USING(employer_id)
                    WHERE salary_from > (SELECT AVG((salary_from + salary_to)/2) FROM vacancies 
                    WHERE salary_from != 0 OR salary_to != 0) OR 
                    salary_to > (SELECT AVG((salary_from + salary_to)/2) FROM vacancies 
                    WHERE salary_from != 0 OR salary_to != 0)
		            ORDER BY salary_from
                    """
                )
                rows = cur.fetchall()
            db_table = pd.DataFrame(rows, columns=['company_name', 'title', 'url', 'salary_from', 'salary_to'],
                                    index=range(1, len(rows) + 1))

        return db_table

    def get_vacancies_with_keyword(self, keyword: str) -> pd.DataFrame:
        """получает список всех вакансий, в названии которых содержится переданное в метод слово"""
        with psycopg2.connect(database=self.database_name, **self.params) as conn_hh:
            with conn_hh.cursor() as cur:
                cur.execute(
                    f"""   
                    SELECT company_name, title, url, salary_from, salary_to, publication_date
                    FROM vacancies
                    JOIN companies USING(employer_id)
                    WHERE title ILIKE '%{keyword}%'
                    ORDER BY publication_date
                """
                )
                rows = cur.fetchall()
        db_table = pd.DataFrame(rows, columns=['company_name', 'title', 'url', 'salary_from', 'salary_to',
                                               'publication_date'], index=range(1, len(rows) + 1))

        return db_table
