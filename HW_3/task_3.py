# Развернуть у себя на компьютере/виртуальной машине/хостинге MongoDB и реализовать функцию,
# записывающую собранные вакансии в созданную БД.
import pymongo
from pymongo import MongoClient
from HW_2.task_2 import main_parsing

# Подготовим базу данных
client = MongoClient('localhost', 27017)
db = client.db_vacancy # Создаем базу данных
collection = db.vacancy_13082021

# Функция для добавление вакансий в базу
def insert_vavation(vacancy):
    for v in vacancy:
        collection.insert_one(v)
    return collection

# Получим датафрейм с вакансиями
vacancy = main_parsing()

# Загрузим наши вакансии в базу
insert_vavation(vacancy)

# Посмотрим вакансии с сайта SuperJob
for item in collection.find({'site': 'SuperJob'}):
    print(item)

# Написать функцию, которая производит поиск и выводит на экран вакансии с заработной платой больше введённой суммы.

# def get_salary(vacancy):
#     sal = input('Введите размер заработной платы: ')
#     # $gt - >, $lt - <
#     for item in collection.find({
#         'salary_currency': 'руб.',
#         '$or': [{'salary_min': {'$lt' : sal}}, {'salary_max': {'$gt' : sal}}]
#         }):
#         print(item)
#
# get_salary(vacancy)