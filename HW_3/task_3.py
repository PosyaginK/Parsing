# Развернуть у себя на компьютере/виртуальной машине/хостинге MongoDB и реализовать функцию,
# записывающую собранные вакансии в созданную БД.
import pymongo
from pymongo import MongoClient
from task_2 import main_parsing


# Подготовим базу данных
client = MongoClient('localhost', 27017)
db = client.db_vacancy # Создаем базу данных
collection = db.vacancy_17082021

# Функция для добавление вакансий в базу
def insert_vavation(vacancy):
    for v in vacancy:
        collection.insert_one(v)
    return collection

# Получим датафрейм с вакансиями
vacancy = main_parsing()

print(vacancy)

# Загрузим наши вакансии в базу
insert_vavation(vacancy)

# Посмотрим вакансии с сайта SuperJob
# for item in collection.find({'site': 'SuperJob'}):
#     print(item)


