from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup as bs
from pprint import pprint
from pymongo import MongoClient
from pymongo.errors import InvalidDocument as idoc

# Подготовим базу данных
client = MongoClient('localhost', 27017)
db = client.headhunter
vacations = db.hh_vacations

# Headers
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0'
}

hh_url = 'https://hh.ru/search/vacancy'

# Получаем html страницы по url
def get_html(url, params=''):
    response = requests.get(url, headers=HEADERS, params=params)
    return response

# response = get_html(hh_url, params={'area': '113', 'text': 'python', 'page': 0})

def get_hh_content(response):
    soup = bs(response.text, 'html.parser')
    items = soup.find_all('div', class_='vacancy-serp-item')
    vacancy = []
    for item in items:
        title = item.find('span', attrs={'class': 'resume-search-item__name'})
        link = title.find('a').get('href')
        city = item.find('span', attrs={'class': 'vacancy-serp-item__meta-info'}).get_text()
        company = item.find('div', attrs={'class': 'vacancy-serp-item__meta-info-company'}).get_text()
        try:
            salary = item.find('div', attrs={'class': 'vacancy-serp-item__sidebar'}).get_text()
        except:
            salary = ''

        vacancy.append(
            {
                'title': title.get_text(),
                'link': link,
                'city': city,
                'company': company,
                'salary': salary,
                'site': 'HeadHunter'
             }
        )
    return vacancy

def edit_data_hh(vacancy):
    for vac in vacancy:
        # Del \xa0
        vac['company'] = vac['company'].replace(u'\xa0', u' ')
        vac['salary'] = vac['salary'].replace(u'\u202f', u'')

        # Salary (min, max, currency)
        if vac['salary']:
            salary_list = vac['salary'].split(' ')
            if salary_list[0] == 'от':
                vac['salary_min'] = float(salary_list[1])
                vac['salary_max'] = None
            elif salary_list[0] == 'до':
                vac['salary_min'] = None
                vac['salary_max'] = float(salary_list[1])
            else:
                vac['salary_min'] = float(salary_list[0])
                vac['salary_max'] = float(salary_list[2])
            vac['salary_currency'] = salary_list[-1]
        else:
            vac['salary_min'] = None
            vac['salary_max'] = None
            vac['salary_currency'] = None
        vac.pop('salary')

        # Id
        id = vac['link'].split(sep='/')[4].split(sep='?')[0]
        # id = id.split(sep='?')[0]
        vac['_Id'] = id
    return vacancy

def main():
    search_name = input('Job title to search: ')
    page_count = int(input('Number of pages to search: '))

    result = []
    for i in range(page_count):
        print(f'Page {i+1}')
        response = get_html(hh_url, params={'area': '113', 'text': search_name, 'page': i})
        vacancy = get_hh_content(response)
        vacancy_edit = edit_data_hh(vacancy)
        if response.ok and vacancy:
            result.extend(vacancy_edit)
        else:
            break

    return result

result = main()
# pprint(result)
"""
1. Развернуть у себя на компьютере/виртуальной машине/хостинге MongoDB и реализовать функцию. 
Добавить в решение со сбором вакансий(продуктов) функцию, которая будет добавлять 
только новые вакансии/продукты в вашу базу.
"""
# Добавляем данные в монго
def insert_mongo(result):
    for vac in result:
        try:
            vacations.update_one({'_Id': vac['_Id']}, {'$set': vac}, upsert=True)
        except idoc:
            print(f'Error {idoc}')
            continue

insert_mongo(result)


"""
2. Написать функцию, которая производит поиск и выводит на экран вакансии с заработной платой больше введённой суммы 
(необходимо анализировать оба поля зарплаты - минимальнную и максимульную). 
"""
def search_by_salary():
    value = int(input('Enter the salary value: '))
    res = vacations.find({'$or':
                              [{'salary_currency': 'руб.',
                                    '$or': [
                                        {'salary_min': {'$gt': value}},
                                        {'salary_max': {'$gt': value}},]
                                },
                                {'salary_currency': 'USD',
                                    '$or': [
                                        {'salary_min': {'$gt': value / 75}},
                                        {'salary_max': {'$gt': value / 75}},]
                                }]
                            })

    return list(res)

res = search_by_salary()
pprint(res)
