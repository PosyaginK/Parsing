"""
Вариант 1

Необходимо собрать информацию о вакансиях на вводимую должность (используем input или через аргументы получаем должность) с сайтов HH(обязательно) и/или Superjob(по желанию). Приложение должно анализировать несколько страниц сайта (также вводим через input или аргументы). Получившийся список должен содержать в себе минимум:

Наименование вакансии.
Предлагаемую зарплату (разносим в три поля: минимальная и максимальная и валюта. цифры преобразуем к цифрам).
Ссылку на саму вакансию.
Сайт, откуда собрана вакансия.

По желанию можно добавить ещё параметры вакансии (например, работодателя и расположение). Структура должна быть одинаковая для вакансий с обоих сайтов. Общий результат можно вывести с помощью dataFrame через pandas. Сохраните в json либо csv.
"""

import requests
from bs4 import BeautifulSoup as bs
import pandas as pd

# Headers
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0'
}

# Получаем html страницы по url
def get_html(url, params=''):
    response = requests.get(url, headers=HEADERS, params=params)
    return response

# HeadHanter
URL_HH = 'https://hh.ru/search/vacancy'

# SuperJob
HOST_SJ = 'https://superjob.ru' # Нужен для получения полных ссылок на вакансии.
URL_SJ = 'https://superjob.ru/vacancy/search/'


# Получаем контент со страницы html
def get_hh_content(response):
    soup = bs(response, 'lxml')
    items = soup.find_all('div', class_='vacancy-serp-item')
    hh_vacancy = []

    for item in items:
        hh_vacancy.append(
            {
                'site': 'HeadHanter',  # Название сайта
                'title': item.find('div', class_='vacancy-serp-item__info').get_text(),  # Название вакансии
                'link': item.find('div', class_='vacancy-serp-item__info').find('a').get('href'),  # Ссылка на вакансию
                'salary': item.find('div', class_='vacancy-serp-item__sidebar'),  # Зарплата
                'city': item.find('span', class_='vacancy-serp-item__meta-info').get_text(),  # Город
                'organization': item.find('div', class_='vacancy-serp-item__meta-info-company').get_text(),
                # Название компании
                'note': item.find('div', class_='vacancy-label')  # Примечание
            }
        )
    # Приведем данные к нормальному виду
    for i in hh_vacancy:
        # Salary
        try:
            i['salary'] = i['salary'].text
        except:
            i['salary'] = None
        if i['salary']:
            salary_list = i['salary'].split(' ')
            if salary_list[0] == 'от':
                i['salary_min'] = salary_list[1]
                i['salary_max'] = None
            elif salary_list[0] == 'до':
                i['salary_min'] = None
                i['salary_max'] = salary_list[1]
            else:
                i['salary_min'] = salary_list[0]
                i['salary_max'] = salary_list[2]
            i['salary_currency'] = salary_list[-1]
        else:
            i['salary_min'] = None
            i['salary_max'] = None
            i['salary_currency'] = None
        i.pop('salary')
        # note
        if i['note'] != None:
            i['note'] = i['note'].get_text()
        # City
        if i['city']:
            city_list = i['city'].split(',')
            i['city'] = city_list[0]
    return hh_vacancy


def superjob_get_content(html):
    soup = bs(html, 'lxml')
    items = soup.find_all('div', class_='f-test-vacancy-item')

    sj_vacancy = []
    for item in items:
        sj_vacancy.append(
            {
                'site': 'SuperJob',  # Название сайта
                'title': item.find('a').get_text(),  # Название вакансии
                'link': item.find('a').get('href'),  # Ссылка на вакансию
                'salary': item.find('span', class_='f-test-text-company-item-salary').get_text(),  # Зарплата
                'city': item.find('span', class_='f-test-text-company-item-location').get_text(),  # Город
                'organization': item.find('span', class_='f-test-text-vacancy-item-company-name').get_text(),
                # Название компании
                'note': item.find('span', class_='f-test-badge')  # Примечание
            }
        )
    # Почистим данные
    for v in sj_vacancy:
        # link
        v['link'] = HOST_SJ + v['link']
        # salary
        if v['salary'] != 'По договорённости':
            salary_list = v['salary'].split('\xa0')
            if salary_list[0] == 'от':
                v['salary_min'] = salary_list[1] + salary_list[2]
                v['salary_max'] = None
            elif salary_list[0] == 'до':
                v['salary_min'] = None
                v['salary_max'] = salary_list[1] + salary_list[2]
            elif len(salary_list) == 3:
                v['salary_min'] = salary_list[0] + salary_list[1]
                v['salary_max'] = salary_list[0] + salary_list[1]
            else:
                v['salary_min'] = salary_list[0] + salary_list[1]
                v['salary_max'] = salary_list[3] + salary_list[4]
            v['salary_currency'] = salary_list[-1].split('/')[0]
        else:
            v['salary_min'] = None
            v['salary_max'] = None
            v['salary_currency'] = None
        v.pop('salary')
        # note
        if v['note'] != None:
            v['note'] = v['note'].get_text()
        # city
        city_split = v['city'].split(' ')
        if len(city_split[2]) >= 3:
            v['city'] = city_split[2]
        else:
            v['city'] = city_split[3]

    return sj_vacancy

# Главные функции
def parser_hh():
    post = str(input('Введите название вакансии для парсинга: '))
    pages = int(input('Количество страниц для парсинга: '))
    html = get_html(URL)
    if html.status_code == 200:
        vacancy = []
        for page in range(1, pages + 1):
            print(f'Парсим страницу {page}')
            html = get_html(URL, params={'text': post, 'page': page})
            vacancy.extend(get_hh_content(html.text))
        hh_result = pd.DataFrame(vacancy)
    else:
        hh_result = html.status_code
    return hh_result

def parser_sj():
    POST = str(input('Введите название вакансии для парсинга: '))
    PAGES = int(input('Количество страниц для парсинга: '))
    html = get_html(URL)
    if html.status_code == 200:
        vacancy = []
        for page in range(1, PAGES + 1):
            print(f'Парсим страницу {page}')
            html = get_html(URL, params={'keywords': POST, 'page': page})
            vacancy.extend(superjob_get_content(html))
        sj_result = pd.DataFrame(vacancy)
    else:
        print('error')
    return sj_result

# ОБЪЕДИНИМ ПАРСИНГ ДВУХ САЙТОВ В ОДНУ ФУНКЦИЮ
def main_parsing():
    HH_URL = 'https://hh.ru/search/vacancy'
    SJ_URL = 'https://superjob.ru/vacancy/search/'

    post = str(input('Введите название вакансии для парсинга: '))
    pages = int(input('Количество страниц для парсинга: '))
    HH_HTML = get_html(HH_URL)
    SJ_HTML = get_html(SJ_URL)

    if HH_HTML.status_code == 200 and SJ_HTML.status_code == 200:
        vacancy = []
        for page in range(1, pages + 1):
            print(f'Парсятся страницы {page}')
            hh = get_html(HH_URL, params={'text': post, 'page': page})
            sj = get_html(SJ_URL, params={'keywords': post, 'page': page})
            vacancy.extend(get_hh_content(hh.text) + superjob_get_content(sj.text))
        result = pd.DataFrame(vacancy)
    else:
        print('error')
    return result

df = main_parsing()
print(df)

df.to_csv('data.csv', sep=',', index=False, encoding='utf-8')