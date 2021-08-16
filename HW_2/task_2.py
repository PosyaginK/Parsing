# Развернуть у себя на компьютере/виртуальной машине/хостинге MongoDB и реализовать функцию, записывающую собранные вакансии в созданную БД.
# Библиотеки
import requests as req
from bs4 import BeautifulSoup as bs
import pandas as pd
from pymongo import MongoClient

# Headers
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0'
}

# Получаем html страницы по url
def get_html(url, params=''):
    html = req.get(url, headers=HEADERS, params=params)
    return html

# HeadHanter
URL = 'https://hh.ru/search/vacancy'

# Получаем контент со страницы html
def get_hh_content(html):
    soup = bs(html, 'lxml')
    items = soup.find_all('div', class_='vacancy-serp-item')
    vacancy = []

    for item in items:
        vacancy.append(
            {
                'site': 'HeadHanter', # Название сайта
                'title': item.find('div', class_='vacancy-serp-item__info').get_text(), # Название вакансии
                'link': item.find('div', class_='vacancy-serp-item__info').find('a').get('href'), # Ссылка на вакансию
                'salary': item.find('div', class_='vacancy-serp-item__sidebar').get_text(), # Зарплата
                'city': item.find('span', class_='vacancy-serp-item__meta-info').get_text(), # Город
                'organization': item.find('div', class_='vacancy-serp-item__meta-info-company').get_text(), # Название компании
                'note': item.find('div', class_='vacancy-label') # Примечание
            }
        )
    # Приведем данные к нормальному виду
    for i in vacancy:
        # Salary
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
    return vacancy

# Главная функция
def parser_hh():
    POST = str(input('Введите название вакансии для парсинга: '))
    PAGES = int(input('Количество страниц для парсинга: '))
    html = get_html(URL)
    if html.status_code == 200:
        vacancy = []
        for page in range(1, PAGES + 1):
            print(f'Парсим страницу {page}')
            html = get_html(URL, params={'text': POST, 'page': page})
            vacancy.extend(get_hh_content(html.text))
        result = pd.DataFrame(vacancy)
    else:
        print('error')
    return result


# SuperJob
HOST = 'https://superjob.ru' # Нужен для получения полных ссылок на вакансии.
URL = 'https://superjob.ru/vacancy/search/'

# Получаем контент со страницы html
def superjob_get_content(html):
    soup = bs(html, 'lxml')
    items = soup.find_all('div', class_='f-test-vacancy-item')

    vacancy = []
    for item in items:
        vacancy.append(
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
    for v in vacancy:
        # link
        v['link'] = HOST + v['link']
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
    return vacancy

# Главная функция
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
        result = pd.DataFrame(vacancy)
    else:
        print('error')
    return result

# ОБЪЕДИНИМ ПАРСИНГ ДВУХ САЙТОВ В ОДНУ ФУНКЦИЮ
def main_parsing():
    HH_URL = 'https://hh.ru/search/vacancy'
    SJ_URL = 'https://superjob.ru/vacancy/search/'

    POST = str(input('Введите название вакансии для парсинга: '))
    PAGES = int(input('Количество страниц для парсинга: '))
    HH_HTML = get_html(HH_URL)
    SJ_HTML = get_html(SJ_URL)

    if HH_HTML.status_code == 200 and SJ_HTML.status_code == 200:
        vacancy = []
        for page in range(1, PAGES + 1):
            print(f'Парсятся страницы {page}')
            hh = get_html(HH_URL, params={'text': POST, 'page': page})
            sj = get_html(SJ_URL, params={'keywords': POST, 'page': page})
            vacancy.extend(get_hh_content(hh.text) + superjob_get_content(sj.text))
        result = pd.DataFrame(vacancy)
    else:
        print('error')
    return result

# # df = main_parsing()
# print(df)
# print(df.loc[df['site'] == 'SuperJob'].head(5))
# print(df.loc[df['site'] == 'HeadHanter'].head(5))
# Сохраним в файл csv
# df.to_csv('13082021', index=False)
# ВЗЯЛ отсрочку до 20.08.2021
