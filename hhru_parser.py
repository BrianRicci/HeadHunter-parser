import fake_useragent
import time
from bs4 import BeautifulSoup
import pandas as pd
import asyncio
import aiohttp
import csv
import os

start_time = time.time()

jobs = {
        'title': [],
        'link': []
}


def upload_to_csv(jobs: dict) -> None:
    # проверка на наличие папки с csv в директории
    if not os.path.isdir('csv_files'):
        os.mkdir('csv_files')

    current_time = time.strftime('%Y-%m-%d_%H-%M-%S')
    file_path = f'csv_files/jobs_{current_time}.csv'

    with open(file_path, mode='w', encoding='utf-8') as file:
        file_writer = csv.writer(file, delimiter=',', lineterminator='\n')
        file_writer.writerow(['title', 'link'])

        for i in range(len(jobs['title'])):
            file_writer.writerow([jobs['title'][i], jobs['link'][i]])


def create_data_table(data: dict) -> None:
    'Функция для создания таблицы с помощью pandas, принимает в себя словарь data.'
    table = pd.DataFrame(data)
    print(table)


async def get_page_data(session, page, target):
    LINK = f'https://makhachkala.hh.ru/search/vacancy?text={target}&salary=&ored_clusters=true&enable_snippets=true&area=113&page={page}'

    headers = {'user-agent': fake_useragent.UserAgent().random}

    async with session.get(LINK, headers=headers) as response:
        response_text = await response.text()
        soup = BeautifulSoup(response_text, 'lxml')

        try:
            jobs_block = soup.find('div', id='a11y-main-content')
            title = [item.text for item in jobs_block.find_all('a', {'class': 'serp-item__title'})]
            link = [item['href'] for item in jobs_block.find_all('a', {'class': 'serp-item__title'})]

            jobs['title'] += title
            jobs['link'] += link
            print(f'Запрос вакансий #{page}')
        except AttributeError:
            print(f'Неудавшийся запрос вакансий #{page}')
            return


async def gather_data(target):
    LINK = f'https://makhachkala.hh.ru/search/vacancy?text={target}&salary=&ored_clusters=true&enable_snippets=true&area=113&page=1'
    headers = {'user-agent': fake_useragent.UserAgent().random}

    async with aiohttp.ClientSession() as session:
        response = await session.get(LINK, headers=headers)
        soup = BeautifulSoup(await response.text(), 'lxml')
        pager_block = soup.find('div', {'class': 'pager'})
        pages_count = int(pager_block.find_all('a', {'class': 'bloko-button', 'data-qa': 'pager-page'})[-1].text)
        tasks = []

        for page in range(pages_count):
            await asyncio.sleep(.3) # 300мс по тестам самое оптимальное время
            task = asyncio.create_task(get_page_data(session, page, target))
            tasks.append(task)

        await asyncio.gather(*tasks)


def main():
    asyncio.run(gather_data('Python'))
    finish_time = time.time() - start_time
    print(f'Время исполнения запросов: {finish_time}')
    create_data_table(jobs)

    upload = input('Добавить данные в csv?[y, n]: ')
    if upload in ('y', 'н'):
        # try:
        upload_to_csv(jobs)
        # print('Загрузка в csv прошла в успешно.')
        # except Exception:
        #     print('Возникла ошибка при загрузке в csv.')


if __name__ == '__main__':
    main()
