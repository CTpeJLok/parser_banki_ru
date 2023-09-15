import datetime
import json
import math
import os.path
import re
import time

import grequests
import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from .Data import proxy

# pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

site_url = 'https://www.banki.ru'
bank_list_url = f'{site_url}/services/responses/?page='

if os.path.isfile('banks.csv'):
    banks = pd.read_csv('banks.csv')
else:
    banks = pd.DataFrame(
        columns=['name', 'place', 'url', 'rating', 'reviews_count', 'answers_count', 'solve_percent', 'best_rating',
                 'worst_rating']
    )

    for i in range(1, 7 + 1):
        request = requests.get(f'{bank_list_url}{i}', headers={'user-agent': UserAgent().random}, proxies=proxy)
        request.close()
        root = request.content

        root = BeautifulSoup(root, 'lxml')
        tbody = root.find('tbody')
        trs = tbody.find_all('tr')

        scripts = root.find_all('script', type='application/ld+json')

        for tr in trs:
            tds = tr.find_all('td')

            bank = pd.Series()
            bank['place'] = int(tds[0].text.strip())
            bank['url'] = tds[1].find('a')['href']
            bank['reviews_count'] = int(''.join(tds[3].text.strip().split()))
            bank['answers_count'] = int(''.join(tds[4].text.strip().split()))
            bank['solve_percent'] = int(tds[5].text.strip()[:-1])

            while len(scripts) > 0:
                script = str(scripts[0]).strip()

                scripts = scripts[1:]

                script = re.sub('(\\t|\\n|\\r|\\xa0)', '', script)
                script = re.sub('(&lt;(.{2}|.{1})&gt;|&lt;\/.{2}&gt;)', '', script)
                script = re.sub('\\\\', ' ', script)
                script = re.sub('&quot;', "'", script)

                script = script[script.find('>') + 1:]
                script = script[: script.find('</script>')]

                bank_info_json = json.loads(script)

                if bank_info_json['@type'] != 'Product':
                    break

                bank['name'] = bank_info_json['name']
                bank_rating = bank_info_json['aggregateRating']

                bank['rating'] = bank_rating['ratingValue']
                bank['best_rating'] = bank_rating['bestRating']
                bank['worst_rating'] = bank_rating['worstRating']
                bank['reviews_count'] = int(bank_rating['ratingCount'])
                bank['answers_count'] = int(bank_rating['reviewCount'])
                break

            banks = pd.concat([banks, bank.to_frame().T])

    banks = banks.drop_duplicates()
    banks = banks.sort_values(['reviews_count'])
    banks.to_csv('banks.csv', index=False)

banks.reset_index(inplace=True)
print(banks.head())
print(banks.info())

reviews = pd.DataFrame(
    columns=['bank_index', 'author', 'published', 'name', 'text', 'rating', 'best_rating', 'worst_rating', 'url']
)


def exception_handler(request: requests.Request, exception: Exception) -> requests.Response:
    get = requests.get(request.url, headers={'user-agent': UserAgent().random}, proxies=proxy)
    get.close()
    return get


# a = pd.read_pickle('reviews/Альфа-Банк/Альфа-Банк.pkl.zip', compression='zip')
# print(a.head())
# print(a.shape)
# exit()


for index, value in banks.iterrows():
    # 155, 160, 161, 167, 168, 170, 171, 172, 173, 174, 175
    if index < 180:
        continue

    # if value['reviews_count'] > 100000:
    #     continue

    print(f'{index}.{value["name"]}: parsing...')
    page_count = math.ceil(value['reviews_count'] / 25)
    if page_count < 10:
        page_count = 10

    tm = time.time()

    gets = (grequests.get(f'{site_url}{value["url"]}?type=all&page={i}', headers={'user-agent': UserAgent().random},
                          proxies=proxy)
            for i in range(1, page_count + 1))
    responses = grequests.map(gets, size=16, exception_handler=exception_handler)

    print(f'{index}.{value["name"]}: {len(responses)} download ok')

    k = -1
    for response in responses:
        k += 1
        if k % 100 == 0:
            print(f'{index}.{value["name"]}: parsed {k}')

        try:
            root = response.content

            root = BeautifulSoup(root, 'lxml')
            # f = open('saved.html', 'w', encoding='utf-8')
            # f.writelines(str(root))
            # f.close()

            a = root.find_all('a', {'href': re.compile(r'^\/services\/responses\/bank\/response.*'),
                                    'data-gtm-click': '{"event":"GTM_event","eventCategory":"ux_data","eventAction":"click_responses_response_user_rating_banks"}'})
            reviews_html = [i.parent.parent.parent for i in a if '#' not in i['href']]
            a = reviews_html[::2]
            res = {}
            for i in a:
                name = i.find("a").text.strip().replace('"', "'").replace('\xa0', '')
                published = i.find_all("span")
                published = published[-1].text if '.' in published[-1].text else published[-3].text
                published = datetime.datetime.strptime(published.strip(), '%d.%m.%Y %H:%M')
                published = published.strftime('%Y-%m-%d %H:%M')
                res[f'{name}{published}'] = i.find('a')['href']
            a = res.copy()

            script = root.find('script', type='application/ld+json')

            script = str(script).strip()
            script = re.sub('(\\t|\\r|\\xa0)', '', script)
            script = re.sub('(\\n)', ' ', script)
            script = re.sub('(&lt;(.{2}|.{1})&gt;|&lt;\/.{2}&gt;)', '', script)
            script = re.sub('\\\\', ' ', script)
            script = re.sub('&quot;', "'", script)

            script = script[script.find('>') + 1:]
            script = script[: script.find('</script>')]

            bank_info_json = json.loads(script)
            reviews_list = bank_info_json['review']

            for review_json in reviews_list:
                review = pd.Series()
                review['bank_index'] = index

                review['author'] = review_json['author'].strip()
                review['published'] = review_json['datePublished'].strip()
                review['name'] = review_json['name'].strip()
                review['text'] = review_json['description'].strip()

                review_rating = review_json['reviewRating']
                review['rating'] = review_rating['ratingValue'].strip()
                review['best_rating'] = review_rating['bestRating'].strip()
                review['worst_rating'] = review_rating['worstRating'].strip()

                review['url'] = a[f'{review["name"]}{review["published"][:-3]}']

                # reviews_html Зачтено, Отзыв проверен, Ответ банка

                reviews = pd.concat([reviews, review.to_frame().T])
        except Exception as e:
            print(response.url)
            print(e)
            try:
                print(script)
            except:
                pass

    path = f'reviews/{value["name"]}/'
    path = path.replace('|', ' ')
    if not os.path.isdir(path):
        os.mkdir(path)

    bank_reviews = reviews[reviews['bank_index'] == index]
    bank_reviews.to_csv(f'{path}{value["name"]}.csv.zip'.replace('|', ' '), compression='zip', index=False)
    bank_reviews.to_pickle(f'{path}{value["name"]}.pkl.zip'.replace('|', ' '), compression='zip')

    print(f'{index}.{value["name"]}: {time.time() - tm}')
