import datetime
import json
import math
import os.path
import random
import re
import time

import grequests
import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from Data import proxies


def exception_handler(request: requests.Request, exception: Exception) -> requests.Response:
    get = requests.get(request.url, headers={'user-agent': UserAgent().random}, proxies=random.choice(proxies))
    get.close()
    return get


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
        request = requests.get(f'{bank_list_url}{i}', headers={'user-agent': UserAgent().random},
                               proxies=random.choice(proxies))
        request.close()
        root = request.content

        root = BeautifulSoup(root, 'lxml')
        tbody = root.find('tbody')
        trs = tbody.find_all('tr')

        bank_info_jsons = root.find_all('script', type='application/ld+json')

        for tr in trs:
            tds = tr.find_all('td')

            bank = pd.Series()
            bank['place'] = int(tds[0].text.strip())
            bank['url'] = tds[1].find('a')['href']
            bank['reviews_count'] = int(''.join(tds[3].text.strip().split()))
            bank['answers_count'] = int(''.join(tds[4].text.strip().split()))
            bank['solve_percent'] = int(tds[5].text.strip()[:-1])

            while len(bank_info_jsons) > 0:
                bank_info_json = str(bank_info_jsons[0]).strip()
                bank_info_jsons = bank_info_jsons[1:]

                bank_info_json = re.sub('(\\t|\\n|\\r|\\xa0)', '', bank_info_json)
                bank_info_json = re.sub('(&lt;(.{2}|.{1})&gt;|&lt;\/.{2}&gt;)', '', bank_info_json)
                bank_info_json = re.sub('\\\\', ' ', bank_info_json)
                bank_info_json = re.sub('&quot;', "'", bank_info_json)

                bank_info_json = bank_info_json[bank_info_json.find('>') + 1:]
                bank_info_json = bank_info_json[: bank_info_json.find('</script>')]

                bank_info_json = json.loads(bank_info_json)

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
    columns=['bank_index', 'author', 'published', 'name', 'text', 'rating', 'best_rating', 'worst_rating', 'url',
             'is_credited', 'is_verified', 'is_answered', 'is_in_check', 'is_solved']
)

# a = pd.read_pickle('reviews/Альфа-Банк/Альфа-Банк.pkl.zip', compression='zip')
# print(a.head())
# print(a.shape)
# exit()

f = open('logfile.txt', 'w')

for index, value in banks.iterrows():
    # 28, 38, 63, 110, 121, 137, 145, 147, 155, 161, 167, 168, 170, 174, 175, 177, 178, 179, 180
    if index < 130:
        continue

    # if value['reviews_count'] > 100000:
    #     break

    print(f'{index}.{value["name"]}: parsing...')
    page_count = math.ceil(value['reviews_count'] / 25)
    if page_count < 10:
        page_count = 10

    tm = time.time()

    gets = (grequests.get(f'{site_url}{value["url"]}?type=all&page={i}', headers={'user-agent': UserAgent().random},
                          proxies=random.choice(proxies))
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

            a = root.find_all('a', {'href': re.compile(r'^\/services\/responses\/bank\/response.*'),
                                    'data-gtm-click': '{"event":"GTM_event","eventCategory":"ux_data","eventAction":"click_responses_response_user_rating_banks"}'})

            from_html = [i.parent.parent.parent for i in a if '#' not in i['href']]
            from_html = from_html[::2]

            a = {}
            reviews_html = {}
            for i in from_html:
                name = i.find("a").text.strip().replace('"', "'").replace('\xa0', '').replace('&amp;', '&')\
                    .replace('&#039;', "'")
                published = i.find_all("span")
                published = published[-1].text if '.' in published[-1].text else published[-3].text
                published = datetime.datetime.strptime(published.strip(), '%d.%m.%Y %H:%M')
                published = published.strftime('%Y-%m-%d %H:%M')

                key = f'{name}{published}'
                a[key] = i.find('a')['href']
                reviews_html[key] = i

            json_html = root.find('script', type='application/ld+json')

            json_html = str(json_html).strip()
            json_html = re.sub('(\\t|\\r|\\xa0)', '', json_html)
            json_html = re.sub('(\\n)', ' ', json_html)
            json_html = re.sub('(&lt;(.{2}|.{1})&gt;|&lt;\/.{2}&gt;)', '', json_html)
            json_html = re.sub('\\\\', ' ', json_html)
            json_html = re.sub('&quot;', "'", json_html)
            json_html = re.sub('&amp;', "&", json_html)
            json_html = re.sub('&#039;', "'", json_html)

            json_html = json_html[json_html.find('>') + 1:]
            json_html = json_html[: json_html.find('</script>')]

            bank_info_json = json.loads(json_html)
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

                key = f'{review["name"]}{review["published"][:-3]}'
                number = a[key]
                number = re.search(r'\d+', number).group(0)
                review['url'] = number

                review_html = reviews_html[key]
                review_html = str(review_html).lower()
                review['is_credited'] = 1 if 'зачтено' in review_html else 0
                review['is_solved'] = 1 if 'проблема решена' in review_html else 0
                review['is_in_check'] = 1 if 'проверяется' in review_html else 0
                review['is_verified'] = 1 if 'отзыв проверен' in review_html else 0
                review['is_answered'] = 1 if 'ответ банка' in review_html else 0

                # reviews_html Зачтено, Отзыв проверен, Ответ банка

                reviews = pd.concat([reviews, review.to_frame().T])
        except Exception as e:
            print(response.url)
            print(e)
            try:
                f.write(response.url)
                f.write(e)
                f.write(json_html)
                f.write('\n')
                print(json_html)
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

f.close()
