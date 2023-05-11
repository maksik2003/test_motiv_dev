from bs4 import BeautifulSoup
import datetime
import pandas as pd
import xmltodict, json
import psycopg2
import requests
import mail

FILE = 'dump.xml'                               # Файл, который надо распарсить
API_KEY = '4a0926c6c8637fa1c22880c061ae940f'    # Ключ для доступа к openweather
CITY = 'Ekaterinburg, RU'                       # Город

def create_connect():
    try:
        connect = psycopg2.connect(
            user = 'postgres',
            password = '1020Love',
            host = 'localhost',
            port = '5432',
            database = 'postgres'
        )
        cursor = connect.cursor()
        return connect, cursor

    except Exception as e:
        print(e)
        return False, False

def xml_to_dict() -> list:
    start_time = datetime.datetime.now()
    with open(FILE, 'r') as file:
        text = file.read()
        file.close()

    o = xmltodict.parse(text)

    data = []
    content = o['reg:register']['content']
    for item in content:
        to_add = {
            'id': int(item['@id']),
            'entryType': int(item['@entryType']),
            'includeTime': item['@includeTime'],
            'blockType': None,
            'hash': item['@hash'],
            'decision': {
                'date': item['decision']['@date'],
                'number': item['decision']['@number'],
                'org': item['decision']['@org']
            },
            'domain': None,
            'url': None,
            'ip': None,
            'ipv6': None
        }
        if 'domain' in item:
            if type({}) == type(item['domain']):
                to_add['domain'] = item['domain']['#text']
            else:
                to_add['domain'] = item['domain']

        if 'blockType' in item:
            to_add['blockType'] = item['@blockType']
        
        # Создаем списки, в которых далее будем хранить 
        ipv4_list = []
        ipv6_list = []
        url_list = []

        if 'ip' in item: # Проверяем что есть хоть один IP
            if type(ipv4_list) == type(item['ip']): # Если item['ip'] это список, то добавляем каждый ip в массив для добавления
                for ip in item['ip']:
                    if type({}) == type(ip):
                        ipv4_list.append(ip['#text'])
                        continue

                    ipv4_list.append(ip)
                    
            elif type('s') == type(item['ip']): # Если item['ip'] это строка, то добавляем единственный IP адрес в список
                ipv4_list.append(item['ip'])
        
        if 'ipv6' in item:
            if type(ipv6_list) == type(item['ipv6']): # Если item['ipv6'] это список, то добавляем каждый ip в массив для добавления
                for ip in item['ipv6']:
                    if type({}) == type(ip):
                        ipv6_list.append(ip['#text'])
                        continue

                    ipv6_list.append(ip)

            elif type('s') == type(item['ipv6']): # Если item['ipv6'] это строка, то добавляем единственный IP адрес в список
                ipv6_list.append(item['ipv6'])

        def del_symbol(s: str) -> str:
            if "'" in s:
                return s.replace("'", '')
            return s 

        if 'url' in item:
            if type({}) == type(item['url']): # Если URL - JSON
                to_add['url'] = del_symbol(item['url']['#text'])

            elif type('') == type(item['url']): # Если URL - строка
                url_list.append( del_symbol(item['url']) )

            elif type([]) == type(item['url']): # Если URL - массив
                for link in item['url']:
                    if type({}) == type(link): # Если URL - JSON
                        url_list.append( del_symbol(link['#text']) )
                        continue
                    url_list.append( del_symbol(link) )

        to_add['ip'] = ipv4_list
        to_add['ipv6'] = ipv6_list
        to_add['url'] = url_list

        data.append(to_add)

    # Вывод статистической информации
    print('[=] Время, потраченное на сбор информации в json формат = ' + str(datetime.datetime.now() - start_time))
    return data

def get_weather() -> None:
    url = 'http://api.openweathermap.org/data/2.5/find'
    request_res = requests.get(url = url, params = {
        'q': CITY,
        'type': 'like',
        'units': 'metric',
        'APPID': API_KEY
    } ).json()
    
    # Проверяем что в результате запроса были получены данные
    if request_res['list']:
        temp = request_res['list'][0]['main']['temp']
        time = datetime.datetime.now()
        return temp, time
    
def insert_weather(connect, cursor) -> None:
    start_time = datetime.datetime.now()
    cursor.execute('SELECT id FROM content')
    id_list = cursor.fetchall()

    temp, time = get_weather()
    for i in id_list:
        i = i[0]
        cursor.execute("""
            INSERT INTO weather
            VALUES ({id}, {temp}, '{timestamp}')
        """.format(
            id = i,
            temp = temp,
            timestamp = str(time)
        ))
        connect.commit()
    
    print('[=] Время, потраченное на внесение погоды в БД = ' + str(datetime.datetime.now() - start_time))

def insert_data_to_db(data, connect, cursor) -> None:
    start_time = datetime.datetime.now()
    try:
        for elem in data:
            if elem['blockType']:
                blockType = "'" + str(elem['blockType']) + "'"
            else:
                blockType = 'Null'
            
            # ({id}, '{domain}', {url}, '{includetime}', {entrytype}, {blocktype}, '{hash}', '{decision_date}', '{decision_number}', '{decision_org}')
            # ({id}, {domain}, {url}, {includetime}, {entrytype}, {blocktype}, {hash}, {decision_date}, {decision_number}, {decision_org})
            # print('Первый пошел')
            cursor.execute("""
                INSERT INTO content VALUES
                ({id}, '{domain}', '{includetime}', {entrytype}, {blocktype}, '{hash}', '{decision_date}', '{decision_number}', '{decision_org}', '{hash_int}')
            """.format(
                id = int(elem['id']),
                domain = elem['domain'],
                includetime = elem['includeTime'],
                entrytype = int(elem['entryType']),
                blocktype = blockType,
                hash = elem['hash'],
                decision_date = elem['decision']['date'],
                decision_number = elem['decision']['number'],
                decision_org = elem['decision']['org'],
                hash_int = str( int( elem['hash'], 16 ) )
            ))
            connect.commit()

            id_ = elem['id']
            # Заносим в БД IPv4
            if elem['ip']:
                for i in elem['ip']:
                    # print('Второй пошел')
                    cursor.execute("""
                        INSERT INTO ipv4(id, ip) VALUES( {id}, '{ip}' )
                    """.format(
                        id = id_,
                        ip = i
                    ))
                    connect.commit()

            # Заносим в БД IPv6
            if elem['ipv6']:
                for i in elem['ipv6']:
                    # print('Третий пошел')
                    cursor.execute("""
                        INSERT INTO ipv6(id, ip) VALUES( {id}, '{ip}' )
                    """.format(
                        id = id_,
                        ip = i
                    ))
                    connect.commit()

            # Заносим в БД URL
            if elem['url']:
                for i in elem['url']:
                    cursor.execute("""
                        INSERT INTO urls(id, url) VALUES( {id}, '{ip}' )
                    """.format(
                        id = id_,
                        ip = i
                    ))
                    connect.commit()
        
        insert_weather(connect, cursor) # Вносим погоду в БД

        print('[=] Время, потраченное на загрузку информации в БД = ' + str( datetime.datetime.now() - start_time ))
    
    except Exception as e:
        print(elem)
        print('[!] Возникла ошибка при загрузке данных в БД')
        print(e)

def count_distinct(cursor) -> int:
    cursor.execute("""
        SELECT COUNT(DISTINCT split_part(domain, '.', -2))
        FROM content
    """)

    return cursor.fetchone()[0]

if __name__ == '__main__':
    app_start_time = datetime.datetime.now()
    connect, cursor = create_connect()
    if not (connect and cursor):
        # Проверяем есть ли подключение к БД
        print('[!] Ошибка подключения к БД')
        exit()

    data = xml_to_dict()
    insert_data_to_db(data, connect, cursor)

    # Формируем сообщение для отправки:
    unique_domains = str( count_distinct(cursor) )
    message = [
        'Unique domains = ' + unique_domains,
        'Processed elements = ' + str( len( data ) ),
        'Process time = ' + str( datetime.datetime.now() - app_start_time )
    ]

    mail.send_mail(
        msg = '\n'.join(message)
    )
    print('[=] Кол-во уникальных доменов = ' + unique_domains)
    print('[=] Обработано элементов = ' + str( len( data ) ) )
    print('[=] Общее время работы приложения = ' + str( datetime.datetime.now() - app_start_time ))