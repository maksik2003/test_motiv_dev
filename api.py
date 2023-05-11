from fastapi import FastAPI
import psycopg2
import requests

app = FastAPI()

API_CURRENCY = 'bb51c444c3f74133ab4fa699e89a2222' # API ключ доступа к сайту с курсом валют
BASE_CURRENCY = 'USD'
TO_CURRENCY = 'RUB'

def get_data(cursor, id_: int) -> dict:
    # Получаем данные о домене из БД по ID
    cursor.execute(
        """
            SELECT domain, includetime, entrytype, hash, decision_date, decision_number, decision_org, hash_int
            FROM content
            WHERE id = {id}
            LIMIT 1
        """.format(
            id = id_
        )
    )
    req = cursor.fetchone()
    # Переводим переменные в понятный вид
    domain = req[0]
    includetime = req[1]
    entrytype = req[2]
    hash_ = req[3]
    decision_date = req[4]
    decision_number = req[5]
    decision_org = req[6]
    hash_int = req[7]

    def get_ip_or_url(id_: int, table: str, column: str) -> list:
        # Функция для получения IPv4, IPv6, URLS из таблиц
        cursor.execute("""
            SELECT {column} FROM {table}
            WHERE id = {id}
        """.format(
            column = column,
            table = table,
            id = id_
        ))
        data = cursor.fetchall()
        result = []
        for i in data:
            result.append(i[0])
        return result
    
    def get_weather(id_) -> dict:
        # Функция для получения погоды из БД
        cursor.execute("SELECT temp, request_time FROM weather WHERE id = {id} LIMIT 1".format(id = id_))
        result = cursor.fetchone()
        return {
            'temp': result[0],
            'request_time': result[1]
        }

    return {
        'id': id_,
        'domain': domain,
        'includetime': includetime,
        'entrytype': entrytype,
        'hash': hash_,
        'decision': {
            'date': decision_date,
            'number': decision_number,
            'org': decision_org,
        },
        'hash_int': hash_int,
        'ipv4': get_ip_or_url(id_, table = 'ipv4', column = 'ip'),     # Получаем список IPv4
        'ipv6': get_ip_or_url(id_, table = 'ipv6', column = 'ip'),     # Получаем список IPv6
        'urls': get_ip_or_url(id_, table = 'urls', column = 'url'),    # Получаем список URL
        'weather': get_weather(id_)
    }

@app.get('/api/info')
def get_info(domain: str = None, hash = None) -> dict:

    try:
        connect = psycopg2.connect(
            user = 'postgres',
            password = '1020Love',
            host = 'localhost',
            port = '5432',
            database = 'postgres'
        )
        cursor = connect.cursor()

    except:
        return {'result': {
            'status': 500
        }}

    # Если подключение было успешно создано
    # Проверяем чтобы было введено только одно значение, либо hash, либо domain и далаем запрос в БД для получения id
    if hash and not domain:
        cursor.execute("""
            SELECT id FROM content
            WHERE hash_int = '{hash_int}'
            LIMIT 1
        """.format(
            hash_int = hash
        ))
        id_ = cursor.fetchone()

    elif domain and not hash:
        cursor.execute("""
            SELECT id FROM content
            WHERE domain = '{domain}'
            LIMIT 1
        """.format(
            domain = domain
        ))
        id_ = cursor.fetchone()
    
    else:
        return {'result': {
            'status': 400
        }}
    
    if not id_:
        # Проверка что был получен ID
        # Условие возникает если ID не получен
        return {'result': {
            'status': 200,
            'result': {}
        }}
    
    # Если до этого пункта не был вызван return, то выводим полученный результат
    return {'result': {
        'status': 200,
        'result': get_data(cursor, id_[0])
    }}

@app.get('/api/rates')
def get_currency(base: str = BASE_CURRENCY, to: str = TO_CURRENCY) -> dict:
    data = requests.get(
        url = 'https://openexchangerates.org/api/latest.json',
        params = {
            'app_id': API_CURRENCY,
            'base': base,
            'symbols': to
        }
    ).json()
    return {'result': {
        'status': 200,
        'result': data['rates'][to]
    }}