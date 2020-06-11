import requests
import json
import logging
import time

logging.basicConfig(filename='logs.txt', format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(name=__name__)
logger.setLevel(logging.INFO)

auth_dict = {}
try:
    file = open('settings/auth.txt', 'r', encoding="utf-8")
    data = file.read()
    data = data.replace('\n', '')
except Exception as e:
    print("Не удалось загрузить auth.txt. \nПроверьте наличие файла в папке с программой")
    logger.exception("Ошибка загрузки auth.txt")
    time.sleep(100)
    exit(0)

try:
    auth_dict = json.loads(data)
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
except Exception as e:
    print("Неверный формат файла конфигурации auth.txt.\n")
    logger.exception("Не удалось открыть auth.txt")
    time.sleep(100)
    exit(0)

def auth():
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'grant_type=password'}
    payload.update(f'username={auth_dict["username"]}')
    payload.update(f'password={auth_dict["password"]}')
    payload.update(f'client_id={auth_dict["client_id"]}')
    payload.update(f'client_secret={auth_dict["client_secret"]}')
    response = requests.post(auth_dict["url"], data=payload, headers=headers)
    response_dict = json.loads(response.text)
    if response.status_code == 200:
        return True, response_dict
    else:
        return False, {}


def token_update(refresh_token):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'grant_type=refresh_token'}
    payload.update(f'client_id={auth_dict["client_id"]}')
    payload.update(f'client_secret={auth_dict["client_secret"]}')
    payload.update(f'refresh_token={refresh_token}')
    response = requests.post(auth_dict["url"], data=payload, headers=headers)
    response_dict = json.loads(response.text)
    if response.status_code == 200:
        return True, response_dict
    else:
        return False, {}