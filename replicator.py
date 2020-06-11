import time
import web_connector
import db_monitor
import json
import logging


logger = logging.getLogger(name=__name__)
logging.basicConfig(filename='logs.txt', format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")
logger.setLevel(logging.INFO)
logger.info("Запуск программы")


# загрузка файла конфигурации
config = {}
try:
    file = open('settings/config.txt', 'r', encoding="utf-8")
    data = file.read()
    data = data.replace('\n', '')
except Exception as e:
    print("Не удалось загрузить config.txt. \nПроверьте наличие файла в папке с программой")
    logger.exception("Ошибка загрузки config.txt")
    time.sleep(10)
    exit(0)

# загрузка конфигурации в словарь config
try:
    config = json.loads(data)
except Exception as e:
    print("Неверный формат файла конфигурации config.txt.\n")
    logger.exception("Формат config.txt неверный")
    time.sleep(100)
    exit(0)


DB_timeout = config["DB_timeout"] # интервал проверки записей в БД (в секундах)
agent_objects = config["agent_objects"]


if __name__ == "__main__":
    new_values = []
    platform_id = (config["field"], config["bush"], config["well"], config["brigade"])
    query_rowid, query_main = db_monitor.prepare_queries()    # шаблоны запросов
    while True:
        new_last_rowid, json_data = db_monitor.monitoring(config["db_path"],
                                                          query_rowid,
                                                          query_main,
                                                          config["last_rowid"],
                                                          config["query_columns"],
                                                          config["agent_objects"],
                                                          platform_id,
                                                          config["max_values_per_request"])
        if json_data[0] != "":  # json_data - список строк
            for i in json_data:
                while True:
                    if web_connector.send_request(i, config["HOST"],
                                                  config["PORT"],
                                                  config["server_url"],
                                                  config["TCP_timeout"]):
                        print("Веб-запрос отправлен")
                        break
                    else:
                        print("Не удалось отправить запрос. Повторная попытка")
                        logger.warning("Запрос не отправлен")
                        continue
                config.update({"last_rowid":new_last_rowid})
        time.sleep(config["DB_timeout"])
        continue