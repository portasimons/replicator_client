import sqlite3
from sqlite3 import Error
import datetime
import logging
import message_preparator


logging.basicConfig(filename='logs.txt', format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(name=__name__)
logger.setLevel(logging.INFO)


def create_connection(path): # создание соединения с БД
    connection = None
    try:
        connection = sqlite3.connect(path)
    except Error as e:
        print(f"Не удалось подключиться к базе данных. Ошибка: '{e}'")
        logger.exception("Не установлено соединение с БД")
    return connection


def prepare_queries():
    query_rowid = """ 
            SELECT max(rowid) FROM data;
            """
    # получение новых значений из БД
    query_main = """SELECT {} FROM data, nodeids WHERE data.nodeid = nodeids.id AND data.rowid BETWEEN {} AND {}{}"""
    return query_rowid, query_main

def prepare_agent_objects(objects_list):
    # прописать случай для списка с одним элементом
    conditions_list = ["identifier LIKE \"%" + s + "\"" for s in objects_list]
    if objects_list != []:
        return f""" AND ({' OR '.join(conditions_list)})"""
    else:
        return ""

def execute_query(connection, query): # исполнение запроса к БД
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")
        logger.exception("Не выполнен запрос к БД")


def monitoring(path, query_rowid, query_main, last_rowid, query_columns_list, agent_objects_list, platform_id, max_values_per_request):
    connection = create_connection(path)
    field, bush, well, brigade = platform_id

    if connection == None:  # случай ошибки соединения
        return last_rowid, ""

    new_last_rowid, = execute_query(connection, query_rowid)[0] # получить rowID последней добавленной записи
    if new_last_rowid > last_rowid: # условие: если последний добавленный rowid больше исходного
        print(f"Номер последнего rowID: {new_last_rowid}. Время получения: {datetime.datetime.now()}")
        agent_objects_string = prepare_agent_objects(agent_objects_list)
        query_columns_string = ','.join(query_columns_list)
        new_values = execute_query(connection, query_main.format(query_columns_string,
                                                                 last_rowid,
                                                                 new_last_rowid,
                                                                 agent_objects_string))
        json_data = message_preparator.prepare_data(max_values_per_request,
                                                    new_values,
                                                    query_columns_list,
                                                    field, bush, well, brigade)
        last_rowid = new_last_rowid
        return last_rowid, json_data
    else:
        return last_rowid, [""]

