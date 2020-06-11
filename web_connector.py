import socket
import logging
import requests
import threading
import ctypes
import time

logging.basicConfig(filename='logs.txt', format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(name=__name__)
logger.setLevel(logging.INFO)


class ConnectionChecker(threading.Thread): # поток проверки TCP-соединения
    def __init__(self, name, id_main_thread, HOST, PORT, TCP_timeout):
        threading.Thread.__init__(self)
        self.name = name
        self.id_main_thread = id_main_thread
        self.HOST = HOST
        self.PORT = PORT


    def warmup_run(self):
        return self.check_connection()

    def run(self):        # основная функция потока
        try:
            while True:
                check = self.check_connection()
                if check:
                    time.sleep(10)
                else:
                    self.raise_exception()

        finally:
            print("TCP поток: завершен")

    def check_connection(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.HOST, self.PORT))
            s.send(b"test connection")
            check = s.recv(1024)
            s.close()
            if check == b"ack":
                return True
            else:
                return False
        except socket.error as e:
            logger.exception("Ошибка")
            s.close()
            return False

    def get_id(self):        # получить ID текущего потока
        if hasattr(self, '_thread_id'):
            return self._thread_id
        for id, thread in threading._active.items():
            if thread is self:
                return id

    def raise_exception(self): # stop main thread
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(self.id_main_thread,
                                                         ctypes.py_object(SystemExit))
        if res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(self.id_main_thread, 0)
            print('TCP-поток: не удалось вызвать исключение')
            logger.error('TCP-поток: не удалось вызвать исключение')


class Replicator(threading.Thread): # поток отправки запроса
    def __init__(self, name, payload, server_url):
        threading.Thread.__init__(self)
        self.name = name
        self.allow_start = False
        self.payload = payload
        self.request_result = False
        self.server_url = server_url

    def run(self):          # отправка запроса
        tcp_tries = 5 # количество попыток обращения к TCP-серверу
        for i in range(tcp_tries):
            event = threading.Event()
            if not self.allow_start:
                event.wait(3)
            else:
                break
        if self.allow_start:
            try:
                print("Отправка запроса")
                r = requests.post(self.server_url, data=(self.payload))
                if r.status_code == 200:
                    self.request_result = True
                else: self.request_result = False
            except:
                self.request_result = False
        else:
            print("Не удалось подключиться к TCP-серверу")
            logger.warning("Не установлено соединение TCP")
            self.request_result = False


    def stop_TCP(self, thread_id):
        self.raise_exception(thread_id)

    def get_id(self):        # returns id of the respective thread
        if hasattr(self, '_thread_id'):
            return self._thread_id
        for id, thread in threading._active.items():
            if thread is self:
                return id

    def raise_exception(self, thread_id): # stop main thread
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id,
                                                         ctypes.py_object(SystemExit))
        if res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
            print('Поток репликатора: не удалось вызвать исключение')
            logger.error('Поток репликатора: не удалось вызвать исключение')


def send_request(payload, HOST, PORT, server_url, TCP_timeout):
    main_thread = Replicator('Replication thread', payload, server_url)
    main_thread.start()
    id_main_thread = main_thread.get_id()

    connection_thread = ConnectionChecker('Connection checker thread', id_main_thread, HOST, PORT, TCP_timeout)
    main_thread.allow_start = connection_thread.warmup_run()
    connection_thread.start()
    id_connection_thread = connection_thread.get_id()

    # killing threads
    main_thread.stop_TCP(id_connection_thread)
    main_thread.join()
    connection_thread.join()
    return main_thread.request_result
