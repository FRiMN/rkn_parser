from __future__ import annotations

import os
import re
import shelve
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from pathlib import Path
from pprint import pprint
from time import time
from typing import Optional, Any, Union

import pymysql
from tqdm import tqdm

from src.utils.pipedrive_client import get_pipedrive_orgs_for_inn, get_pipedrive_org


def _log(message):
    if message['total_time'] > 0.1:
        print('[SimpleTimeTracker] {function_name} {total_time:.3f}'.format(**message))


def simple_time_tracker(log_fun):
    def _simple_time_tracker(fn):
        @wraps(fn)
        def wrapped_fn(*args, **kwargs):
            start_time = time()

            try:
                result = fn(*args, **kwargs)
            finally:
                elapsed_time = time() - start_time

                # log the result
                log_fun({
                    'function_name': repr(fn),
                    'total_time': elapsed_time,
                })

            return result

        return wrapped_fn

    return _simple_time_tracker


class Handler(ABC):
    """
    Интерфейс Обработчика объявляет метод построения цепочки обработчиков. Он
    также объявляет метод для выполнения запроса.
    """

    @abstractmethod
    def set_next(self, handler: Handler) -> Handler:
        pass

    @abstractmethod
    def handle(self, item: dict) -> Optional[str]:
        pass


class AbstractHandler(Handler):
    """
    Поведение цепочки по умолчанию может быть реализовано внутри базового класса
    обработчика.
    """

    _next_handler: Handler = None

    def set_next(self, handler: Handler) -> Handler:
        self._next_handler = handler
        # Возврат обработчика отсюда позволит связать обработчики простым
        # способом, вот так:
        # monkey.set_next(squirrel).set_next(dog)
        return handler

    @abstractmethod
    def handle(self, item: dict) -> Optional[str]:
        if self._next_handler:
            return self._next_handler.handle(item)

        return None


class StartsWithFilter(AbstractHandler):
    def __init__(self, filter_field: str, filter_string: str):
        self.filter_string = filter_string
        self.filter_field = filter_field

    def handle(self, item: dict) -> Optional[str]:
        if self.filter_field not in item.keys():
            print('not valid: empty')
            return

        if (not item[self.filter_field]
                or not item[self.filter_field].startswith(self.filter_string)):
            # print(f'not valid starts {item[self.filter_field]}')
            return

        return super().handle(item)


class ParseDatesConverter(AbstractHandler):
    def __init__(self, date_field: str):
        self.date_field = date_field

    def handle(self, item: dict) -> Optional[str]:
        item[self.date_field] = datetime.strptime(item[self.date_field], '%Y-%m-%d')
        return super().handle(item)


class DateRangeFilter(AbstractHandler):
    def __init__(self, date_field: str, start: datetime, end: datetime):
        self.start = start
        self.end = end
        self.date_field = date_field

    # @simple_time_tracker(_log)
    def handle(self, item: dict) -> Optional[str]:
        # TODO: may be raise exception?
        date = item[self.date_field]
        if not isinstance(date, datetime):
            print('===> not datetime')
            return

        in_range = (self.start <= date <= self.end)
        if not in_range:
            # print(f'==> not in range {self.start} <= {date} <= {self.end}')
            return

        return super().handle(item)


class InnEnricher(AbstractHandler):
    """ Обогащает данные полем 'inn' по имени организации из РКН """

    def __init__(self):
        self.inn_dictionary = self.generate_dictionary()
        print(f"Prepared dictionary with {len(self.inn_dictionary)} inn's")

    def generate_dictionary(self) -> dict:
        from src.conveers import RKNLicenses

        source = RKNLicenses()
        return {
            l['name']: l['inn']
            for l in tqdm(source.get_licenses_from_source())
            if 'inn' in l.keys() and l['inn'].isdigit()
        }

    def handle(self, item: dict) -> Optional[str]:
        item['inn'] = self.inn_dictionary.get(item['owner_name'])
        return super().handle(item)


class MissCacheMixin:
    miss_cache_counter = 0
    cache_counter = 0

    @property
    def miss_cache_percent(self) -> float:
        if not self.cache_counter:
            return 0
        return 100 * self.miss_cache_counter / self.cache_counter


class OursEnricher(AbstractHandler, MissCacheMixin):
    """ Обогащает данные булевым полем обозначающем наличие в базе CRM """

    def __init__(self):
        self.cache = {}
        self.con = pymysql.connect(
            host='crm-db', port=3306,
            user='root', password='root',
            database='crm'
            # cursorclass=pymysql.cursors.DictCursor
        )

    # @simple_time_tracker(_log)
    def handle(self, item: dict) -> Optional[str]:
        inn = item["inn"]
        self.cache_counter += 1
        is_exist = self.cache.get(inn)

        if is_exist is None:
            self.miss_cache_counter += 1
            with self.con.cursor() as cur:
                cur.execute("SELECT id FROM Organisation WHERE inn = %s", inn)

                is_exist = cur.fetchone() is not None
                self.cache[inn] = is_exist

        item['our'] = is_exist
        return super().handle(item)

    def __del__(self):
        print('Close mysql connection')
        self.con.close()


class OursEnricherFromCSV(AbstractHandler):
    file_path = 'crmdbsync/all_clients.csv'
    inns = set()
    inn_index = 7

    def __init__(self):
        print(f'Open file {self.file_path}')
        with open(self.file_path, 'r', encoding='cp1251') as f:
            orgs = f.readlines()

        # Pop headers
        orgs.pop(0)
        print(f'Find {len(orgs)} organisations')

        # Remove first and last quote symbols in string
        orgs = [org[1:-1] for org in orgs]
        # Split by `;` symbol and remove inner quote symbols
        orgs = [org.split('";"') for org in orgs]

        inns = [org[self.inn_index] for org in orgs]
        inns = [re.sub('[^0-9]', '', inn) for inn in inns]

        self.inns = set(inns)
        print(f'Find {len(self.inns)} uniq organisations')

    def handle(self, item: dict) -> Optional[str]:
        inn = item["inn"]

        is_exist = inn in self.inns

        item['our'] = is_exist
        return super().handle(item)


class OursFieldEnricher(AbstractHandler, MissCacheMixin):
    def __init__(self, enrich_field: str, put_field: str):
        self.search_field = enrich_field
        self.put_field = put_field
        self.cache = {}
        self.con = pymysql.connect(
            host='crm-db', port=3306,
            user='root', password='root',
            database='crm'
            # cursorclass=pymysql.cursors.DictCursor
        )

    def handle(self, item: dict) -> Optional[str]:
        inn = item["inn"]
        self.cache_counter += 1
        exist = self.cache.get(inn)

        if exist is None:
            self.miss_cache_counter += 1
            with self.con.cursor() as cur:
                cur.execute(f"SELECT {self.search_field} FROM Organisation WHERE inn = %s", inn)

                exist = cur.fetchone()[0]
                # print(cur.description)
                # print('Exist db:', exist)
                self.cache[inn] = exist

        item[self.put_field] = exist
        return super().handle(item)

    def __del__(self):
        print('Close mysql connection')
        self.con.close()


class PipedriveOrganisationsEnricher(AbstractHandler):
    cache = {}

    # @simple_time_tracker(_log)
    def handle(self, item: dict) -> Optional[str]:
        inn = item['inn']
        in_cache = inn in self.cache.keys()
        if not in_cache:
            org_id = get_pipedrive_orgs_for_inn(inn)
            self.cache[inn] = org_id
        else:
            org_id = self.cache[inn]

        item['pipedrive_org_id'] = org_id
        return super().handle(item)


class PipedriveOrganisationsFieldEnricher(AbstractHandler):
    cache = {}

    def __init__(self, enrich_field: str, put_field: str):
        self.search_field = enrich_field
        self.put_field = put_field

    def handle(self, item: dict) -> Optional[str]:
        org_id = item['pipedrive_org_id']
        if org_id:
            in_cache = org_id in self.cache
            if not in_cache:
                org_data = get_pipedrive_org(org_id)
                self.cache[org_id] = org_data
            else:
                org_data = self.cache[org_id]

            item[self.put_field] = org_data[self.search_field]
        return super().handle(item)


class DropEmptyFilter(AbstractHandler):
    def __init__(self, field: str):
        self.field = field

    def handle(self, item: dict) -> Optional[str]:
        if item.get(self.field) is None or item[self.field] == '':
            return
        return super().handle(item)


class BoolFilter(AbstractHandler):
    def __init__(self, field: str, valid_value: bool):
        self.field = field
        self.value = valid_value
        self.total_counter = 0
        self.false_counter = 0

    @property
    def false_percent(self) -> float:
        if not self.total_counter:
            return 0
        return 100 * self.false_counter / self.total_counter

    def handle(self, item: dict) -> Optional[str]:
        self.total_counter += 1
        if item[self.field] is not self.value:
            self.false_counter += 1
            return
        return super().handle(item)


class ValuesFilter(AbstractHandler):
    def __init__(self, field: str, exclude_values: list, substring_filter=False):
        self.field = field
        self.exclude_values = exclude_values
        self.substring_filter = substring_filter

    # @simple_time_tracker(_log)
    def handle(self, item: dict) -> Optional[str]:
        if self.substring_filter:
            for excl_val in self.exclude_values:
                if excl_val in item[self.field]:
                    return
        else:
            if item[self.field] in self.exclude_values:
                return
        return super().handle(item)


class NotEqualFieldsFilter(AbstractHandler):
    def __init__(self, *fields):
        self.fields = fields

    def handle(self, item: dict) -> Optional[str]:
        uniq_vals = {item[x] for x in self.fields}
        if len(uniq_vals) < len(self.fields):
            return
        return super().handle(item)


class PutToStore(AbstractHandler):
    storage_dir = 'cached_data/'
    date = datetime.now().strftime('%Y-%m-%d')

    def __init__(self, filename: str):
        self.filename = filename
        path = self.create_store_path()
        self.store: shelve.Shelf = shelve.open(path, flag='c', writeback=True)

    def create_store_path(self) -> str:
        Path(self.storage_dir).mkdir(parents=True, exist_ok=True)
        return os.path.join(self.storage_dir, f'{self.filename}_{self.date}')

    @staticmethod
    def merge_values(stored_item_val: Any, item_val: Any) -> Union[set, Any]:
        """ В этом методе происходит сжатие значений из разных записей с одним ИНН в одну запись """
        if stored_item_val != item_val and item_val is not None:
            if not isinstance(stored_item_val, set):
                stored_item_val = {stored_item_val}
            stored_item_val.add(item_val)
        return stored_item_val

    def merge_records(self, item: dict, key: str):
        """ В этом методе происходит сжатие записей с одним ИНН в одну запись """
        stored_item = self.store[key]
        for k in stored_item.keys():
            if k in item.keys():
                stored_item[k] = self.merge_values(stored_item[k], item[k])
        self.store[key] = stored_item

    def handle(self, item: dict) -> Optional[str]:
        key = item['inn']

        is_exist = key in self.store.keys()
        if not is_exist:
            self.store[key] = item
            return super().handle(item)

        self.merge_records(item, key)
        return super().handle(item)

    def __del__(self):
        # self.store.sync()
        self.store.close()


class DumbHandler(AbstractHandler):
    def handle(self, item: dict) -> Optional[str]:
        pprint(item)
        return super().handle(item)


@dataclass
class Counters:
    counter: int = 0
    # empty_inn_counter: int = 0
    false_our_counter: int = 0
    # not_found_organisations: int = 0


class CounterHandler(AbstractHandler):
    counters = Counters()

    def handle(self, item: dict) -> Optional[str]:
        self.counters.counter += 1
        # if not item.get('inn'):
        #     self.counters.empty_inn_counter += 1
        if not item['our']:
            self.counters.false_our_counter += 1
        # if not item.get('pipedrive_org_id'):
        #     self.counters.not_found_organisations += 1
        return super().handle(item)

    def __str__(self):
        return str(self.counters)
