import os
import re
from pathlib import Path
from pprint import pprint

from tqdm import tqdm

from src.handlers import PutToStore
from src.utils.pipedrive_client import push_deal, COMISSIONING_STAGE_ID, RESOLUTIONS_STAGE_ID, PROLONGATION_STAGE_ID
from src.utils.utils import get_earlier_date, set_processor, set_stringer, head

HUMANIZED_FIELDS = {
    'name': 'Наименование лицензиата',
    'ownership': 'Организационно-правовая форма',
    'name_short': 'Краткое наименование',
    'addr_legal': 'Адрес места нахождения',
    'inn': 'ИНН лицензиата',
    'ogrn': 'ОГРН/ОГРНИП',
    'licence_num': 'Регистрационный номер лицензии',
    'lic_status_name': 'Статус лицензии',
    'date_start': 'День начала оказания услуг (не позднее)',
    'date_end': 'Срок действия, до',
    'date_order': 'Дата внесения в реестр сведений о выдаче (продлении срока действия, переоформлении, прекращении действия лицензии, приостановлении действия лицензии, возобновления действия лицензии)',
    'service_name': 'Лицензируемый вид деятельности с указанием выполняемых работ, составляющих лицензируемый вид деятельности',
    'territory': 'Территория действия лицензии',
    'num_order': 'Номер лицензионного приказа',
    'our': 'Наши',
    'pipedrive_org_id': 'ID организации в Pipedrive'
}


def humanized_header(header: str) -> str:
    return HUMANIZED_FIELDS.get(header, header)


def csv_generator(store_name: str):
    Path('reports/').mkdir(parents=True, exist_ok=True)

    store_ = PutToStore(store_name)
    store = store_.store

    file = os.path.join('reports/', f'{store_.filename}.csv')
    with open(file, mode='w') as f:
        first_rec_key = [x for x in store.keys()][0]
        headers = store[first_rec_key].keys()
        humanized_headers = [humanized_header(header) for header in headers]
        headers_line = '\t'.join(humanized_headers)
        f.write(headers_line + '\n')

        data = sorted(
            store.values(),
            key=lambda _: set_processor(get_earlier_date, _['date_end'])
        )
        for rec in tqdm(data):
            # Sorting values by headers
            rec_list = [rec.get(k) for k in headers]
            for index, val in enumerate(rec_list):
                if isinstance(val, set):
                    val = {str(x) for x in val}

                if isinstance(val, (list, set, tuple)):
                    val = list(val)
                    val.sort()

                    val = '; '.join(val)

                if val is None or val == 'NULL':
                    val = ''

                if isinstance(val, bool):
                    val = 'да' if val else 'нет'

                val = str(val)
                val = re.sub('\n', ' ', val, re.MULTILINE)

                rec_list[index] = val
            rec_line = '\t'.join(rec_list)
            f.write(rec_line + '\n')


def prolongation_resolutions_csv(start, end):
    csv_generator('prolongation_resolutions')


def prolongation_licenses_csv(start, end, ours):
    csv_generator(f'prolongation_licenses_{start}-{end}_{ours}')


def commissioning_licenses_csv(start, end, ours: bool = True):
    csv_generator(f'commissioning_licenses_{start}-{end}_{ours}')


def special_licenses_csv():
    csv_generator('special_licenses')


def prolongation_resolutions_push(start, end):
    """ TODO: Длина полей ограничена, не все номера влазят из `reason_num` """
    store_ = PutToStore('prolongation_resolutions')
    store = store_.store

    for rec in tqdm(store.values()):
        priority_field = rec['reason_num']
        priority = len(priority_field) if isinstance(priority_field, set) else 1
        data = {
            "title": 'РИЧ ' + rec['owner_name'],
            "org_id": '' if rec['pipedrive_org_id'] is None else rec['pipedrive_org_id'],
            "stage_id": RESOLUTIONS_STAGE_ID,
            "expected_close_date": set_processor(get_earlier_date, rec['valid_to']),
            "aa70bec98d1f7191a451b82b0d3ca4a41197d958": rec['inn'],
            "0839c880bbc29931ae1bc343832bff5c45286114": str(rec['radio_service']),
            "b9b8918e32d97ef42975dc1655bd13500b83f0e4": str(rec['territory']),
            "9f717de3f3f6516b604dac85f84a2d3b3143dd5e": set_processor(set_stringer, rec['reason_num']),
            "754eb6f2cc6c441f9def7920ff3525f4254b71b9": priority
        }
        # pprint(data)
        push_deal(data)


def commissioning_licenses_push(start, end, ours: bool = True):
    store_ = PutToStore(f'commissioning_licenses_{start}-{end}_{ours}')
    store = store_.store

    for rec in tqdm(store.values()):
        priority_field = rec['licence_numbers']
        priority = len(priority_field) if isinstance(priority_field, set) else 1
        data = {
            "title": 'Ввод ' + rec['name'],
            "org_id": '' if rec['pipedrive_org_id'] is None else rec['pipedrive_org_id'],
            "stage_id": COMISSIONING_STAGE_ID,
            "expected_close_date": set_processor(get_earlier_date, rec['date_service_start']),
            "aa70bec98d1f7191a451b82b0d3ca4a41197d958": rec['inn'],
            "0839c880bbc29931ae1bc343832bff5c45286114": str(rec['service_name']),
            "b9b8918e32d97ef42975dc1655bd13500b83f0e4": str(rec['territory']),
            "9f717de3f3f6516b604dac85f84a2d3b3143dd5e": set_processor(set_stringer, rec['licence_numbers']),
            "754eb6f2cc6c441f9def7920ff3525f4254b71b9": priority
        }
        # pprint(data)
        push_deal(data)


def prolongation_licenses_push(start, end, ours: bool = True):
    store_ = PutToStore(f'prolongation_licenses_{start}-{end}_{ours}')
    store = store_.store

    for rec in tqdm(store.values()):
        priority_field = rec['licence_num']
        priority = len(priority_field) if isinstance(priority_field, set) else 1
        data = {
            "title": 'Продление ' + set_processor(head, rec['name']),
            "org_id": '' if rec['pipedrive_org_id'] is None else rec['pipedrive_org_id'],
            "stage_id": PROLONGATION_STAGE_ID,
            "expected_close_date": str(set_processor(get_earlier_date, rec['date_end'])),
            "aa70bec98d1f7191a451b82b0d3ca4a41197d958": rec['inn'],
            "0839c880bbc29931ae1bc343832bff5c45286114": set_processor(set_stringer, rec['service_name']),
            "b9b8918e32d97ef42975dc1655bd13500b83f0e4": set_processor(set_stringer, rec['territory']),
            "9f717de3f3f6516b604dac85f84a2d3b3143dd5e": set_processor(set_stringer, rec['licence_num']),
            "754eb6f2cc6c441f9def7920ff3525f4254b71b9": priority
        }
        # pprint(data)
        push_deal(data)
