import re
from datetime import datetime
from io import BytesIO
from typing import Generator
from urllib.parse import urljoin
from zipfile import ZipFile

import requests
import requests_cache
from lxml import etree as et
from tqdm import tqdm

from src.handlers import StartsWithFilter, ParseDatesConverter, DumbHandler, DateRangeFilter, CounterHandler, \
    InnEnricher, DropEmptyFilter, OursEnricher, BoolFilter, PipedriveOrganisationsEnricher, PutToStore, ValuesFilter, \
    NotEqualFieldsFilter, OursFieldEnricher, OursEnricherFromCSV

requests_cache.install_cache(expire_after=60 * 60 * 24)


class RKNXMLSource():
    domain = 'https://rkn.gov.ru'
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Cookie": "PHPSESSID=j8ntojnp0qg94j56g02uv6tbu2; _ym_uid=1612756719157320509; _ym_d=1612756719; csrf-token-name=csrftoken; csrf-token-value=166245c5dc5c505d143de631cc92cf296c4d85e21edbc023945c25a6a2b792952275ecf3795c760f; _ym_isad=2; sputnik_session=1612933427252|0",
        "Host": "rkn.gov.ru",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0"
    }

    def get_licenses_from_source(self) -> Generator[dict, None, None]:
        """
        Данные очень грязные. Вот пример того, что в ИНН прилетает из xml:
        {'5001037073/500101001', '5007011040/500701001', '7725166581/772501001', '5007006650?', ';7726184054',
        '7717000085/5894', '5258044957/525801001', '773500895(4)', '6900000300/690302001', '7708130641; ; ; ;',
        '770459*3720', '0326001306; 0326001306', '7703361349/770301001', '7705130844;', '-', 'Граждане2437004056',
        'ИНН 5254018804', '7810143017-------------', 'Банка 7831001415, ОАО "Телеком ХХI" 7802103476',
        '5038001436/503801001', '6319069199; ;', '5906045271/59061001', '7701195664/770101001', '772402668 (7)',
        '6168042080; ; ; ;', '7704185167;', '5408118537; ;', '7704010978Г', '5190110664/519001001',
        '8401005730/245702001', '3308000577-', '2309007069 Красносельский банк СБ РФ г.Краснодар',
        '6662021726/665901001', '7743662198=', '7453060522/745301001', 'ИНН 7702217896', '4909070651; 4909070651',
        '0411061779/041101001', '3906080890/390601001', '+7717020194', '5009026330; ; ; ; ; ;', '773404090(9)',
        '771003595(6)', '773402222(6)', '5190406703/519001001', '7708114431;'}
        Такие данные занимают менее 1% от всех (по полю ИНН).
        :return:
        """
        abs_path = self.get_xml_link()
        print(f'xml link found {abs_path}')

        zip_file_resp = requests.get(abs_path, headers=self.headers)
        print(f'From cache: {zip_file_resp.from_cache}')
        filedata = self.unpack_zip(zip_file_resp.content)
        del zip_file_resp

        with BytesIO(filedata) as xmlfile:
            yield from self.load_xml(xmlfile, node_tag=self.node_tag)

    def get_xml_link(self) -> str:
        get_url = urljoin(self.domain, self.data_url)
        print(f'Request {get_url}')
        resp = requests.get(get_url, headers=self.headers)
        print(f'From cache: {resp.from_cache}')
        match = re.search(r'<td>Гиперссылка \(URL\) на набор</td>\r\n\t+<td\s*><a target="_blank" href="(.+?)">',
                          resp.text)
        path = match.group(1)
        abs_path = urljoin(self.domain, path)
        return abs_path

    @staticmethod
    def unpack_zip(content: bytes) -> bytes:
        print('unpack zip')
        with BytesIO(content) as zipfile:
            with ZipFile(zipfile) as zipfile_info:
                # Only one file in zip-archive
                filename = zipfile_info.namelist()[0]
                filedata = zipfile_info.read(filename)

                return filedata

    def load_xml(self, path, node_tag: str):
        print('load xml')
        for event, elem in et.iterparse(path, encoding="utf-8", recover=True):
            if elem.tag == f'{node_tag}record':
                license = {child_elem.tag.replace(node_tag, ""): child_elem.text
                           for child_elem in elem.getchildren()}
                yield license

                elem.clear()


class RKNResolutionRadioCHF(RKNXMLSource):
    data_url = '/opendata/7705846236-ResolutionRadioCHF/'
    node_tag = "{http://rsoc.ru/opendata/7705846236-ResolutionRadioCHF}"


class RKNLicenses(RKNXMLSource):
    data_url = '/opendata/7705846236-LicComm/'
    node_tag = "{http://rsoc.ru/opendata/7705846236-LicComm}"


def prolongation_resolutions_fetch(start_date: datetime, end_date: datetime):
    source = RKNResolutionRadioCHF()
    date_field = 'valid_to'

    # Dirty data: иногда встречаются даты типа 3018-03-03 или 2109-02-26
    filter_year = StartsWithFilter(date_field, '2021')
    parse_date = ParseDatesConverter(date_field)
    range_filter = DateRangeFilter(date_field, start_date, end_date)
    inn_enricher = InnEnricher()
    drop_inn_empty = DropEmptyFilter('inn')
    ours_enricher = OursEnricher()
    ours_filter = BoolFilter('our', True)
    pipedrive_org_enricher = PipedriveOrganisationsEnricher()
    put_to_store = PutToStore('prolongation_resolutions')
    counter = CounterHandler()

    (
        filter_year
            .set_next(parse_date)
            .set_next(range_filter)
            .set_next(inn_enricher)
            .set_next(drop_inn_empty)
            .set_next(ours_enricher)
            .set_next(ours_filter)
            .set_next(pipedrive_org_enricher)
            .set_next(DumbHandler())
            .set_next(put_to_store)
            .set_next(counter)
    )

    with tqdm(source.get_licenses_from_source()) as t:
        for item in t:
            filter_year.handle(item)
            t.set_postfix(counter=counter, stored=len(put_to_store.store))


def prolongation_licenses_fetch(start_date: datetime, end_date: datetime, ours: True):
    source = RKNLicenses()
    date_field = 'date_end'
    exclude_service_name = [
        'Услуги телеграфной связи',
        # 'Услуги связи для целей эфирного вещания',
        'Услуги подвижной радиосвязи в выделенной сети связи',
        'Услуги местной телефонной связи с использованием таксофонов',
        'Услуги подвижной спутниковой радиосвязи',
        'Услуги подвижной радиосвязи в сети связи общего пользования',
        'Услуги подвижной радиотелефонной связи',
        'Услуги связи персонального радиовызова',
        'Услуги междугородной и международной телефонной связи',
        # 'Услуги местной телефонной связи с использованием средств коллективного доступа'
    ]
    exclude_name = [
        'Индивидуальный предприниматель'
    ]

    filter_year = StartsWithFilter(date_field, '202')
    parse_date = ParseDatesConverter(date_field)
    exclude_service_name_filter = ValuesFilter('service_name', exclude_service_name)
    exclude_name_filter = ValuesFilter('name', exclude_name, substring_filter=True)
    exclude_not_active = ValuesFilter('lic_status_name', ['недействующая'])
    range_filter = DateRangeFilter(date_field, start_date, end_date)
    # ours_enricher = OursEnricher()
    ours_enricher = OursEnricherFromCSV()
    ours_filter = BoolFilter('our', ours)
    pipedrive_org_enricher = PipedriveOrganisationsEnricher()
    put_to_store = PutToStore(f'prolongation_licenses_{start_date}-{end_date}_{ours}')
    counter = CounterHandler()

    (
        filter_year
            .set_next(parse_date)
            .set_next(exclude_not_active)

            .set_next(range_filter)

            .set_next(exclude_service_name_filter)
            .set_next(exclude_name_filter)

            .set_next(ours_enricher)
            .set_next(ours_filter)
            .set_next(pipedrive_org_enricher)
            .set_next(put_to_store)
            .set_next(counter)
    )

    with tqdm(source.get_licenses_from_source()) as t:
        for item in t:
            filter_year.handle(item)
            t.set_postfix(
                counter=counter,
                stored=len(put_to_store.store),
                not_ours='{:.2f}%'.format(ours_filter.false_percent)
            )


def commissioning_licenses_fetch(start_date: datetime, end_date: datetime, ours: bool = True):
    source = RKNLicenses()
    date_field = 'date_service_start'
    exclude_service_name = [
        'Услуги телеграфной связи', 'Услуги связи для целей эфирного вещания',
        'Услуги подвижной радиосвязи в выделенной сети связи',
        'Услуги местной телефонной связи с использованием таксофонов',
        'Услуги подвижной спутниковой радиосвязи',
        'Услуги подвижной радиосвязи в сети связи общего пользования',
        'Услуги подвижной радиотелефонной связи',
        'Услуги связи персонального радиовызова',
        'Услуги междугородной и международной телефонной связи',
        # 'Услуги местной телефонной связи с использованием средств коллективного доступа'
    ]
    exclude_name = [
        'Индивидуальный предприниматель'
    ]

    filter_year = StartsWithFilter(date_field, '20')
    parse_date = ParseDatesConverter(date_field)
    parse_date_start = ParseDatesConverter('date_start')
    exclude_service_name_filter = ValuesFilter('service_name', exclude_service_name)
    exclude_name_filter = ValuesFilter('name', exclude_name, substring_filter=True)
    range_filter = DateRangeFilter(date_field, start_date, end_date)
    equal_dates_filter = NotEqualFieldsFilter(date_field, 'date_start')
    ours_enricher = OursEnricher()
    ours_filter = BoolFilter('our', ours)
    pipedrive_org_enricher = PipedriveOrganisationsEnricher()
    put_to_store = PutToStore(f'commissioning_licenses_{start_date}-{end_date}_{ours}')
    counter = CounterHandler()

    (
        filter_year
            .set_next(parse_date)
            .set_next(parse_date_start)
            .set_next(exclude_service_name_filter)
            .set_next(exclude_name_filter)
            .set_next(range_filter)
            .set_next(equal_dates_filter)
            .set_next(ours_enricher)
            .set_next(ours_filter)
            .set_next(pipedrive_org_enricher)
            # .set_next(DumbHandler())
            .set_next(put_to_store)
            .set_next(counter)
    )

    with tqdm(source.get_licenses_from_source()) as t:
        for item in t:
            filter_year.handle(item)
            t.set_postfix(counter=counter, stored=len(put_to_store.store))


def special_licenses_fetch():
    source = RKNLicenses()
    date_field = 'date_end'
    exclude_service_name = [
        'Услуги телеграфной связи',
        'Услуги подвижной радиосвязи в выделенной сети связи',
        'Услуги подвижной спутниковой радиосвязи',
        'Услуги подвижной радиосвязи в сети связи общего пользования',
        'Услуги подвижной радиотелефонной связи',
        'Услуги связи персонального радиовызова',
        'Услуги междугородной и международной телефонной связи',
        'Услуги почтовой связи',
        'Услуги связи по передаче данных, за исключением услуг связи по передаче данных для целей передачи голосовой информации',
        'Предоставление услуг по трансляции телевизионных и звуковых программ по сети кабельного телевидения',
        'Предоставление услуг по эфирной трансляции телевизионных программ',
    ]

    filter_year = StartsWithFilter(date_field, '20')
    parse_date = ParseDatesConverter(date_field)
    exclude_service_name_filter = ValuesFilter('service_name', exclude_service_name)
    empty_inn_filter = DropEmptyFilter('inn')
    ours_enricher = OursEnricher()
    ours_filter = BoolFilter('our', True)
    crm_tel_enricher = OursFieldEnricher('smsPhone', 'tel')
    put_to_store = PutToStore('special_licenses')
    counter = CounterHandler()

    (
        filter_year
            .set_next(parse_date)
            .set_next(exclude_service_name_filter)
            .set_next(empty_inn_filter)
            # .set_next(DumbHandler())
            .set_next(ours_enricher)
            .set_next(ours_filter)
            .set_next(crm_tel_enricher)
            .set_next(put_to_store)
            .set_next(counter)
    )

    with tqdm(source.get_licenses_from_source()) as t:
        for item in t:
            filter_year.handle(item)
            t.set_postfix(
                counter=counter,
                stored=len(put_to_store.store),
                miss_cache_crm_tel='{:.2f}%'.format(crm_tel_enricher.miss_cache_percent)
            )
