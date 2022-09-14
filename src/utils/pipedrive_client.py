from requests_futures.sessions import FuturesSession
from tqdm import tqdm

PIPDERIVE_URL="https://api.pipedrive.com"
API_KEY = None

TEST_PIPELINE_ID = 32
TEST_STAGE_ID = 187

COMISSIONING_PIPELINE_ID = 33
COMISSIONING_STAGE_ID = 197

RESOLUTIONS_PIPELINE_ID = 34
RESOLUTIONS_STAGE_ID = 198

PROLONGATION_PIPELINE_ID = 36
PROLONGATION_STAGE_ID = 208

TASK_USER_ID = 0
TEST_USER_ID = 0

session = FuturesSession()


def pipedrive_client(suffix, parameters=None, data=None, delete: bool = False):
    parameters = parameters or []
    url = "{}/v1/{}".format(PIPDERIVE_URL, suffix)
    params = [('api_token', API_KEY)]
    params = params + parameters
    if data:
        return session.post(url, params=params, data=data)
    if delete:
        return session.delete(url, params=params)
    return session.get(url, params=params)


def push_deal(deal: dict) -> dict:
    res = pipedrive_client("deals", data=deal)
    res = res.result()
    res.raise_for_status()
    res_data = res.json()

    if not res_data['success']:
        raise ValueError

    return res_data


def push_task(task: dict) -> dict:
    res = pipedrive_client("activities", data=task)
    res = res.result()
    res.raise_for_status()
    res_data = res.json()

    if not res_data['success']:
        raise ValueError

    return res_data


def get_deals(stage_id):
    parameters = [('stage_id', stage_id), ('limit', 1000)]
    return pipedrive_client("deals", parameters)


def get_pipedrive_orgs_for_inn(inn):
    parameters = [
        ('term', inn),
        ('field_type', 'organizationField'),
        ('field_key', '7a39d86c8364a65f52792bbc9fd40c8a9ddae525'),
        ('exact_match', 'true'),
        ('return_item_ids', '1'), ('start', '0')
    ]
    # print("Resolving inn: {}".format(inn))
    future = pipedrive_client("itemSearch/field", parameters)
    res = future.result()
    # print("Status: {}".format(res.status_code))
    res.raise_for_status()
    if res.status_code == 200:
        data = res.json()['data']
        # print("Data: {}".format(data))
        if data:
            for org in data:
                return org['id']
    # print("Couldn't find organization for inn {}, status_code: {}".format(inn, res.status_code))


def get_pipedrive_org(id):
    """
        {'id': 4008, 'key': 'ba2d5c3d14926f9581c70f23bf4245c925752026', 'name': 'тел. контактн.',
        'order_nr': 27, 'field_type': 'phone', 'json_column_flag': False,
        'add_time': '2015-02-26 16:32:36', 'update_time': '2015-02-26 16:32:36',
        'last_updated_by_user_id': None, 'active_flag': True, 'edit_flag': True, 'index_visible_flag': True,
        'details_visible_flag': True, 'add_visible_flag': True, 'important_flag': False,
        'bulk_edit_allowed': True, 'searchable_flag': True, 'filtering_allowed': True, 'sortable_flag': True,
        'mandatory_flag': False}
        :param inn:
        :return:
    """
    # print("Resolving inn: {}".format(inn))
    future = pipedrive_client(f"organizations/{id}")
    res = future.result()
    # print("Status: {}".format(res.status_code))
    res.raise_for_status()
    if res.status_code == 200:
        data = res.json()['data']
        # print("Data: {}".format(data))
        if data:
            return data


def get_deals_for_stage_id_and_delete(stage_id):
    parameters = [('stage_id', stage_id), ('limit', 200)]
    print("Resolving deals for stage_id: {}".format(stage_id))
    res = pipedrive_client("deals", parameters)
    res = res.result()
    print("Status: {}".format(res.status_code))
    res.raise_for_status()
    if res.status_code == 200:
        data = res.json()['data']
        print("Found {} deals".format(len(data)))
        for deal in tqdm(data):
            print("Deleting deal {}".format(deal['id']))
            res = pipedrive_client("deals/{}".format(deal['id']), delete=True)
            res = res.result()
            print(res.status_code)
        res = pipedrive_client("deals", parameters)
        res = res.result()
        if res.status_code == 200:
            if res.json()['data']:
             get_deals_for_stage_id_and_delete(stage_id)
