import json
import os
import time
import urllib

import requests
from kbcstorage.base import Endpoint
from kbcstorage.buckets import Buckets
from kbcstorage.tables import Tables

URL_SUFFIXES = {"US": ".keboola.com",
                "EU": ".eu-central-1.keboola.com",
                "AZURE-EU": ".north-europe.azure.keboola.com",
                "CURRENT_STACK": os.environ.get('KBC_STACKID', 'connection.keboola.com').replace('connection', '')}

"""
Various Adhoc scripts for KBC api manipulations.

"""


def run_config(component_id, config_id, token, region='US'):
    values = {
        "config": config_id
    }

    headers = {
        'Content-Type': 'application/json',
        'X-StorageApi-Token': token
    }
    response = requests.post('https://syrup' + URL_SUFFIXES[region] + '/docker/' + component_id + '/run',
                             data=json.dumps(values),
                             headers=headers)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()


def get_job_status(token, url):
    headers = {
        'Content-Type': 'application/json',
        'X-StorageApi-Token': token
    }
    response = requests.get(url, headers=headers)
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()


def block_storage_job_until_completed(token, url):
    """
    Poll the API until the job is completed.
    Args:
        # job_id (str): The id of the job
    Returns:
        response_body: The parsed json from the HTTP response
            containing a storage Job.
    Raises:
        requests.HTTPError: If any API request fails.
    """
    retries = 1
    while True:
        job = get_job_status(token, url)
        if job['status'] in ('error', 'success'):
            return job
        retries += 1
        time.sleep(min(2 ** retries, 20))


def list_component_configurations(token, component_id, region='US'):
    cl = Endpoint('https://connection' + URL_SUFFIXES[region], 'components', token)
    url = '{}/{}/configs'.format(cl.base_url, component_id)
    return cl._get(url)


def list_project_components(token, region='US', component_type=None, include='configuration,rows,state'):
    cl = Endpoint('https://connection' + URL_SUFFIXES[region], 'components', token)
    url = cl.base_url
    params = {'componentType': component_type,
              'include': include}
    return cl._get(url, params)


def get_config_detail(token, region, component_id, config_id):
    """

    :param region: 'US' or 'EU'
    """
    cl = Endpoint('https://connection' + URL_SUFFIXES[region], 'components', token)
    url = '{}/{}/configs/{}'.format(cl.base_url, component_id, config_id)
    return cl._get(url)


def get_config_row_detail(token, region, component_id, config_id, row_id):
    """

    :param region: 'US' or 'EU'
    """
    cl = Endpoint('https://connection' + URL_SUFFIXES[region], 'components', token)
    url = f'{cl.base_url}/{component_id}/configs/{config_id}/rows/{row_id}'
    return cl._get(url)


def get_config_version(token, region, component_id, config_id, limit=10):
    """

    :param limit:
    :param token:
    :param config_id:
    :param component_id:
    :param region: 'US' or 'EU'
    """

    cl = Endpoint('https://connection' + URL_SUFFIXES[region], 'components', token)
    params = {"limit": limit}
    url = f'{cl.base_url}/{component_id}/configs/{config_id}/versions'
    return cl._get(url, params=params)


def get_config_rows(token, region, component_id, config_id):
    """
    Retrieves component's configuration detail.

    Args:
        component_id (str or int): The id of the component.
        config_id (int): The id of configuration
        region: 'US' or 'EU'
    Raises:
        requests.HTTPError: If the API request fails.
    """
    cl = Endpoint('https://connection' + URL_SUFFIXES[region], 'components', token)
    url = '{}/{}/configs/{}/rows'.format(cl.base_url, component_id, config_id)

    return cl._get(url)


def delete_config(token, region, component_id, configuration_id, branch_id=None, **kwargs):
    """
    Create a new table from CSV file.

    Args:
        component_id (str):
        configuration_id
        region: 'US' or 'EU'

    Returns:
        table_id (str): Id of the created table.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    if not branch_id:
        enpoint_prefix = 'components'
    else:
        enpoint_prefix = f'branch/{branch_id}/components'

    cl = Endpoint('https://connection' + URL_SUFFIXES[region], enpoint_prefix, token)
    url = f'{cl.base_url}/{component_id}/configs/{configuration_id}'
    return cl._delete(url)


def create_config(token, region, component_id, name, description, configuration, configurationId=None, state=None,
                  changeDescription='', branch_id=None, **kwargs):
    """
    Create a new table from CSV file.

    Args:
        component_id (str):
        name (str): The new table name (only alphanumeric and underscores)
        configuration (dict): configuration JSON; the maximum allowed size is 4MB
        state (dict): configuration JSON; the maximum allowed size is 4MB
        changeDescription (str): Escape character used in the CSV file.
        region: 'US' or 'EU'

    Returns:
        table_id (str): Id of the created table.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    if not branch_id:
        enpoint_prefix = 'components'
    else:
        enpoint_prefix = f'branch/{branch_id}/components'

    cl = Endpoint('https://connection' + URL_SUFFIXES[region], enpoint_prefix, token)
    url = '{}/{}/configs'.format(cl.base_url, component_id)
    parameters = {}
    if configurationId:
        parameters['configurationId'] = configurationId
    parameters['configuration'] = json.dumps(configuration)
    parameters['name'] = name
    parameters['description'] = description
    parameters['changeDescription'] = changeDescription
    if state:
        parameters['state'] = json.dumps(state)
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = urllib.parse.urlencode(parameters)
    return cl._post(url, data=data, headers=header)


def update_config(token, region, component_id, configurationId, name, description='', configuration=None, state=None,
                  changeDescription='', branch_id=None, **kwargs):
    """
    Update table from CSV file.

    Args:
        component_id (str):
        name (str): The new table name (only alphanumeric and underscores)
        configuration (dict): configuration JSON; the maximum allowed size is 4MB
        state (dict): configuration JSON; the maximum allowed size is 4MB
        changeDescription (str): Escape character used in the CSV file.
        region: 'US' or 'EU'

    Returns:
        table_id (str): Id of the created table.

    Raises:
        requests.HTTPError: If the API request fails.
    """

    if not branch_id:
        url = f'https://connection{URL_SUFFIXES[region]}/v2/storage/components/{component_id}/configs/{configurationId}'
    else:
        url = f'https://connection{URL_SUFFIXES[region]}/v2/storage/branch/{branch_id}/components/{component_id}/configs/{configurationId}'
    parameters = {}
    parameters['configurationId'] = configurationId
    if configuration:
        parameters['configuration'] = json.dumps(configuration)
    parameters['name'] = name
    parameters['description'] = description
    parameters['changeDescription'] = changeDescription
    if state is not None:
        parameters['state'] = json.dumps(state)
    headers = {'Content-Type': 'application/x-www-form-urlencoded'
        , 'X-StorageApi-Token': token}
    response = requests.put(url,
                            data=parameters,
                            headers=headers)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()


def clone_configuration(token, region, component_id, configuration_id, name, description=''):
    """
    Update table from CSV file.

    Args:
        component_id (str):
        name (str): The new config name (only alphanumeric and underscores)
        region: 'US' or 'EU'

    Returns:
        configuration_id (str): Id of the created config.

    Raises:
        requests.HTTPError: If the API request fails.
    """

    # get latest version
    versions = get_config_version(token, region, component_id, configuration_id, 1)
    latest_version = versions[0]['version']
    url = f'https://connection{URL_SUFFIXES[region]}/v2/storage/components/{component_id}/configs/{configuration_id}' \
          f'/versions/{latest_version}/create'

    parameters = {'name': name, 'description': description}

    headers = {'Content-Type': 'application/x-www-form-urlencoded'
        , 'X-StorageApi-Token': token}
    response = requests.post(url,
                             data=parameters,
                             headers=headers)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()['id']


def update_config_row(token, region, component_id, configurationId, row_id, name, description='', configuration=None,
                      state=None,
                      changeDescription='', branch_id=None, is_disabled=False, **kwargs):
    """
    Update table from CSV file.

    Args:
        component_id (str):
        name (str): The new table name (only alphanumeric and underscores)
        configuration (dict): configuration JSON; the maximum allowed size is 4MB
        state (dict): configuration JSON; the maximum allowed size is 4MB
        changeDescription (str): Escape character used in the CSV file.
        region: 'US' or 'EU'
        is_disabled:

    Returns:
        table_id (str): Id of the created table.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    if not branch_id:
        url = f'https://connection{URL_SUFFIXES[region]}/v2/storage/components/{component_id}/configs/' \
              f'{configurationId}/rows/{row_id}'
    else:
        url = f'https://connection{URL_SUFFIXES[region]}/v2/storage/branch/{branch_id}' \
              f'/components/{component_id}/configs/' \
              f'{configurationId}/rows/{row_id}'

    parameters = {}
    parameters['configurationId'] = configurationId
    if configuration:
        parameters['configuration'] = json.dumps(configuration)
    parameters['name'] = name
    parameters['description'] = description
    parameters['changeDescription'] = changeDescription
    parameters['isDisabled'] = str(is_disabled).lower()
    if state is not None:
        parameters['state'] = json.dumps(state)
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'X-StorageApi-Token': token}
    response = requests.put(url,
                            data=parameters,
                            headers=headers)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()


def create_config_row(token, region, component_id, configuration_id, name, configuration,
                      description='', rowId=None, state=None, changeDescription='', isDisabled=False,
                      branch_id=None, is_disabled=False, **kwargs):
    """
    Create a new table from CSV file.

    Args:
        component_id (str):
        name (str): The new table name (only alphanumeric and underscores)
        configuration (dict): configuration JSON; the maximum allowed size is 4MB
        state (dict): configuration JSON; the maximum allowed size is 4MB
        changeDescription (str): Escape character used in the CSV file.
        region: 'US' or 'EU'
        is_disabled:

    Returns:
        table_id (str): Id of the created table.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    if not branch_id:
        enpoint_prefix = 'components'
    else:
        enpoint_prefix = f'branch/{branch_id}/components'
    cl = Endpoint('https://connection' + URL_SUFFIXES[region], enpoint_prefix, token)

    url = '{}/{}/configs/{}/rows'.format(cl.base_url, component_id, configuration_id)
    parameters = {}
    # convert objects to string
    parameters['configuration'] = json.dumps(configuration)
    parameters['name'] = name
    parameters['description'] = description
    parameters['is_disabled'] = str(is_disabled).lower()
    if rowId:
        parameters['rowId'] = rowId
    parameters['changeDescription'] = changeDescription
    parameters['isDisabled'] = isDisabled
    if state:
        parameters['state'] = json.dumps(state)

    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = urllib.parse.urlencode(parameters)
    return cl._post(url, data=data, headers=header)


def clone_orchestration(src_token, dest_token, src_region, dst_region, orch_id):
    """
    Clones orchestration. Note that all component configs that are part of the tasks need to be migrated first using
    the migrate_config function. Otherwise it will fail.
    :param src_token:
    :param orch_id:
    :param dest_token:
    :param region:
    :return:
    """
    src_config = get_config_detail(src_token, src_region, 'orchestrator', orch_id)
    return create_orchestration(dest_token, dst_region, src_config['name'], src_config['configuration']['tasks'])


def create_orchestration(token, region, name, tasks, active=True, crontabRecord=None, crontabTimezone=None,
                         variableValuesId=None,
                         variableValuesData=None):
    values = {
        "name": name,
        "tasks": tasks,
        "active": active,
        "crontabRecord": crontabRecord,
        "crontabTimezone": crontabTimezone,
        "variableValuesId": variableValuesId,
        "variableValuesData": variableValuesData
    }

    headers = {
        'Content-Type': 'application/json',
        'X-StorageApi-Token': token
    }
    response = requests.post('https://syrup' + URL_SUFFIXES[region] + '/orchestrator/orchestrations',
                             data=json.dumps(values),
                             headers=headers)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()


def update_orchestration(token, region, orchestration_id, name, tasks, active=True, crontabRecord=None,
                         crontabTimezone=None,
                         variableValuesId=None,
                         variableValuesData=None):
    values = {
        "name": name,
        "tasks": tasks,
        "active": active,
        "crontabRecord": crontabRecord,
        "crontabTimezone": crontabTimezone,
        "variableValuesId": variableValuesId,
        "variableValuesData": variableValuesData
    }

    headers = {
        'Content-Type': 'application/json',
        'X-StorageApi-Token': token
    }
    response = requests.put(f'https://syrup{URL_SUFFIXES[region]}/orchestrator/orchestrations/{orchestration_id}',
                            data=json.dumps(values),
                            headers=headers)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()


def run_orchestration(orch_id, token, region='US'):
    headers = {
        'Content-Type': 'application/json',
        'X-StorageApi-Token': token
    }
    response = requests.post(
        'https://syrup' + URL_SUFFIXES[region] + '/orchestrator/orchestrations/' + str(orch_id) + '/jobs',
        headers=headers)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()


def get_orchestrations(token, region='US'):
    syrup_cl = Endpoint('https://syrup' + URL_SUFFIXES[region], 'orchestrator', token)

    url = syrup_cl.root_url + '/orchestrator/orchestrations'
    res = syrup_cl._get(url)
    return res


def _download_table(table, client: Tables, out_file):
    print('Downloading table %s into %s from source project', table['id'], out_file)
    res_path = client.export_to_file(table['id'], out_file, is_gzip=True, changed_until='')

    return res_path


PAR_WORKDIRPATH = os.path.dirname(os.path.join(os.path.abspath('')))


def transfer_storage_bucket(from_token, to_token, src_bucket_id, region_from='EU', region_to='EU', dest_bucket_id=None,
                            tmp_folder=os.path.join(PAR_WORKDIRPATH, 'data')):
    storage_api_url_from = 'https://connection' + URL_SUFFIXES[region_from]
    storage_api_url_to = 'https://connection' + URL_SUFFIXES[region_to]
    from_tables = Tables(storage_api_url_from, from_token)
    from_buckets = Buckets(storage_api_url_from, from_token)
    to_tables = Tables(storage_api_url_to, to_token)
    to_buckets = Buckets(storage_api_url_to, to_token)
    print('Getting tables from bucket %s', src_bucket_id)
    tables = from_buckets.list_tables(src_bucket_id)

    if dest_bucket_id:
        new_bucket_id = dest_bucket_id
    else:
        new_bucket_id = src_bucket_id

    bucket_exists = (new_bucket_id in [b['id'] for b in to_buckets.list()])

    for tb in tables:
        tb['new_id'] = tb['id'].replace(src_bucket_id, new_bucket_id)
        tb['new_bucket_id'] = new_bucket_id

        if bucket_exists and tb['new_id'] in [b['id'] for b in to_buckets.list_tables(new_bucket_id)]:
            print('Table %s already exists in destination bucket, skipping..', tb['new_id'])
            continue

        local_path = _download_table(tb, from_tables, tmp_folder)

        b_split = tb['new_bucket_id'].split('.')

        if not bucket_exists:
            print('Creating new bucket %s in destination project', tb['new_bucket_id'])
            to_buckets.create(b_split[1].replace('c-', ''), b_split[0])
            bucket_exists = True

        print('Creating table %s in the destination project', tb['id'])

        to_tables.create(tb['new_bucket_id'], tb['name'], local_path,
                         primary_key=tb['primaryKey'])
        # , compress=True)

        print('Deleting temp file')
        os.remove(local_path)
        # os.remove(local_path + '.gz')

    print('Finished.')


def migrate_configs(src_token, dst_token, src_config_id, component_id, src_region='EU', dst_region='EU',
                    use_src_id=False):
    """
    Super simple method, getting all table config objects and updating/creating them in the destination configuration.
    Includes all attributes, even the ones that are not updateble => API service will ignore them.

    :par use_src_id: If true the src config id will be used in the destination

    """
    src_config = get_config_detail(src_token, src_region, component_id, src_config_id)
    src_config_rows = get_config_rows(src_token, src_region, component_id, src_config_id)

    dst_config = src_config.copy()
    # add component id
    dst_config['component_id'] = component_id

    if use_src_id:
        dst_config['configurationId'] = src_config['id']

    # add token and region to use wrapping
    dst_config['token'] = dst_token
    dst_config['region'] = dst_region

    print('Transfering config..')
    new_cfg = create_config(**dst_config)

    print('Transfering config rows')
    for row in src_config_rows:
        row['component_id'] = component_id
        row['configuration_id'] = new_cfg['id']
        test = row['configuration'].pop('id', {})
        test = row['configuration'].pop('rowId', {})
        test = row.pop('rowId', {})

        # add token and region to use wrapping
        row['token'] = dst_token
        row['region'] = dst_region

        create_config_row(**row)


def update_config_state(token, region, component_id, configurationId, name, state):
    """

    Args:
        component_id (str):
        name (str): The config name
        state (dict): configuration JSON; the maximum allowed size is 4MB
        changeDescription (str): Escape character used in the CSV file.
        region: 'US' or 'EU'

    :return:
    """
    return update_config(token, region, component_id, configurationId, name, state=state,
                         changeDescription='Update state')


def create_branch(token, region, name, description=''):
    """
    Create a new development branch

    Args:
        name (str): The new branch name
        region: 'US' or 'EU'

    Returns:
        branch_id (str): Id of the created branch.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    cl = Endpoint('https://connection' + URL_SUFFIXES[region], 'dev-branches', token)
    url = cl.base_url + '/'
    parameters = {'name': name, 'description': description}
    # convert objects to string
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = urllib.parse.urlencode(parameters)
    resp = cl._post(url, data=data, headers=header)

    job = block_storage_job_until_completed(token, resp['url'])
    return job['results']['id']


# ------------ Management scripts ----------------

def create_new_project(storage_token, name, organisation, p_type='poc6months', region='EU',
                       defaultBackend='snowflake'):
    headers = {
        'Content-Type': 'application/json',
        'X-KBC-ManageApiToken': storage_token,
    }

    data = {
        "name": name,
        "type": p_type,
        "defaultBackend": defaultBackend
    }

    response = requests.post(
        f'https://connection{URL_SUFFIXES[region]}/manage/organizations/' + str(organisation) + '/projects',
        headers=headers, data=json.dumps(data))
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()


def invite_user_to_project(token, project_id, email, region='US'):
    headers = {
        'Content-Type': 'text/plain',
        'X-KBC-ManageApiToken': token
    }
    data = {
        "email": email
    }
    response = requests.post(
        f'https://connection{URL_SUFFIXES[region]}/manage/projects/' + str(project_id) + '/users',
        data=json.dumps(data),
        headers=headers)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return True


def generate_token(decription, token, proj_id, region, expires_in=1800, manage_tokens=False, additional_params=None):
    headers = {
        'Content-Type': 'application/json',
        'X-KBC-ManageApiToken': token,
    }

    data = {
        "description": decription,
        "canManageBuckets": True,
        "canReadAllFileUploads": False,
        "canPurgeTrash": False,
        "canManageTokens": manage_tokens,
        "bucketPermissions": {"*": "write"},
        "expiresIn": expires_in
    }

    response = requests.post(f'https://connection{URL_SUFFIXES[region]}/manage/projects/' + str(proj_id) + '/tokens',
                             headers=headers,
                             data=json.dumps(data))
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()


def get_organization(master_token, region, org_id):
    headers = {
        'Content-Type': 'application/json',
        'X-KBC-ManageApiToken': master_token,
    }

    response = requests.get(
        f'https://connection{URL_SUFFIXES[region]}/manage/organizations/' + str(org_id),
        headers=headers)
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise e
    else:
        return response.json()
