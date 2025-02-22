'''
Template Component main class.

'''
import datetime
import logging
import re
from dataclasses import dataclass
from typing import Optional, Dict, List

from dateutil import parser
from keboola.component.base import ComponentBase, UserException
from keboola.utils import helpers
from requests import HTTPError

from kbc_scripts import kbcapi_scripts

# configuration variables
KEY_TRANSFER_STATES = 'transfer_states'
DEFAULT_IGNORED_ROOT_PROPERTIES = ['authorization']
KEY_ORCHESTRATION_MAPPING = 'orchestration_mapping'
KEY_SKIPPED_COMPONENTS = 'skipped_components'
KEY_TOKENS_CACHE = 'storage_tokens_cache'
PROD_TO_DEV_MODE = 'prod_to_dev'
DEV_TO_PROD_MODE = 'dev_to_prod'
KEY_API_TOKEN = '#api_token'
KEY_MODE = 'mode'
KEY_REGION = 'region'

KEY_PROD_PROJ_ID = 'prod_id'
KEY_DEV_PROJ_ID = 'dev_id'
KEY_CONFIG_OVERRIDE = 'configuration_override'
KEY_NAME = 'name'
KEY_CFG_URL = 'config_url'
KEY_IGNORED_PROPERTIES = 'ignored_properties'
KEY_IGNORE_INACTIVE_ORCH = 'ignore_inactive_orchestration_updates'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_API_TOKEN, KEY_MODE, KEY_REGION, KEY_PROD_PROJ_ID, KEY_DEV_PROJ_ID]
REQUIRED_IMAGE_PARS = []


@dataclass
class StorageToken:
    id: str
    token: str
    expires: str

    @classmethod
    def try_build_from_dict(cls, token_dict: dict) -> Optional['StorageToken']:
        if all(x in ['id', '#token', 'expires'] for x in token_dict):
            return cls(token_dict['id'], token_dict['#token'], token_dict['expires'])
        else:
            return None

    def is_expired(self):
        expiration_date = self._get_expires_timestamp()
        if not expiration_date:
            return True

        current = int(datetime.datetime.now().timestamp())
        expiration = expiration_date - current
        logging.debug(f"Token expiration: {expiration_date}, current_timestamp:{current}. Diff: {expiration}")

        if expiration <= (10 * 60):
            return True
        else:
            return False

    def _get_expires_timestamp(self) -> Optional[int]:

        if self.expires == '' or self.expires is None:
            return None
        else:
            return int(parser.parse(self.expires).timestamp())

    def to_dict(self):
        return {'id': self.id,
                '#token': self.token,
                'expires': self.expires}


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

        # initialization
        params = self.configuration.parameters
        self.src_project_id, self.dst_project_id = self._get_project_ids()
        self.__token_cache: Dict[str, StorageToken] = self._build_token_cache()
        self.region = params[KEY_REGION]
        self.mange_token = params[KEY_API_TOKEN]
        self.run_mode = params[KEY_MODE]
        self.ignored_properties_cfg: dict = self._get_ignored_properties_dict()
        self.orchestration_mapping = self._retrieve_orchestration_mapping()
        self.ignore_inactive_orch = params.get(KEY_IGNORE_INACTIVE_ORCH, False)
        self.branch_mode = params.get("branch_mode", False)

        self.__source_token, self.__destination_token = None, None

        skipped_cfg = self.configuration.parameters[KEY_SKIPPED_COMPONENTS]
        self.skipped_component_ids = helpers.comma_separated_values_to_list(skipped_cfg)

    def run(self):
        '''
        Main execution code
        '''

        logging.warning(
            f'Running in mode {self.run_mode}, syncing from project {self.src_project_id} to project '
            f'{self.dst_project_id}')
        self._init_tokens()

        branch_id = None
        if self.branch_mode:
            branch_id = self._create_new_branch()

        src_components, src_orchestrations = self._get_all_component_configurations_split_by_type(
            project='source')
        src_components = self._filter_components(src_components)
        if 'orchestrator' in self.skipped_component_ids:
            src_orchestrations = {}
        for src_component in src_components:
            self.upsert_component_configurations_to_dst(component_id=src_component['id'],
                                                        src_configurations=src_component['configurations'],
                                                        branch_id=branch_id)
        # create linked transformations first
        # safe because KBC does not allow to nest deeper than 1
        orchestration_list = self._order_orchestration_by_link(src_orchestrations.get('configurations', []))
        self.upsert_orchestrations_to_dst(orchestration_list)

        # TODO: remove configurations

        self._store_state()

    def _order_orchestration_by_link(self, orchestrations: list):
        non_prio_list = orchestrations.copy()
        prio_list = []
        for c in orchestrations:
            for task in c['configuration'].get('tasks', []):
                if task.get('component', '') == 'orchestrator':
                    orchestration_id = task['actionParameters']['config']
                    cfg = self._pop_orchestration_id_from_list(non_prio_list, int(orchestration_id))
                    if cfg:
                        prio_list.append(cfg)
        prio_list.extend(non_prio_list)
        return prio_list

    def _pop_orchestration_id_from_list(self, orchestrations: list, orchestration_id):
        pop_index = None
        found_element = None
        for idx, o in enumerate(orchestrations):
            if int(o['id']) == orchestration_id:
                pop_index = idx
                break
        if pop_index:
            found_element = orchestrations.pop(pop_index)
        return found_element

    def _create_new_branch(self):
        description = self._build_change_description('')

        merge_message = self.configuration.parameters.get('merge_message', '')
        name = f'{merge_message}. Time: {datetime.datetime.utcnow().isoformat()}'
        branch_id = kbcapi_scripts.create_branch(self.__destination_token, self.region,
                                                 name, description)
        return branch_id

    def upsert_component_configurations_to_dst(self, component_id: str, src_configurations, branch_id=None):
        for src_config in src_configurations:
            dst_config = self._get_configuration(component_id, src_config['id'])

            root_config, row_configs = self._split_configuration_parts(src_config, dst_config)

            row_configs = self._filter_ignored_row_properties(dst_config, row_configs)
            root_config = self._filter_ignored_properties(dst_config, root_config)
            root_config = self._skip_auth_properties(root_config)

            logging.info(f"Updating component {component_id}, configuration ID {src_config['id']}")

            if root_config.get('update') and component_id == 'keboola.orchestrator' and self.ignore_inactive_orch:
                logging.warning(f'Ignoring disabled orchestration state, ID: {root_config["update"]["id"]}')
                root_config['update']['isDisabled'] = dst_config['isDisabled']

            # UPDATES
            self._update_destination_config(component_id, root_config['update'], mode='update', branch_id=branch_id)
            self._update_destination_rows(component_id, src_config['id'],
                                          row_configs['update'], mode='update', branch_id=branch_id)

            # CREATES
            self._update_destination_config(component_id, root_config['create'], mode='create', branch_id=branch_id)
            self._update_destination_rows(component_id, src_config['id'],
                                          row_configs['create'], mode='create', branch_id=branch_id)

    def _update_destination_rows(self, component_id, configuration_id, rows, mode='create', branch_id=None):
        """
        Updates rows in destination project
        Args:
            component_id:
            configuration_id:
            rows:

        Returns:

        """
        # TODO: output log
        for row in rows:
            change_description = self._build_change_description(f'Row {row["id"]} {mode}d')
            state = row.get('state', {}) if self.configuration.parameters.get(KEY_TRANSFER_STATES) else None
            if mode == 'update':
                kbcapi_scripts.update_config_row(token=self.__destination_token,
                                                 region=self.region,
                                                 component_id=component_id,
                                                 configurationId=configuration_id,
                                                 row_id=row['id'],
                                                 name=row['name'],
                                                 state=state,
                                                 description=row['description'],
                                                 configuration=row['configuration'],
                                                 changeDescription=change_description,
                                                 branch_id=branch_id,
                                                 is_disabled=row['isDisabled'])
            elif mode == 'create':
                kbcapi_scripts.create_config_row(token=self.__destination_token,
                                                 region=self.region,
                                                 component_id=component_id,
                                                 configuration_id=configuration_id,
                                                 rowId=row['id'],
                                                 name=row['name'],
                                                 state=state,
                                                 description=row['description'],
                                                 configuration=row['configuration'],
                                                 changeDescription=change_description,
                                                 branch_id=branch_id,
                                                 isDisabled=row['isDisabled'])

    def _update_destination_config(self, component_id, configuration, mode='create', branch_id=None):
        """
        Updates rows in destination project
        Args:
            component_id:
            configuration_id:
            configuration:

        Returns:

        """
        # TODO: output log
        if not configuration:
            return

        change_description = self._build_change_description(f'Config {mode}d')
        state = configuration.get('state', {}) if self.configuration.parameters.get(KEY_TRANSFER_STATES) else None
        if mode == 'update':
            kbcapi_scripts.update_config(token=self.__destination_token,
                                         region=self.region,
                                         component_id=component_id,
                                         configurationId=configuration['id'],
                                         name=configuration['name'],
                                         description=configuration['description'],
                                         configuration=configuration['configuration'],
                                         state=state,
                                         changeDescription=change_description,
                                         branch_id=branch_id,
                                         is_disabled=configuration['isDisabled'])
        elif mode == 'create':
            kbcapi_scripts.create_config(token=self.__destination_token,
                                         region=self.region,
                                         component_id=component_id,
                                         configurationId=configuration['id'],
                                         name=configuration['name'],
                                         description=configuration['description'],
                                         configuration=configuration['configuration'],
                                         state=state,
                                         changeDescription=change_description,
                                         branch_id=branch_id,
                                         is_disabled=configuration['isDisabled'])

    def _split_configuration_parts(self, src_configuration: dict, dst_configuration: dict):

        row_configs = {"update": [],
                       "create": []}
        root_config = {"update": None,
                       "create": None}

        if dst_configuration:
            dst_row_ids = [row['id'] for row in dst_configuration.get('rows', [])]
            for row in src_configuration.get('rows', []):
                if row['id'] in dst_row_ids:
                    row_configs['update'].append(row)
                else:
                    row_configs['create'].append(row)
            root_config['update'] = src_configuration

        else:
            root_config['create'] = src_configuration
            for row in src_configuration.get('rows', []):
                row_configs['create'].append(row)
        return root_config, row_configs

    def _skip_auth_properties(self, configuration: dict):
        create_config = configuration['create']
        if not create_config:
            return configuration

        if create_config.get('configuration', {}).get('authorization'):
            create_config['configuration']['authorization'] = {}
        return configuration

    def _filter_ignored_row_properties(self, dst_config, row_configs):
        """
        Change only updated (existing in remote) / newly created are transferred
        Args:
            dst_config:
            row_configs:

        Returns:

        """
        if not dst_config:
            return row_configs

        dst_rows = {k['id']: k for k in dst_config['rows']}
        new_cfg_rows = []

        # we know that rows in update mode are in remote config
        for row in row_configs['update']:
            config_key = self._build_config_key(dst_config['id'], row['id'])

            ignored_parameter_properties = self.ignored_properties_cfg.get(config_key, [])
            # add secret values
            ignored_parameter_properties.extend(self._retrieve_encrypted_properties(row))
            ignored_parameter_properties = [f'parameters.{p}' for p in ignored_parameter_properties]

            row = self._replace_ignored_properties(changed_config=row,
                                                   original_config=dst_rows[row['id']],
                                                   ignored_properties=ignored_parameter_properties)
            new_cfg_rows.append(row)
        row_configs['update'] = new_cfg_rows
        return row_configs

    def _filter_ignored_properties(self, dst_config, root_config):
        """
        Change only updated (existing in remote) / newly created are transferred
        Args:
            dst_config:
            root_config:

        Returns:

        """
        if not dst_config:
            return root_config
        configuration = root_config['update']
        key = self._build_config_key(configuration['id'])
        ignored_properties = []

        # ignore authentication (oAuth) by default
        if configuration['configuration'].get('authorization'):
            ignored_properties.append('authorization')

        ignored_parameter_properties = []
        ignored_parameter_properties.extend(self.ignored_properties_cfg.get(key, []))
        ignored_parameter_properties = [f'parameters.{p}' for p in ignored_parameter_properties]

        ignored_properties.extend(ignored_parameter_properties)
        # add secret values
        ignored_properties.extend([f'parameters.{p}' for p in self._retrieve_encrypted_properties(configuration)])

        row = self._replace_ignored_properties(changed_config=configuration,
                                               original_config=dst_config,
                                               ignored_properties=ignored_properties)
        root_config['update'] = row
        return root_config

    def _replace_ignored_properties(self, changed_config, original_config, ignored_properties):
        def find_value(element, config: dict):
            keys = element.split('.')
            rv = config.copy()

            for key in keys:
                try:
                    rv = rv[key]
                except KeyError:
                    logging.debug(f'Key {key} not found in {config} when looking up the value {element}')
                    break

            return rv

        def replace_value(element_path: str, dict_object: dict, value):
            """
            Inplace change dictionary element. Object position in hierarchy delimited by .
            e.g. config['configuration']['db'] => 'configuration.db'
            Args:
                element_path (str): element path, delimited by . E.g. 'configuration.db'
                dict_object (dict): dictionary object
                value: Value to put on the defined position

            Returns:

            """
            keys = element_path.split('.')

            rv = dict_object
            prev_object = dict_object
            for index, key in enumerate(keys):
                rv = rv.get(key)
                if index == len(keys) - 1:
                    # replace
                    prev_object[key] = value
                prev_object = prev_object[key]

            return dict_object

        if ignored_properties:

            for ignored_property_path in ignored_properties:
                ignored_property_path = f'configuration.{ignored_property_path}'
                original_value = find_value(ignored_property_path, original_config)
                changed_value = find_value(ignored_property_path, changed_config)

                if original_value and changed_value:
                    changed_config = replace_value(ignored_property_path, changed_config, original_value)

        return changed_config

    @staticmethod
    def _retrieve_encrypted_properties(configuration):
        def find_secret(path: str, config_part, current_key: str):
            if path:
                result_path = f'{path}.{current_key}'
            else:
                result_path = current_key
            if current_key.startswith('#'):
                secret_key_paths.append(result_path)

            elif isinstance(config_part, dict):
                for key, value in config_part.items():
                    find_secret(result_path, value, key)

        secret_key_paths = []
        parameters = configuration['configuration'].get('parameters', {})
        for par in parameters:
            find_secret('', parameters[par], par)

        return secret_key_paths

    def _get_ignored_properties_dict(self):
        cfg_override = self.configuration.parameters.get(KEY_CONFIG_OVERRIDE, [])
        ignored_dict = {}
        for c in cfg_override:
            config_id, row_id = self._parse_config_url(c[KEY_CFG_URL])
            key = self._build_config_key(config_id, row_id)
            ignored_dict[key] = [p.strip() for p in c[KEY_IGNORED_PROPERTIES].split(',')]
        return ignored_dict

    def _build_config_key(self, configuration_id: str, row_id: str = None):
        key = str(configuration_id)
        if row_id:
            key += f'.{row_id}'
        return key

    @staticmethod
    def _parse_config_url(cfg_url: str):
        config_id, row_id = None, None
        if not cfg_url.endswith('/'):
            cfg_url += '/'
        rows_match = r'.+\/(writers|extractors|applications|components)\/(.+\..+)\/(\d+)\/rows\/(\d+)'
        cfg_match = r'.+\/(writers|extractors|applications|components)\/(.+)?'

        match = re.match(cfg_match, cfg_url)
        if not match:
            raise UserException(f'Provided configuration URL is invalid: {cfg_url}')
        else:
            config_id = match.groups()[1].split('/')[1]

        match = re.match(rows_match, cfg_url)
        if match:
            row_id = match.groups()[3]
        return config_id, row_id

    def _get_configuration(self, component_id, configuration_id):
        configuration = None
        try:
            configuration = kbcapi_scripts.get_config_detail(self.__destination_token, self.region, component_id,
                                                             configuration_id)
        except HTTPError as e:
            if e.response.status_code != 404:
                raise e
        return configuration

    def _get_all_component_configurations_split_by_type(self, project='source'):
        """
        Separates orchestrations from normal components.

        Returns:

        """
        if project == 'source':
            token = self.__source_token
        else:
            token = self.__destination_token

        src_components = kbcapi_scripts.list_project_components(token, self.region,
                                                                include='configuration,rows,state')
        orchestrations = {}
        components = []
        for c in src_components:
            if c['id'] == 'orchestrator':
                orchestrations = c
            else:
                components.append(c)
        return components, orchestrations

    def _get_all_schedules(self, project='source'):
        """
        Get all schedules
        Returns:

        """
        if project == 'source':
            token = self.__source_token
        else:
            token = self.__destination_token

        return kbcapi_scripts.get_schedules(self.region, token)

    def _build_change_description(self, custom_text):
        if self.run_mode == DEV_TO_PROD_MODE:
            mode = 'SYNC FROM DEV'
        else:
            mode = 'SYNC FROM PROD'
        merge_message = self.configuration.parameters.get('merge_message', '')
        return f'{merge_message} - {mode}: {custom_text}, runID:{self.environment_variables.run_id}, ' \
               f'Time: {datetime.datetime.utcnow().isoformat()}'

    def _init_tokens(self):
        self.__source_token = self._init_project_storage_token(self.src_project_id)
        self.__destination_token = self._init_project_storage_token(self.dst_project_id)

    def _init_project_storage_token(self, project_id):
        project_pk = self._build_project_pk(project_id)

        if not self.configuration.parameters.get("branch_mode"):
            storage_token = self.__token_cache.get(project_pk)
        else:
            # use user master tokens in case of branch mode
            master_tokens = [self.configuration.parameters['master_tokens']['#dev_token'],
                             self.configuration.parameters['master_tokens']['#prod_token']]
            token_key: str = [t for t in master_tokens if t.startswith(project_id)][0]
            storage_token = StorageToken(token_key.split('-')[1], token_key, "2050-11-01T11:18:52+0100")

        if not storage_token or storage_token.is_expired():
            logging.info(f'Generating token for project {self.region}-{project_id}')
            try:
                token = kbcapi_scripts.generate_token('DEV/PROD Sync Application', self.mange_token,
                                                      project_id, self.region, manage_tokens=True)
            except HTTPError as e:
                if e.response.status_code == 401:
                    raise UserException("Cannot create Storage token. Invalid Manage token provided.")
                else:
                    raise

            storage_token = StorageToken(token['id'], token['token'], token['expires'])
            # update storage token
            self._update_storage_token_cache(project_pk, storage_token)

        return storage_token.token

    def _get_project_ids(self):
        mode = self.configuration.parameters[KEY_MODE]

        if mode == DEV_TO_PROD_MODE:
            src_project_id = self.configuration.parameters[KEY_DEV_PROJ_ID]
            dst_project_id = self.configuration.parameters[KEY_PROD_PROJ_ID]
        elif mode == PROD_TO_DEV_MODE:
            src_project_id = self.configuration.parameters[KEY_PROD_PROJ_ID]
            dst_project_id = self.configuration.parameters[KEY_DEV_PROJ_ID]
        else:
            raise UserException(
                f"Mode {mode} is invalid! Supported modes are '{DEV_TO_PROD_MODE}' and '{PROD_TO_DEV_MODE}'")
        return src_project_id, dst_project_id

    def _build_project_pk(self, project_id):
        return f'{self.region}-{project_id}'

    def _update_storage_token_cache(self, key, storage_token: StorageToken):
        self.__token_cache[key] = storage_token

    def _build_token_cache(self):
        cache = {}
        state_cache = self.get_state_file().get(KEY_TOKENS_CACHE, {})
        # fix ancient kbc bug
        if isinstance(state_cache, list):
            state_cache = {}

        for key, token_dict in state_cache.items():
            storage_token = StorageToken.try_build_from_dict(token_dict)
            if storage_token:
                cache[key] = storage_token
        return cache

    def _filter_components(self, components):
        return [c for c in components if c['id'] not in self.skipped_component_ids]

    def _store_state(self):
        state = {KEY_TOKENS_CACHE: self._get_token_cache_dict(),
                 KEY_ORCHESTRATION_MAPPING: self.orchestration_mapping}

        self.write_state_file(state)

    def _get_token_cache_dict(self):
        cache_dict = {}
        for key, token in self.__token_cache.items():
            cache_dict[key] = token.to_dict()
        return cache_dict

    def _replace_linked_orchestrations(self, orchestration_cfg: dict, project_pk):
        for task in orchestration_cfg.get('tasks', []):
            if task.get('component', '') == 'orchestrator':
                orchestration_id = task['actionParameters']['config']
                task['actionParameters']['config'] = self.orchestration_mapping.get(project_pk, {}).get(
                    str(orchestration_id))

    def upsert_orchestrations_to_dst(self, orchestration_cfgs: List[dict]):
        for cfg in orchestration_cfgs:
            project_pk = self._build_project_pk(self.src_project_id)
            if not cfg.get('id'):
                raise Exception(f'Orchestration config does not contain ID: {cfg}')
            existing_orchestration_id = self.orchestration_mapping.get(project_pk, {}).get(cfg['id'])
            cfg_pars = cfg['configuration']
            self._replace_linked_orchestrations(cfg_pars, project_pk)
            if existing_orchestration_id:
                logging.info(f"Updating orchestrator, source configuration ID {cfg['id']}")
                dst_configuration = self._get_configuration('orchestrator', existing_orchestration_id)

                if not dst_configuration:
                    logging.warning(
                        f"Matching orchestration ID {existing_orchestration_id} does not exist in the remote project "
                        f"{self._build_project_pk(self.dst_project_id)}!"
                        f"It was probably removed manually. Please recreate it or drop from state file.")
                    continue

                # ignore state
                if self.ignore_inactive_orch:
                    cfg_pars['active'] = dst_configuration['configuration']['active']

                kbcapi_scripts.update_orchestration(self.__destination_token, self.region,
                                                    dst_configuration['id'],
                                                    cfg['name'],
                                                    cfg['configuration']['tasks'],
                                                    active=cfg_pars.get('active'),
                                                    crontabRecord=cfg_pars.get('crontabRecord'),
                                                    crontabTimezone=cfg_pars.get('crontabTimezone'),
                                                    variableValuesId=cfg_pars.get('variableValuesId'),
                                                    variableValuesData=cfg_pars.get('variableValuesData')
                                                    )
            else:
                logging.info(f"Creating orchestrator, source configuration ID {cfg['id']}")
                new_orchestration = kbcapi_scripts.create_orchestration(self.__destination_token, self.region,
                                                                        cfg['name'],
                                                                        cfg['configuration']['tasks'],
                                                                        active=cfg_pars.get('active'),
                                                                        crontabRecord=cfg_pars.get('crontabRecord'),
                                                                        crontabTimezone=cfg_pars.get('crontabTimezone'),
                                                                        variableValuesId=cfg_pars.get(
                                                                            'variableValuesId'),
                                                                        variableValuesData=cfg_pars.get(
                                                                            'variableValuesData')
                                                                        )
                if not cfg_pars.get('active'):
                    # update activity because can't do on create
                    kbcapi_scripts.update_orchestration(self.__destination_token, self.region,
                                                        new_orchestration['id'],
                                                        cfg['name'],
                                                        cfg['configuration']['tasks'],
                                                        active=cfg_pars.get('active'))
                self._add_orchestration_mapping(cfg['id'], new_orchestration['id'])

    def _add_orchestration_mapping(self, src_id, dst_id):
        """
        Add mapping for both directions
        Args:
            src_id:
            dst_id:

        Returns:

        """
        # SRC direction
        project_pk = self._build_project_pk(self.src_project_id)
        if not self.orchestration_mapping.get(project_pk):
            self.orchestration_mapping[project_pk] = {}

        self.orchestration_mapping[project_pk][src_id] = dst_id

        # DST direction
        project_pk = self._build_project_pk(self.dst_project_id)
        if not self.orchestration_mapping.get(project_pk):
            self.orchestration_mapping[project_pk] = {}

        self.orchestration_mapping[project_pk][dst_id] = src_id

    def _retrieve_orchestration_mapping(self):
        return self.get_state_file().get(KEY_ORCHESTRATION_MAPPING) or {}


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
