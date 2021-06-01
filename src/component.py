'''
Template Component main class.

'''
from dataclasses import dataclass

import datetime
import logging
import re
from dateutil import parser
from keboola.component.base import ComponentBase, UserException
from keboola.utils import helpers
from requests import HTTPError
from typing import Optional, Dict

from kbc_scripts import kbcapi_scripts

# configuration variables
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
        expiration = current - expiration_date

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
        self.ignored_properties: dict = self._get_ignored_properties_dict()
        self.orchestration_mapping = self._retrieve_orchestration_mapping()

        self.__source_token, self.__destination_token = None, None

    def run(self):
        '''
        Main execution code
        '''

        self._init_tokens()

        components, orchestrations = self._get_all_component_configurations_split_by_type()
        components = self._filter_components(components)
        # components = [c for c in components if c['id'] == 'keboola.ex-aws-s3']
        # for src_component in components:
        #     self.upsert_component_configurations_to_dst(component_id=src_component['id'],
        #                                                 src_configurations=src_component['configurations'])

        for orchestration in orchestrations:
            self.upsert_orchestrations_to_dst(orchestration['configurations'])

        self._store_state()

    def upsert_component_configurations_to_dst(self, component_id: str, src_configurations):
        for src_config in src_configurations:
            dst_config = self._get_configuration(component_id, src_config['id'])

            root_config, row_configs = self._split_configuration_parts(src_config, dst_config)

            row_configs = self._filter_ignored_row_properties(dst_config, row_configs)
            root_config = self._filter_ignored_properties(dst_config, root_config)

            logging.info(f"Updating component {component_id}, configuration ID {src_config['id']}")

            # UPDATES
            self._update_destination_rows(component_id, src_config['id'],
                                          row_configs['update'], mode='update')
            self._update_destination_config(component_id, root_config['update'], mode='update')

            # CREATES
            self._update_destination_rows(component_id, src_config['id'],
                                          row_configs['create'], mode='create')
            self._update_destination_config(component_id, root_config['create'], mode='create')

    def _update_destination_rows(self, component_id, configuration_id, rows, mode='create'):
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
            if mode == 'update':
                kbcapi_scripts.update_config_row(token=self.__destination_token,
                                                 region=self.region,
                                                 component_id=component_id,
                                                 configurationId=configuration_id,
                                                 row_id=row['id'],
                                                 name=row['name'],
                                                 description=row['description'],
                                                 configuration=row['configuration'],
                                                 changeDescription=change_description)
            elif mode == 'create':
                kbcapi_scripts.create_config_row(token=self.__destination_token,
                                                 region=self.region,
                                                 component_id=component_id,
                                                 configuration_id=configuration_id,
                                                 rowId=row['id'],
                                                 name=row['name'],
                                                 description=row['description'],
                                                 configuration=row['configuration'],
                                                 changeDescription=change_description)

    def _update_destination_config(self, component_id, configuration, mode='create'):
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
        if mode == 'update':
            kbcapi_scripts.update_config(token=self.__destination_token,
                                         region=self.region,
                                         component_id=component_id,
                                         configurationId=configuration['id'],
                                         name=configuration['name'],
                                         description=configuration['description'],
                                         configuration=configuration['configuration'],
                                         changeDescription=change_description)
        elif mode == 'create':
            kbcapi_scripts.create_config(token=self.__destination_token,
                                         region=self.region,
                                         component_id=component_id,
                                         configurationId=configuration['id'],
                                         name=configuration['name'],
                                         description=configuration['description'],
                                         configuration=configuration['configuration'],
                                         changeDescription=change_description)

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
        return root_config, row_configs

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
            ignored_properties = self.ignored_properties.get(config_key, [])
            # add secret values
            ignored_properties.extend(self._retrieve_encrypted_properties(row))

            row = self._replace_ignored_properties(changed_config=row,
                                                   original_config=dst_rows[row['id']],
                                                   ignored_properties=ignored_properties)
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

        row = root_config['update']
        key = self._build_config_key(dst_config['id'], row['id'])
        ignored_properties = self.ignored_properties.get(key, [])

        # add secret values
        ignored_properties.extend(self._retrieve_encrypted_properties(row))

        row = self._replace_ignored_properties(changed_config=row,
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

        def replace_value(element, config: dict, value):
            keys = element.split('.')

            rv = config.copy()
            for index, key in enumerate(keys):
                rv = rv[key]
                if index == len(keys) - 1:
                    # replace
                    config[key] = value

            return config

        if ignored_properties:

            for ignored_property_path in ignored_properties:
                ignored_property_path = f'configuration.parameters.{ignored_property_path}'
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
            ignored_dict[key] = c[KEY_IGNORED_PROPERTIES]
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
        rows_match = r'.+\/(writers|extractors|applications)\/(.+\..+)\/(\d+)\/rows\/(\d+)'
        cfg_match = r'.+\/(writers|extractors|applications)\/(.+\..+)\/(\d+)\/?'

        match = re.match(cfg_match, cfg_url)
        if not match:
            raise UserException(f'Provided configuration URL is invalid: {cfg_url}')
        else:
            config_id = match.groups()[3]

        match = re.match(rows_match, cfg_url)
        if match:
            row_id = match.groups()[4]
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

    def _get_all_component_configurations_split_by_type(self):
        """
        Separates orchestrations from normal components.

        Returns:

        """
        src_components = kbcapi_scripts.list_project_components(self.__source_token, self.region,
                                                                include='configuration,rows')
        orchestrations = []
        components = []
        for c in src_components:
            if c['id'] == 'orchestrator':
                orchestrations.append(c)
            else:
                components.append(c)
        return components, orchestrations

    def _build_change_description(self, custom_text):
        if self.run_mode == DEV_TO_PROD_MODE:
            mode = 'SYNC FROM DEV'
        else:
            mode = 'SYNC FROM PROD'
        merge_message = self.configuration.parameters.get('merge_message', '')
        return f'{merge_message} - {mode}: {custom_text}, runID:{self.environment_variables.run_id}'

    def _init_tokens(self):
        self.__source_token = self._init_project_storage_token(self.src_project_id)
        self.__destination_token = self._init_project_storage_token(self.dst_project_id)

    def _init_project_storage_token(self, project_id):
        project_pk = self._build_project_pk(project_id)
        storage_token = self.__token_cache.get(project_pk)

        if not storage_token or storage_token.is_expired():
            logging.info(f'Generating token for project {self.region}-{project_id}')
            token = kbcapi_scripts.generate_token('DEV/PROD Sync Application', self.mange_token,
                                                  project_id, self.region, manage_tokens=True)
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

        for key, token_dict in self.get_state_file().get(KEY_TOKENS_CACHE, {}).items():
            storage_token = StorageToken.try_build_from_dict(token_dict)
            if storage_token:
                cache[key] = storage_token
        return cache

    def _filter_components(self, components):
        skipped_cfg = self.configuration.parameters[KEY_SKIPPED_COMPONENTS]
        skipped_ids = helpers.comma_separated_values_to_list(skipped_cfg)
        return [c for c in components if c['id'] not in skipped_ids]

    def _store_state(self):
        state = {KEY_TOKENS_CACHE: self._get_token_cache_dict(),
                 KEY_ORCHESTRATION_MAPPING: self.orchestration_mapping}

        self.write_state_file(state)

    def _get_token_cache_dict(self):
        cache_dict = {}
        for key, token in self.__token_cache.items():
            cache_dict[key] = token.to_dict()
        return cache_dict

    def upsert_orchestrations_to_dst(self, orchestration_cfgs: dict):
        for cfg in orchestration_cfgs:
            project_pk = self._build_project_pk(self.src_project_id)
            existing_orchestration_id = self.orchestration_mapping.get(project_pk, {}).get(cfg['id'])
            if existing_orchestration_id:
                dst_configuration = self._get_configuration('orchestrator', existing_orchestration_id)
            else:
                logging.info(f"Creating component orchestrator, source configuration ID {cfg['id']}")
                cfg_pars = cfg['configuration']
                new_orchestration = kbcapi_scripts.create_orchestration(self.__destination_token, self.region,
                                                                        cfg['name'],
                                                                        cfg['configuration']['tasks'],
                                                                        crontabRecord=cfg_pars.get(
                                                                            'crontabRecord'),
                                                                        crontabTimezone=cfg_pars.get('crontabTimezone'),
                                                                        variableValuesId=cfg_pars.get(
                                                                            'variableValuesId'),
                                                                        variableValuesData=cfg_pars.get(
                                                                            'variableValuesData')
                                                                        )
                self._add_orchestration_mapping(cfg['id'], new_orchestration['id'])

    def _add_orchestration_mapping(self, src_id, dst_id):
        project_pk = self._build_project_pk(self.src_project_id)
        if not self.orchestration_mapping.get(project_pk):
            self.orchestration_mapping[project_pk] = {}
        self.orchestration_mapping[project_pk][src_id] = dst_id

    def _retrieve_orchestration_mapping(self):
        return self.get_state_file().get(KEY_ORCHESTRATION_MAPPING, {})


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
