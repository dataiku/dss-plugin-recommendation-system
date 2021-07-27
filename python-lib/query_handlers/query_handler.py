from dataiku.sql import Column, Constant, SelectQuery, toSQL, Window
from dataiku.core.sql import SQLExecutor2
import dku_constants as constants
from dku_dialects import SUPPORTED_DIALECTS, SUPPORTS_FULL_OUTER_JOIN, SUPPORTS_WITH_CLAUSE
from dku_utils import set_column_description
import logging

logger = logging.getLogger(__name__)


class QueryHandler:
    def __init__(self, dku_config, file_manager):
        self.dku_config = dku_config
        self.file_manager = file_manager
        self.query = None
        self._check_supported_dialect()

    def build(self):
        pass

    def _execute(self, table, output_dataset):
        query = toSQL(table, dataset=output_dataset)
        logger.info(f"Executing query:\n{query}")
        sql_executor = SQLExecutor2(dataset=output_dataset)
        sql_executor.exec_recipe_fragment(output_dataset, query)
        logger.info("Done executing query !")

    def _rename_table(self, to_rename, renaming_mapping):
        renamed_table = SelectQuery()
        renamed_table.select_from(to_rename, alias="_renamed")
        for input_column, renamed_column in renaming_mapping.items():
            renamed_table.select(Column(input_column, table_name="_renamed"), alias=renamed_column)
        return renamed_table

    def _cast_table(self, to_cast, cast_mapping, alias):
        cast_table = SelectQuery()
        cast_table.select_from(to_cast, alias=alias)
        for input_column, target_type in cast_mapping.items():
            cast_table.select(Column(input_column, table_name=alias).cast(target_type), alias=input_column)
        return cast_table

    def _get_cast_type(self, column_name, dataset):
        dataset_schema = dataset.read_schema()
        column_type = next((column["type"] for column in dataset_schema if column["name"] == column_name), "string")
        return constants.DSS_TO_SQL_TYPES.get(column_type, "string")

    def _select_columns_list(self, select_query, column_names, table_name=None):
        for col_name in column_names:
            select_query.select(Column(col_name, table_name=table_name))

    def _or_condition_columns_list(self, select_query, column_names, condition_method):
        for i, col_name in enumerate(column_names):
            if i == 0:
                or_condition = condition_method(Column(col_name))
            else:
                or_condition = or_condition.or_(condition_method(Column(col_name)))
        select_query.where(or_condition)

    def _build_identity(self, select_query):
        return select_query

    def _set_column_description(self, output_dataset, column_name=None):
        column_descriptions = self._get_column_descriptions(column_name)
        set_column_description(output_dataset, column_descriptions)
        pass

    def _get_column_descriptions(self, column_name):
        raise NotImplementedError()

    def _check_supported_dialect(self):
        connection_type = self._get_unique_dialect()
        if connection_type not in SUPPORTED_DIALECTS:
            raise ValueError(
                f"""
                Connection type '{connection_type}' is not supported by the plugin.
                Supported connection types are {list(SUPPORTED_DIALECTS.keys())}
                """
            )
        self.supports_full_outer_join = SUPPORTED_DIALECTS[connection_type].get(SUPPORTS_FULL_OUTER_JOIN, False)
        self.supports_with_clause = SUPPORTED_DIALECTS[connection_type].get(SUPPORTS_WITH_CLAUSE, False)

    def _get_unique_dialect(self):
        connection_types, connection_names = [], []
        for dataset in self.file_manager.values():
            if dataset is not None:
                connection_types += [dataset.get_config().get("type")]
                connection_names += [dataset.get_config().get("params").get("connection")]
        if len(set(connection_types)) != 1 or len(set(connection_names)) != 1:
            raise ValueError("All inputs and outputs datasets must be in the same connection.")
        return connection_types[0]