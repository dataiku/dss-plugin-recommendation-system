from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2


class QueryHandler:
    def __init__(self, dku_config, file_manager):
        self.dku_config = dku_config
        self.file_manager = file_manager
        self.query = None

    def build(self):
        pass

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

    def _select_columns_list(self, select_query, column_names):
        for col_name in column_names:
            select_query.select(Column(col_name))