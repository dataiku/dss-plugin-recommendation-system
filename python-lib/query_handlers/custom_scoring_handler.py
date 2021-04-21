from query_handlers import ScoringHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2
import dku_constants as constants
import logging

logger = logging.getLogger(__name__)


class CustomScoringHandler(ScoringHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.dku_config.similarity_scores_type == constants.SIMILARITY_TYPE.USER_SIMILARITY.value:
            self.based_column = self.dku_config.users_column_name
            self.pivot_column = self.dku_config.items_column_name
        else:
            self.based_column = self.dku_config.items_column_name
            self.pivot_column = self.dku_config.users_column_name

    def build(self):
        # inputs
        samples_dataset = self.file_manager.samples_dataset

        # Build all queries
        cast_mapping = {self.dku_config.users_column_name: "string", self.dku_config.items_column_name: "string"}
        samples_cast = self._cast_table(samples_dataset, cast_mapping, alias="_input_dataset")
        visit_count = self._build_visit_count(samples_cast)
        normed_count = self._build_normed_count(visit_count)
        similarity = self.file_manager.similarity_scores_dataset

        renaming_mapping = {
            self.dku_config.similarity_column_1_name: f"{self.based_column}_1",
            self.dku_config.similarity_column_2_name: f"{self.based_column}_2",
            self.dku_config.similarity_score_column_name: constants.SIMILARITY_COLUMN_NAME,
        }
        similarity_renamed = self._rename_table(similarity, renaming_mapping)
        cast_mapping = {
            f"{self.based_column}_1": "string",
            f"{self.based_column}_2": "string",
            constants.SIMILARITY_COLUMN_NAME: "double",
        }
        similarity_cast = self._cast_table(similarity_renamed, cast_mapping, alias="_sim_renamed")
        row_numbers = self._build_row_numbers(similarity_cast)
        top_n = self._build_top_n(row_numbers)
        cf_scores = self._build_collaborative_filtering(top_n, normed_count)
        # final sql query
        # print(toSQL(cf_scores, dataset=samples_dataset))
        self.query = toSQL(cf_scores, dataset=samples_dataset)
        print("self.query :\n", self.query)
