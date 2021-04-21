from query_handlers import ScoringHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2
import dku_constants as constants
import logging

logger = logging.getLogger(__name__)


class AutoScoringHandler(ScoringHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.dku_config.collaborative_filtering_method == constants.CF_METHOD.USER_BASED.value:
            self.based_column = self.dku_config.users_column_name
            self.pivot_column = self.dku_config.items_column_name
        else:
            self.based_column = self.dku_config.items_column_name
            self.pivot_column = self.dku_config.users_column_name
        self.output_similarity_matrix = self.file_manager.similarity_scores_dataset is not None

    def build(self):
        # inputs
        samples_dataset = self.file_manager.samples_dataset

        # Build all queries
        cast_mapping = {self.dku_config.users_column_name: "string", self.dku_config.items_column_name: "string"}
        samples_cast = self._cast_table(samples_dataset, cast_mapping, alias="_input_dataset")
        visit_count = self._build_visit_count(samples_cast)
        normed_count = self._build_normed_count(visit_count)
        similarity = self._build_similarity(normed_count)
        if self.output_similarity_matrix:
            self.query = toSQL(similarity, dataset=samples_dataset)
            self.execute(self.file_manager.similarity_scores_dataset)
            similarity = self.file_manager.similarity_scores_dataset
        row_numbers = self._build_row_numbers(similarity)
        top_n = self._build_top_n(row_numbers)
        cf_scores = self._build_collaborative_filtering(top_n, normed_count)
        # final sql query

        self.query = toSQL(cf_scores, dataset=samples_dataset)
        print("self.query :\n", self.query)
