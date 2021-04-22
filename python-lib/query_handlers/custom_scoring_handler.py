from query_handlers import ScoringHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2
import dku_constants as constants
import logging

logger = logging.getLogger(__name__)


class CustomScoringHandler(ScoringHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_based = (
            True if self.dku_config.similarity_scores_type == constants.SIMILARITY_TYPE.USER_SIMILARITY.value else False
        )
        # TODO put this in the parent class ?
        if self.user_based:
            self.based_column = self.dku_config.users_column_name
            self.pivot_column = self.dku_config.items_column_name
        else:
            self.based_column = self.dku_config.items_column_name
            self.pivot_column = self.dku_config.users_column_name

    def _prepare_similarity_input(self):
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
        return similarity_cast

    def build(self):
        normed_count = self._prepare_samples()
        similarity = self._prepare_similarity_input()
        cf_scores = self._build_collaborative_filtering(similarity, normed_count)
        self._execute(cf_scores, self.file_manager.scored_samples_dataset)