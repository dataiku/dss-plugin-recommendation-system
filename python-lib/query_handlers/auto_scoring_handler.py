from query_handlers import ScoringHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2
import dku_constants as constants
import logging

logger = logging.getLogger(__name__)


class AutoScoringHandler(ScoringHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_user_based = self.dku_config.collaborative_filtering_method == constants.CF_METHOD.USER_BASED
        self._assign_scoring_mode(self.is_user_based)
        self.output_similarity_matrix = self.file_manager.similarity_scores_dataset is not None

    def build(self):
        normed_count = self._prepare_samples()
        similarity = self._build_similarity(normed_count)

        if self.output_similarity_matrix:
            self._execute(similarity, self.file_manager.similarity_scores_dataset)
            similarity = self.file_manager.similarity_scores_dataset

        cf_scores = self._build_collaborative_filtering(similarity, normed_count)
        self._execute(cf_scores, self.file_manager.scored_samples_dataset)