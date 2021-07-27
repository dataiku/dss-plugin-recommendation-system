from query_handlers import ScoringHandler
import dku_constants as constants
import logging

logger = logging.getLogger(__name__)


class CustomScoringHandler(ScoringHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_user_based = self.dku_config.similarity_scores_type == constants.SIMILARITY_TYPE.USER_SIMILARITY
        self._assign_scoring_mode(self.is_user_based)

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
        similarity_cast = self._cast_table(similarity_renamed, cast_mapping, alias="_similarity_matrix_renamed")
        return similarity_cast

    def build(self):
        normalization_factor = self._prepare_samples()
        similarity = self._prepare_similarity_input()
        cf_scores = self._build_collaborative_filtering(similarity, normalization_factor)
        self._execute(cf_scores, self.file_manager.scored_samples_dataset)
        self._set_column_description(self.file_manager.scored_samples_dataset)

    def _get_column_descriptions(self, column_name=None):
        column_name = constants.SCORE_COLUMN_NAME
        cf_based_on = "user" if self.is_user_based else "item"
        description = (
            f"User-item affinity scores (using {cf_based_on}-based collaborative filtering with custom similarity)"
        )
        return {column_name: description}