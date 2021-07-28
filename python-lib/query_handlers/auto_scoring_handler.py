from query_handlers import ScoringHandler
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
        normalization_factor = self._prepare_samples()
        similarity = self._build_similarity(normalization_factor)

        if self.output_similarity_matrix:
            logger.info("About to compute similarity matrix ...")
            self._execute(similarity, self.file_manager.similarity_scores_dataset)
            self._set_column_description(self.file_manager.similarity_scores_dataset, constants.SIMILARITY_COLUMN_NAME)
            similarity = self.file_manager.similarity_scores_dataset

        cf_scores = self._build_collaborative_filtering(similarity, normalization_factor)
        self._execute(cf_scores, self.file_manager.scored_samples_dataset)
        self._set_column_description(self.file_manager.scored_samples_dataset, constants.SCORE_COLUMN_NAME)

    def _get_column_descriptions(self, column_name):
        cf_based_on = "user" if self.is_user_based else "item"
        if column_name == constants.SCORE_COLUMN_NAME:
            description = f"User-item affinity scores (using {cf_based_on}-based collaborative filtering)"
        elif column_name == constants.SIMILARITY_COLUMN_NAME:
            description = f"Similarity between {cf_based_on}s (higher means more similar)"
        return {column_name: description}