from query_handlers import QueryHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2
import dku_constants as constants


class SamplingHandler(QueryHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.has_historical_data = bool(self.file_manager.historical_samples_dataset)
        self._set_negative_samples_generation_function()
        self._set_postfilter_function()

    def _set_negative_samples_generation_function(self):
        if self.dku_config.negative_samples_generation_mode == constants.NEGATIVE_SAMPLES_GENERATION_MODE.REMOVE_HISTORICAL_SAMPLES:
            self.negative_samples_generation_func = self._build_remove_historical_samples
        else:
            self.negative_samples_generation_func = self._build_remove_historical_samples

    def _set_postfilter_function(self):
        if self.dku_config.negative_samples_generation_mode == constants.SAMPLING_METHOD.NO_SAMPLING:
            self.postfiltering_func = self._build_not_sampled
        elif self.dku_config.negative_samples_generation_mode == constants.SAMPLING_METHOD.NEGATIVE_SAMPLING_PERC:
            self.postfiltering_func = self._build_filtered_with_perc
        else:
            self.postfiltering_func = self._build_not_sampled

    def _build_samples_for_training(self, select_from):
        samples_for_training = SelectQuery()
        samples_for_training.select_from(select_from, alias="samples_for_training")
        samples_for_training.select(Column(self.dku_config.users_column_name))
        samples_for_training.select(Column(self.dku_config.items_column_name))
        samples_for_training.select(Constant(1), alias="is_training_sample")
        return samples_for_training

    def _build_samples_for_scoring(self, select_from):
        samples_for_scores = SelectQuery()
        samples_for_scores.select_from(select_from, alias="samples_for_scores")
        samples_for_scores.select(Column(self.dku_config.users_column_name))
        samples_for_scores.select(Column(self.dku_config.items_column_name))
        samples_for_scores.select(Constant(1), alias="is_score_sample")
        return samples_for_scores

    def _build_all_cf_scores(self, select_from, samples_for_training, samples_for_scores=None):
        all_cf_scores = SelectQuery()
        all_cf_scores.select_from(select_from, alias="all_cf_scores")

        join_conditions_1 = [
            Column(self.dku_config.users_column_name, "samples_for_training_to_join")
            .eq_null_unsafe(Column(self.dku_config.users_column_name, "all_cf_scores")),
            Column(self.dku_config.items_column_name, "samples_for_training_to_join")
            .eq_null_unsafe(Column(self.dku_config.items_column_name, "all_cf_scores"))
        ]
        all_cf_scores.join(samples_for_training, JoinTypes.LEFT, join_conditions_1,
                           alias="samples_for_training_to_join")
        all_cf_scores.select(Column("is_training_sample", table_name="samples_for_training_to_join"))

        if samples_for_scores:
            join_conditions_2 = [
                Column(self.dku_config.users_column_name, "samples_for_scores_to_join")
                .eq_null_unsafe(Column(self.dku_config.users_column_name, "all_cf_scores")),
                Column(self.dku_config.items_column_name, "samples_for_scores_to_join")
                .eq_null_unsafe(Column(self.dku_config.items_column_name, "all_cf_scores"))
            ]
            all_cf_scores.join(samples_for_scores, JoinTypes.LEFT, join_conditions_2, alias="samples_for_scores_to_join")
            all_cf_scores.select(Column("is_score_sample", table_name="samples_for_scores_to_join"))

        all_cf_scores.select(Column(self.dku_config.users_column_name, table_name="all_cf_scores"))
        all_cf_scores.select(Column(self.dku_config.items_column_name, table_name="all_cf_scores"))

        for col in self.dku_config.score_columns_names:
            all_cf_scores.select(Column(col, table_name="all_cf_scores"))

        return all_cf_scores

    def _build_all_cf_scores_with_target(self, select_from):
        all_cf_scores_with_target = SelectQuery()
        all_cf_scores_with_target.select_from(select_from, alias="all_cf_scores_with_target")
        columns_to_select = [self.dku_config.users_column_name, self.dku_config.items_column_name] + self.dku_config.score_columns_names
        self._select_columns_list(
            select_query=all_cf_scores_with_target,
            column_names=columns_to_select)
        all_cf_scores_with_target.select(Column("is_training_sample").coalesce(0), alias="target")
        all_cf_scores_with_target.select(Column("is_score_sample").coalesce(0), alias="score_sample")
        return all_cf_scores_with_target

    def _build_remove_historical_samples(self, select_from):
        historical_negative_samples_removed = SelectQuery()
        historical_negative_samples_removed.select_from(select_from, alias="remove_negative_samples_seen")
        columns_to_select = [self.dku_config.users_column_name, self.dku_config.items_column_name, "target"] + self.dku_config.score_columns_names
        self._select_columns_list(
            select_query=historical_negative_samples_removed,
            column_names=columns_to_select)
        unseen_samples_condition = Column("target").eq(Constant(1)).or_(Column("score_sample").eq(Constant(0)))
        historical_negative_samples_removed.where(unseen_samples_condition)
        return historical_negative_samples_removed

    def _build_filtered_with_perc(self, select_from):
        return select_from

    def _build_not_sampled(self, select_from):
        return select_from

    def build(self):
        samples_for_training = self._build_samples_for_training(self.file_manager.training_samples_dataset)
        if self.has_historical_data:
            samples_for_scores = self._build_samples_for_scoring(self.file_manager.historical_samples_dataset)
        else:
            samples_for_scores = None

        all_cf_scores = _build_all_cf_scores(
            self.file_manager.scored_samples_dataset, samples_for_training, samples_for_scores)
        all_cf_scores_with_target = self._build_all_cf_scores_with_target(all_cf_scores)
        scores_with_negative_samples = self.negative_samples_generation_func(all_cf_scores_with_target)
        negative_samples_filtered = self.postfiltering_func(scores_with_negative_samples)
        self._execute(negative_samples_filtered, self.file_manager.positive_negative_samples_dataset)
