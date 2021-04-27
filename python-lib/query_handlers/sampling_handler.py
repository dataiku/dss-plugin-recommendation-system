from query_handlers import QueryHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2
import dku_constants as constants


class SamplingHandler(QueryHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.has_historical_data = bool(self.file_manager.historical_samples_dataset)
        self.sample_keys = [self.dku_config.users_column_name, self.dku_config.items_column_name]
        self._set_negative_samples_generation_function()
        self._set_postfilter_function()

    def _set_negative_samples_generation_function(self):
        if (
            self.dku_config.negative_samples_generation_mode
            == constants.NEGATIVE_SAMPLES_GENERATION_MODE.REMOVE_HISTORICAL_SAMPLES
        ):
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
        self._select_columns_list(samples_for_training, self.sample_keys, table_name="samples_for_training")
        samples_for_training.select(Constant(1), alias="is_training_sample")
        return samples_for_training

    def _build_samples_for_scoring(self, select_from):
        samples_for_scores = SelectQuery()
        samples_for_scores.select_from(select_from, alias="samples_for_scores")
        self._select_columns_list(samples_for_scores, self.sample_keys, table_name="samples_for_scores")
        samples_for_scores.select(Constant(1), alias="is_score_sample")
        return samples_for_scores

    def _left_join_samples(self, left_select_query, left_table_name, right_select_query, right_table_name):
        join_conditions = [Column(k, left_table_name).eq_null_unsafe(Column(k, right_table_name)) for k in self.sample_keys]
        left_select_query.join(right_select_query, JoinTypes.LEFT, join_conditions, alias=right_table_name)

    def _build_all_cf_scores(self, select_from, samples_for_training, samples_for_scores=None):
        all_cf_scores = SelectQuery()
        all_cf_scores.select_from(select_from, alias="all_cf_scores")

        self._left_join_samples(all_cf_scores, "all_cf_scores", samples_for_training, "samples_for_training_to_join")
        all_cf_scores.select(Column("is_training_sample", table_name="samples_for_training_to_join"))

        if samples_for_scores:
            self._left_join_samples(all_cf_scores, "all_cf_scores", samples_for_scores, "samples_for_scores_to_join")
            all_cf_scores.select(Column("is_score_sample", table_name="samples_for_scores_to_join"))

        columns_to_select = self.sample_keys + self.dku_config.score_column_names
        self._select_columns_list(all_cf_scores, columns_to_select, table_name="all_cf_scores")
        return all_cf_scores

    def _build_all_cf_scores_with_target(self, select_from):
        all_cf_scores_with_target = SelectQuery()
        all_cf_scores_with_target.select_from(select_from, alias="all_cf_scores_with_target")
        columns_to_select = self.sample_keys + self.dku_config.score_column_names
        self._select_columns_list(select_query=all_cf_scores_with_target, column_names=columns_to_select)
        all_cf_scores_with_target.select(Column("is_training_sample").coalesce(0).cast("int"), alias="target")
        all_cf_scores_with_target.select(Column("is_score_sample").coalesce(0).cast("int"), alias="score_sample")
        return all_cf_scores_with_target

    def _build_remove_historical_samples(self, select_from):
        historical_negative_samples_removed = SelectQuery()
        historical_negative_samples_removed.select_from(select_from, alias="remove_negative_samples_seen")
        columns_to_select = self.sample_keys + ["target"] + self.dku_config.score_column_names
        self._select_columns_list(select_query=historical_negative_samples_removed, column_names=columns_to_select)
        unseen_samples_condition = Column("target").eq(Constant(1)).or_(Column("score_sample").eq(Constant(0)))
        historical_negative_samples_removed.where(unseen_samples_condition)
        return historical_negative_samples_removed

    def _build_cf_scores_without_null(self, select_from):
        null_scores_filtered = SelectQuery()
        null_scores_filtered.select_from(select_from, alias="all_cf_scores_to_filter")
        columns_to_select = self.sample_keys + self.dku_config.score_column_names
        self._select_columns_list(select_query=null_scores_filtered, column_names=columns_to_select)
        self._or_condition_columns_list(
            null_scores_filtered, self.dku_config.score_column_names, lambda x: x.is_not_null()
        )
        return null_scores_filtered

    def _build_filtered_with_perc(self, select_from):
        return select_from

    def _build_not_sampled(self, select_from):
        return select_from

    def _prepare_samples(self, query_to_prepare, users_col_name, items_col_name, cast_mapping, alias):
        renaming_mapping = {
            users_col_name: self.dku_config.users_column_name,
            items_col_name: self.dku_config.items_column_name,
        }
        renamed_samples = self._rename_table(query_to_prepare, renaming_mapping)
        cast_samples = self._cast_table(renamed_samples, cast_mapping, alias=alias)
        return cast_samples

    def build(self):
        cast_mapping = {self.dku_config.users_column_name: "string", self.dku_config.items_column_name: "string"}
        prepared_training_samples = self._prepare_samples(
            query_to_prepare=self.file_manager.training_samples_dataset,
            users_col_name=self.dku_config.training_samples_users_column_name,
            items_col_name=self.dku_config.training_samples_items_column_name,
            cast_mapping=cast_mapping,
            alias="_training_samples"
        )

        samples_for_training = self._build_samples_for_training(prepared_training_samples)

        if self.has_historical_data:
            prepared_historical_samples = self._prepare_samples(
                query_to_prepare=self.file_manager.historical_samples_dataset,
                users_col_name=self.dku_config.historical_samples_users_column_name,
                items_col_name=self.dku_config.historical_samples_items_column_name,
                cast_mapping=cast_mapping,
                alias="_historical_samples"
            )
            samples_for_scores = self._build_samples_for_scoring(prepared_historical_samples)
        else:
            samples_for_scores = None

        cast_mapping.update({col: "double" for col in self.dku_config.score_column_names})
        scored_samples_cast = self._cast_table(
            self.file_manager.scored_samples_dataset, cast_mapping, alias="_scored_samples"
        )

        null_scores_filtered = self._build_cf_scores_without_null(scored_samples_cast)

        all_cf_scores = self._build_all_cf_scores(null_scores_filtered, samples_for_training, samples_for_scores)
        all_cf_scores_with_target = self._build_all_cf_scores_with_target(all_cf_scores)
        scores_with_negative_samples = self.negative_samples_generation_func(all_cf_scores_with_target)
        negative_samples_filtered = self.postfiltering_func(scores_with_negative_samples)

        self._execute(negative_samples_filtered, self.file_manager.positive_negative_samples_dataset)
