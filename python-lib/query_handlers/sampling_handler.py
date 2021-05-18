from query_handlers import QueryHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
import dku_constants as constants


class SamplingHandler(QueryHandler):
    IS_TRAINING_SAMPLE = "_is_training_sample"
    IS_SCORE_SAMPLE = "_is_score_sample"
    TARGET = "target"
    SCORE_SAMPLE = "score_sample"
    ROW_NUMBER_AS = "_row_number"
    NB_VISIT_USER_AS = "_nb_visit_user"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.has_historical_data = bool(self.file_manager.historical_samples_dataset)
        self.sample_keys = [self.dku_config.users_column_name, self.dku_config.items_column_name]
        self._set_negative_samples_generation_function()
        self._set_postfilter_function()

    def _set_negative_samples_generation_function(self):
        if self.has_historical_data:
            if (
                self.dku_config.negative_samples_generation_mode
                == constants.NEGATIVE_SAMPLES_GENERATION_MODE.REMOVE_HISTORICAL_SAMPLES
            ):
                self.negative_samples_generation_func = self._build_remove_historical_samples
            else:
                self.negative_samples_generation_func = self._build_remove_historical_samples
        else:
            self.negative_samples_generation_func = self._build_identity

    def _set_postfilter_function(self):
        if self.dku_config.sampling_method == constants.SAMPLING_METHOD.NO_SAMPLING:
            self.postfiltering_func = self._build_not_sampled
        elif self.dku_config.sampling_method == constants.SAMPLING_METHOD.NEGATIVE_SAMPLING_PERC:
            self.postfiltering_func = self._build_filtered_with_perc
        else:
            self.postfiltering_func = self._build_not_sampled

    def _build_samples_for_training(self, select_from, select_from_as="_samples_for_training"):
        samples_for_training = SelectQuery()
        samples_for_training.select_from(select_from, alias=select_from_as)
        self._select_columns_list(samples_for_training, self.sample_keys, table_name=select_from_as)
        samples_for_training.select(Constant(1), alias=self.IS_TRAINING_SAMPLE)
        return samples_for_training

    def _build_samples_for_scoring(self, select_from, select_from_as="_samples_for_scores"):
        samples_for_scores = SelectQuery()
        samples_for_scores.select_from(select_from, alias=select_from_as)
        self._select_columns_list(samples_for_scores, self.sample_keys, table_name=select_from_as)
        samples_for_scores.select(Constant(1), alias=self.IS_SCORE_SAMPLE)
        return samples_for_scores

    def _left_join_samples(self, left_select_query, left_table_name, right_select_query, right_table_name, keys):
        join_conditions = [Column(k, left_table_name).eq_null_unsafe(Column(k, right_table_name)) for k in keys]
        left_select_query.join(right_select_query, JoinTypes.LEFT, join_conditions, alias=right_table_name)

    def _build_all_cf_scores(
        self,
        select_from,
        samples_for_training,
        samples_for_scores=None,
        select_from_as="_all_cf_scores",
        samples_for_training_as="_samples_for_training",
        samples_for_scores_as="_samples_for_scores",
    ):
        all_cf_scores = SelectQuery()
        all_cf_scores.select_from(select_from, alias=select_from_as)

        self._left_join_samples(
            all_cf_scores, select_from_as, samples_for_training, samples_for_training_as, self.sample_keys
        )
        all_cf_scores.select(Column(self.IS_TRAINING_SAMPLE, table_name=samples_for_training_as))

        if samples_for_scores:
            self._left_join_samples(
                all_cf_scores, select_from_as, samples_for_scores, samples_for_scores_as, self.sample_keys
            )
            all_cf_scores.select(Column(self.IS_SCORE_SAMPLE, table_name=samples_for_scores_as))

        columns_to_select = self.sample_keys + self.dku_config.score_column_names
        self._select_columns_list(all_cf_scores, columns_to_select, table_name=select_from_as)
        return all_cf_scores

    def _build_all_cf_scores_with_target(self, select_from, select_from_as="_all_cf_scores_with_target"):
        all_cf_scores_with_target = SelectQuery()
        all_cf_scores_with_target.select_from(select_from, alias=select_from_as)
        columns_to_select = self.sample_keys + self.dku_config.score_column_names
        self._select_columns_list(select_query=all_cf_scores_with_target, column_names=columns_to_select)
        all_cf_scores_with_target.select(Column(self.IS_TRAINING_SAMPLE).coalesce(0).cast("int"), alias=self.TARGET)
        if self.has_historical_data:
            all_cf_scores_with_target.select(
                Column(self.IS_SCORE_SAMPLE).coalesce(0).cast("int"), alias=self.SCORE_SAMPLE
            )
        return all_cf_scores_with_target

    def _build_remove_historical_samples(self, select_from, select_from_as="_remove_negative_samples_seen"):
        historical_negative_samples_removed = SelectQuery()
        historical_negative_samples_removed.select_from(select_from, alias=select_from_as)
        columns_to_select = self.sample_keys + [self.TARGET] + self.dku_config.score_column_names
        self._select_columns_list(select_query=historical_negative_samples_removed, column_names=columns_to_select)
        unseen_samples_condition = Column(self.TARGET).eq(Constant(1)).or_(Column(self.SCORE_SAMPLE).eq(Constant(0)))
        historical_negative_samples_removed.where(unseen_samples_condition)
        return historical_negative_samples_removed

    def _build_cf_scores_without_null(self, select_from, select_from_as="_all_cf_scores_to_filter"):
        null_scores_filtered = SelectQuery()
        null_scores_filtered.select_from(select_from, alias=select_from_as)
        columns_to_select = self.sample_keys + self.dku_config.score_column_names
        self._select_columns_list(select_query=null_scores_filtered, column_names=columns_to_select)
        self._or_condition_columns_list(
            null_scores_filtered, self.dku_config.score_column_names, lambda x: x.is_not_null()
        )
        return null_scores_filtered

    def _build_filtered_with_perc(self, select_from, select_from_as="_filtered_samples"):
        NB_POSITIVE_PER_USER = "nb_positive_per_user"
        ONLY_POSITIVE_TABLE_NAME = "_only_positives"
        ALL_INFOS_TABLE_NAME = "_samples_with_all_infos"

        def _build_samples_with_only_positives(inner_select_from, inner_select_from_as=ONLY_POSITIVE_TABLE_NAME):
            samples_with_only_positives = SelectQuery()
            samples_with_only_positives.select_from(inner_select_from, inner_select_from_as)
            samples_with_only_positives.select(Column(self.dku_config.users_column_name))
            samples_with_only_positives.select(Column("*").count(), alias=NB_POSITIVE_PER_USER)
            samples_with_only_positives.where(Column(self.TARGET).eq(Constant(1)))
            samples_with_only_positives.group_by(Column(self.dku_config.users_column_name))
            return samples_with_only_positives

        def _build_samples_with_all_infos(inner_select_from, join_with, inner_select_from_as=ALL_INFOS_TABLE_NAME):
            samples_with_all_infos = SelectQuery()
            samples_with_all_infos.select_from(inner_select_from, alias=inner_select_from_as)

            columns_to_select = self.sample_keys + self.dku_config.score_column_names + [self.TARGET]
            self._select_columns_list(samples_with_all_infos, columns_to_select, table_name=inner_select_from_as)

            row_number_expression = (
                Expression()
                .rowNumber()
                .over(
                    Window(
                        partition_by=[
                            Column(self.dku_config.users_column_name, table_name=inner_select_from_as),
                            Column(self.TARGET, table_name=inner_select_from_as),
                        ],
                        order_by=[Column(self.TARGET, table_name=inner_select_from_as)],
                        order_types=["DESC"],
                    )
                )
            )
            samples_with_all_infos.select(row_number_expression, alias=self.ROW_NUMBER_AS)
            samples_with_all_infos.select(Column(NB_POSITIVE_PER_USER, table_name=ONLY_POSITIVE_TABLE_NAME))

            self._left_join_samples(
                left_select_query=samples_with_all_infos,
                left_table_name=inner_select_from_as,
                right_select_query=join_with,
                right_table_name=ONLY_POSITIVE_TABLE_NAME,
                keys=[self.dku_config.users_column_name],
            )
            return samples_with_all_infos

        def _build_filtered_samples(inner_select_from, inner_select_from_as):
            ratio = float(self.dku_config.negative_samples_percentage / 100.0)
            filtered_samples = SelectQuery()
            filtered_samples.select_from(inner_select_from, inner_select_from_as)
            columns_to_select = self.sample_keys + self.dku_config.score_column_names + [self.TARGET]
            self._select_columns_list(filtered_samples, columns_to_select)

            nb_negative_threshold_expr = (
                Column(NB_POSITIVE_PER_USER, table_name=select_from_as)
                .times(Constant(ratio))
                .div(Constant(1).minus(Constant(ratio)))
                .ceil()
            )
            filtered_samples.where(
                Column(self.TARGET, table_name=select_from_as)
                .eq(Constant(1))
                .or_(Column(self.ROW_NUMBER_AS, table_name=select_from_as).le(nb_negative_threshold_expr))
            )
            return filtered_samples

        samples_with_only_positives = _build_samples_with_only_positives(select_from)
        samples_with_all_infos = _build_samples_with_all_infos(select_from, samples_with_only_positives)
        filtered_samples = _build_filtered_samples(samples_with_all_infos, select_from_as)

        return filtered_samples

    def _build_not_sampled(self, select_from):
        return self._build_identity(select_from)

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
            alias="_training_samples",
        )

        samples_for_training = self._build_samples_for_training(prepared_training_samples)

        if self.has_historical_data:
            prepared_historical_samples = self._prepare_samples(
                query_to_prepare=self.file_manager.historical_samples_dataset,
                users_col_name=self.dku_config.historical_samples_users_column_name,
                items_col_name=self.dku_config.historical_samples_items_column_name,
                cast_mapping=cast_mapping,
                alias="_historical_samples",
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
