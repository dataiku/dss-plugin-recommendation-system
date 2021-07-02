from query_handlers import QueryHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2
import dku_constants as constants
import logging

logger = logging.getLogger(__name__)


class ScoringHandler(QueryHandler):
    # VISIT_COUNT_TABLE_ALIAS = "visit_count"
    NB_VISIT_USER_AS = "_nb_visit_user"
    NB_VISIT_ITEM_AS = "_nb_visit_item"
    VISIT_USER_NORMED_AS = "_visit_user_normed"
    VISIT_ITEM_NORMED_AS = "_visit_item_normed"
    LEFT_NORMED_COUNT_AS = "_left_normed_count"
    RIGHT_NORMED_COUNT_AS = "_right_normed_count"
    ROW_NUMBER_AS = "_row_number"
    TIMESTAMP_FILTERED_ROW_NB = "_timestamp_filtered_row_nb"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sample_keys = [self.dku_config.users_column_name, self.dku_config.items_column_name]
        self.precomputation_columns = self.sample_keys.copy()
        self.use_explicit = bool(self.dku_config.ratings_column_name)
        self.timestamp_filtering = bool(self.dku_config.timestamp_filtering and self.dku_config.timestamps_column_name)
        if self.use_explicit:
            logger.debug("Using explicit feedbacks")
            self.precomputation_columns += [self.dku_config.ratings_column_name]
        if self.timestamp_filtering:
            logger.debug("Using timestamp filtering")
            self.precomputation_columns += [self.dku_config.timestamps_column_name]

    def _build_timestamp_filtered(self, select_from, select_from_as="_prepared_input_dataset"):
        def _build_timestamp_filtered_row_number(select_from_inner, select_from_as_inner):
            ts_row_numbers = SelectQuery()
            ts_row_numbers.select_from(select_from_inner, alias=select_from_as_inner)

            self._select_columns_list(
                ts_row_numbers, column_names=self.precomputation_columns, table_name=select_from_as_inner
            )

            ts_row_number_expression = (
                Expression()
                .rowNumber()
                .over(
                    Window(
                        partition_by=[Column(self.based_column, table_name=select_from_as_inner)],
                        order_by=[
                            Column(self.dku_config.timestamps_column_name, table_name=select_from_as_inner),
                            Column(self.pivot_column, table_name=select_from_as_inner),
                        ],
                        order_types=["DESC", "DESC"],
                    )
                )
            )
            ts_row_numbers.select(ts_row_number_expression, alias=self.TIMESTAMP_FILTERED_ROW_NB)
            return ts_row_numbers

        built_ts_row_numbers = _build_timestamp_filtered_row_number(select_from, select_from_as)

        ts_row_numbers_alias = "_ts_row_numbers"
        timestamp_filtered = SelectQuery()
        timestamp_filtered.select_from(built_ts_row_numbers, alias=ts_row_numbers_alias)

        self._select_columns_list(
            timestamp_filtered, column_names=self.precomputation_columns, table_name=ts_row_numbers_alias
        )

        timestamp_filtered.where(
            Column(self.TIMESTAMP_FILTERED_ROW_NB, table_name=ts_row_numbers_alias).le(
                Constant(self.dku_config.top_n_most_recent)
            )
        )

        return timestamp_filtered

    def _build_visit_count(self, select_from, select_from_as="_filtered_input_dataset"):
        # total user and item visits
        visit_count = SelectQuery()
        visit_count.select_from(select_from, alias=select_from_as)

        columns_to_select = self.precomputation_columns
        self._select_columns_list(visit_count, column_names=columns_to_select, table_name=select_from_as)

        visit_count.select(
            Column("*")
            .count()
            .over(
                Window(
                    partition_by=[Column(self.dku_config.users_column_name)],
                    order_by=[Column(self.dku_config.items_column_name)],
                    order_types=["ASC"],
                )
            ),
            alias=self.NB_VISIT_USER_AS,
        )
        visit_count.select(
            Column("*")
            .count()
            .over(
                Window(
                    partition_by=[Column(self.dku_config.items_column_name)],
                    order_by=[Column(self.dku_config.users_column_name)],
                    order_types=["ASC"],
                )
            ),
            alias=self.NB_VISIT_ITEM_AS,
        )
        return visit_count

    def _build_normed_count(self, select_from, select_from_as="_visit_count"):
        # normalise total visits
        normed_count = SelectQuery()
        normed_count.select_from(select_from, alias=select_from_as)

        self._select_columns_list(normed_count, column_names=self.precomputation_columns, table_name=select_from_as)

        rating_column = (
            Column(self.dku_config.ratings_column_name) if self.dku_config.ratings_column_name else Constant(1)
        )

        normed_count.select(
            self._get_visit_normalization(Column(self.dku_config.users_column_name), rating_column),
            alias=self.VISIT_USER_NORMED_AS,
        )
        normed_count.select(
            self._get_visit_normalization(Column(self.dku_config.items_column_name), rating_column),
            alias=self.VISIT_ITEM_NORMED_AS,
        )

        self.precomputation_columns = self.sample_keys.copy() + [self.VISIT_USER_NORMED_AS, self.VISIT_ITEM_NORMED_AS]

        # keep only items and users with enough visits
        normed_count.where(
            Column(self.NB_VISIT_USER_AS, table_name=select_from_as).ge(Constant(self.dku_config.user_visit_threshold))
        )
        normed_count.where(
            Column(self.NB_VISIT_ITEM_AS, table_name=select_from_as).ge(Constant(self.dku_config.item_visit_threshold))
        )
        return normed_count

    def _build_similarity(self, select_from):
        can_full_outer_join = False
        if can_full_outer_join:
            ordered_similarity = self._build_ordered_similarity(select_from, can_full_outer_join=can_full_outer_join)
            similarity = self._build_unordered_similarity(ordered_similarity)
        else:
            similarity = self._build_ordered_similarity(select_from, can_full_outer_join=can_full_outer_join)
        return similarity

    def _build_unordered_similarity(
        self,
        select_from,
        left_select_from_as="_left_ordered_similarity",
        right_select_from_as="_right_ordered_similarity",
        with_clause_as="_with_clause_ordered_similarity",
    ):
        """Retrieve both pairs (when col_1 < col_2 and col_1 > col_2) from the ordered similarity table"""
        similarity = SelectQuery()
        # similarity.with_cte(select_from, alias=with_clause_as)
        similarity.select_from(select_from, alias=left_select_from_as)
        join_condition = Constant(1).eq_null_unsafe(Constant(0))

        similarity.join(select_from, JoinTypes.FULL, join_condition, alias=right_select_from_as)

        similarity.select(
            Column(f"{self.based_column}_1", table_name=left_select_from_as).coalesce(
                Column(f"{self.based_column}_2", table_name=right_select_from_as)
            ),
            alias=f"{self.based_column}_1",
        )
        similarity.select(
            Column(f"{self.based_column}_2", table_name=left_select_from_as).coalesce(
                Column(f"{self.based_column}_1", table_name=right_select_from_as)
            ),
            alias=f"{self.based_column}_2",
        )
        similarity.select(
            Column("similarity", table_name=left_select_from_as).coalesce(
                Column("similarity", table_name=right_select_from_as)
            ),
            alias=constants.SIMILARITY_COLUMN_NAME,
        )

        return similarity

    def _build_ordered_similarity(
        self, select_from, with_clause_as="_with_clause_normed_count", can_full_outer_join=False
    ):
        """Build a similarity table col_1, col_2, similarity where col_1 < col_2 """
        similarity = SelectQuery()
        # similarity.with_cte(select_from, alias=with_clause_as)
        similarity.select_from(select_from, alias=self.LEFT_NORMED_COUNT_AS)

        join_conditions = [
            Column(self.pivot_column, self.LEFT_NORMED_COUNT_AS).eq_null_unsafe(
                Column(self.pivot_column, self.RIGHT_NORMED_COUNT_AS)
            )
        ]

        if can_full_outer_join:
            join_conditions += [
                Column(self.based_column, self.LEFT_NORMED_COUNT_AS).lt(
                    Column(self.based_column, self.RIGHT_NORMED_COUNT_AS)
                )
            ]
        else:
            join_conditions += [
                Column(self.based_column, self.LEFT_NORMED_COUNT_AS).ne(
                    Column(self.based_column, self.RIGHT_NORMED_COUNT_AS)
                )
            ]

        similarity.join(select_from, JoinTypes.INNER, join_conditions, alias=self.RIGHT_NORMED_COUNT_AS)

        similarity.group_by(Column(self.based_column, table_name=self.LEFT_NORMED_COUNT_AS))
        similarity.group_by(Column(self.based_column, table_name=self.RIGHT_NORMED_COUNT_AS))

        similarity.select(
            Column(self.based_column, table_name=self.LEFT_NORMED_COUNT_AS), alias=f"{self.based_column}_1"
        )
        similarity.select(
            Column(self.based_column, table_name=self.RIGHT_NORMED_COUNT_AS), alias=f"{self.based_column}_2"
        )

        similarity.select(self._get_similarity_formula(), alias=constants.SIMILARITY_COLUMN_NAME)
        return similarity

    def _build_row_numbers(self, select_from, select_from_as="_similarity_matrix"):
        row_numbers = SelectQuery()
        row_numbers.select_from(select_from, alias=select_from_as)

        columns_to_select = [f"{self.based_column}_1", f"{self.based_column}_2", constants.SIMILARITY_COLUMN_NAME]
        self._select_columns_list(row_numbers, column_names=columns_to_select, table_name=select_from_as)

        row_number_expression = (
            Expression()
            .rowNumber()
            .over(
                Window(
                    partition_by=[Column(f"{self.based_column}_1", table_name=select_from_as)],
                    order_by=[
                        Column(constants.SIMILARITY_COLUMN_NAME, table_name=select_from_as),
                        Column(f"{self.based_column}_2", table_name=select_from_as),
                    ],
                    order_types=["DESC", "DESC"],
                )
            )
        )
        row_numbers.select(row_number_expression, alias=self.ROW_NUMBER_AS)
        return row_numbers

    def _build_top_n(self, select_from, select_from_as="_row_number_table"):
        top_n = SelectQuery()
        top_n.select_from(select_from, alias=select_from_as)

        columns_to_select = [f"{self.based_column}_1", f"{self.based_column}_2", constants.SIMILARITY_COLUMN_NAME]
        self._select_columns_list(top_n, column_names=columns_to_select, table_name=select_from_as)

        top_n.where(
            Column(self.ROW_NUMBER_AS, table_name=select_from_as).le(Constant(self.dku_config.top_n_most_similar))
        )
        return top_n

    def _build_sum_of_similarity_scores(self, top_n, normed_count, top_n_as="_top_n", normed_count_as="_normed_count"):
        cf_scores = SelectQuery()
        cf_scores.select_from(top_n, alias=top_n_as)

        join_condition = Column(f"{self.based_column}_2", top_n_as).eq_null_unsafe(
            Column(self.based_column, normed_count_as)
        )
        cf_scores.join(normed_count, JoinTypes.INNER, join_condition, alias=normed_count_as)

        cf_scores.group_by(Column(f"{self.based_column}_1", table_name=top_n_as))
        cf_scores.group_by(Column(self.pivot_column, table_name=normed_count_as))

        cf_scores.select(Column(f"{self.based_column}_1", table_name=top_n_as), alias=self.based_column)
        cf_scores.select(Column(self.pivot_column, table_name=normed_count_as))

        cf_scores.select(
            Column(constants.SIMILARITY_COLUMN_NAME, table_name=top_n_as).sum(), alias=constants.SCORE_COLUMN_NAME
        )

        cf_scores.order_by(Column(self.based_column))
        cf_scores.order_by(Column(constants.SCORE_COLUMN_NAME), direction="DESC")
        return cf_scores

    def _prepare_samples(self):
        samples_dataset = self.file_manager.samples_dataset
        cast_mapping = {self.dku_config.users_column_name: "string", self.dku_config.items_column_name: "string"}
        if self.use_explicit:
            cast_mapping[self.dku_config.ratings_column_name] = "double"
        if self.timestamp_filtering:
            cast_mapping[self.dku_config.timestamps_column_name] = self._get_cast_type(
                self.dku_config.timestamps_column_name, samples_dataset
            )
        # samples_cast = self._cast_table(samples_dataset, cast_mapping, alias="_raw_input_dataset")
        # visit_count = self._build_visit_count(samples_cast)
        visit_count = self._build_visit_count(samples_dataset)
        normed_count = self._build_normed_count(visit_count)
        if self.timestamp_filtering:
            timestamp_filtered = self._build_timestamp_filtered(normed_count)
        else:
            timestamp_filtered = normed_count
        return timestamp_filtered

    def _build_collaborative_filtering(self, similarity, normed_count):
        row_numbers = self._build_row_numbers(similarity)
        top_n = self._build_top_n(row_numbers)
        cf_scores = self._build_sum_of_similarity_scores(top_n, normed_count)
        return cf_scores

    def _get_visit_normalization(self, column_to_norm, rating_column):
        if self.dku_config.normalization_method == constants.NORMALIZATION_METHOD.L1:
            logger.debug("Using L1 normalization")
            return rating_column.div(
                rating_column.abs()
                .sum()
                .over(
                    Window(
                        partition_by=[column_to_norm],
                        order_by=[column_to_norm],
                        order_types=["ASC"],
                    )
                )
            )
        elif self.dku_config.normalization_method == constants.NORMALIZATION_METHOD.L2:
            logger.debug("Using L2 normalization")
            return rating_column.div(
                rating_column.times(rating_column)
                .sum()
                .over(
                    Window(
                        partition_by=[column_to_norm],
                        order_by=[column_to_norm],
                        order_types=["ASC"],
                    )
                )
                .sqrt()
            )

    def _get_similarity_formula(self):
        rounding_decimals = 15
        column_to_sum = self.VISIT_USER_NORMED_AS if self.is_user_based else self.VISIT_ITEM_NORMED_AS
        rounding_expression = Constant(10 ** rounding_decimals)
        logger.debug(f"Rounding similarity to {rounding_decimals} decimals")
        return (
            Column(column_to_sum, table_name=self.LEFT_NORMED_COUNT_AS)
            .times(Column(column_to_sum, table_name=self.RIGHT_NORMED_COUNT_AS))
            .sum()
            .times(rounding_expression)
            .round()
            .div(rounding_expression)
        )

    def _assign_scoring_mode(self, is_user_based):
        if is_user_based:
            logger.debug("Using user-based collaborative filtering")
            self.based_column = self.dku_config.users_column_name
            self.pivot_column = self.dku_config.items_column_name
        else:
            logger.debug("Using item-based collaborative filtering")
            self.based_column = self.dku_config.items_column_name
            self.pivot_column = self.dku_config.users_column_name
