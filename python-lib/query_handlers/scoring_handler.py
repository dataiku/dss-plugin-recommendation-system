from query_handlers import QueryHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, SelectQuery, Window
import dku_constants as constants
import logging

logger = logging.getLogger(__name__)


class ScoringHandler(QueryHandler):
    # VISIT_COUNT_TABLE_ALIAS = "visit_count"
    NB_VISIT_USER_AS = "_nb_visit_user"
    NB_VISIT_ITEM_AS = "_nb_visit_item"
    RATING_AVERAGE = "_rating_average"
    NORMALIZATION_FACTOR_AS = "_normalization_factor"
    LEFT_NORMALIZATION_FACTOR_AS = "_left_normalization_factor"
    RIGHT_NORMALIZATION_FACTOR_AS = "_right_normalization_factor"
    ROW_NUMBER_AS = "_row_number"
    TIMESTAMP_FILTERED_ROW_NB = "_timestamp_filtered_row_nb"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sample_keys = [self.dku_config.users_column_name, self.dku_config.items_column_name]
        self.similarity_computation_columns = self.sample_keys.copy()  # columns to keep to compute similarity
        self.filtering_columns = []  # columns to keep for filtering
        self.use_explicit = bool(self.dku_config.ratings_column_name)
        self.timestamp_filtering = bool(self.dku_config.timestamp_filtering and self.dku_config.timestamps_column_name)

        if self.use_explicit:
            logger.debug("Using explicit feedbacks")
            self.similarity_computation_columns += [self.dku_config.ratings_column_name]

        if self.timestamp_filtering:
            logger.debug("Using timestamp filtering")
            self.filtering_columns += [self.dku_config.timestamps_column_name]

    def _build_timestamp_filtered(self, select_from, select_from_as="_prepared_input_dataset"):
        def _build_timestamp_filtered_row_number(select_from_inner, select_from_as_inner):
            ts_row_numbers = SelectQuery()
            ts_row_numbers.select_from(select_from_inner, alias=select_from_as_inner)

            self._select_columns_list(
                ts_row_numbers, column_names=self.similarity_computation_columns, table_name=select_from_as_inner
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
                        mode=None,
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
            timestamp_filtered, column_names=self.similarity_computation_columns, table_name=ts_row_numbers_alias
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

        self._select_columns_list(
            visit_count,
            column_names=self.similarity_computation_columns + self.filtering_columns,
            table_name=select_from_as,
        )

        visit_count.select(
            Column("*")
            .count()
            .over(
                Window(
                    partition_by=[Column(self.dku_config.users_column_name)],
                    mode=None,
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
                    mode=None,
                )
            ),
            alias=self.NB_VISIT_ITEM_AS,
        )
        if self.use_explicit:
            visit_count.select(
                Column(self.dku_config.ratings_column_name)
                .avg()
                .over(Window(partition_by=[Column(self.based_column)])),
                alias=self.RATING_AVERAGE,
            )
            self.similarity_computation_columns += [self.RATING_AVERAGE]

        return visit_count

    def _build_normalization_factor(self, select_from, select_from_as="_visit_count"):
        # compute normalization factor
        normalization_factor = SelectQuery()
        normalization_factor.select_from(select_from, alias=select_from_as)

        self._select_columns_list(
            normalization_factor,
            column_names=self.similarity_computation_columns + self.filtering_columns,
            table_name=select_from_as,
        )

        rating_column = (
            Column(self.dku_config.ratings_column_name).minus(Column(self.RATING_AVERAGE))
            if self.use_explicit
            else Constant(1)
        )

        normalization_factor.select(
            self._get_normalization_factor_formula(Column(self.based_column), rating_column),
            alias=self.NORMALIZATION_FACTOR_AS,
        )

        self.similarity_computation_columns += [self.NORMALIZATION_FACTOR_AS]

        # keep only items and users with enough visits
        normalization_factor.where(
            Column(self.NB_VISIT_USER_AS, table_name=select_from_as).ge(Constant(self.dku_config.user_visit_threshold))
        )
        normalization_factor.where(
            Column(self.NB_VISIT_ITEM_AS, table_name=select_from_as).ge(Constant(self.dku_config.item_visit_threshold))
        )
        return normalization_factor

    def _build_similarity(self, select_from):
        similarity = self._build_ordered_similarity(select_from)
        if self.supports_full_outer_join:
            return self._build_unordered_similarity(similarity)
        else:
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

        if self.supports_with_clause:
            similarity.with_cte(select_from, alias=with_clause_as)
            select_from = with_clause_as

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

    def _build_ordered_similarity(self, select_from, with_clause_as="_with_clause_normalization_factor"):
        """Build a similarity table col_1, col_2, similarity where col_1 < col_2 """
        similarity = SelectQuery()

        if self.supports_with_clause:
            similarity.with_cte(select_from, alias=with_clause_as)
            select_from = with_clause_as

        similarity.select_from(select_from, alias=self.LEFT_NORMALIZATION_FACTOR_AS)

        join_conditions = [
            Column(self.pivot_column, self.LEFT_NORMALIZATION_FACTOR_AS).eq_null_unsafe(
                Column(self.pivot_column, self.RIGHT_NORMALIZATION_FACTOR_AS)
            )
        ]

        if self.supports_full_outer_join:
            join_conditions += [
                Column(self.based_column, self.LEFT_NORMALIZATION_FACTOR_AS).lt(
                    Column(self.based_column, self.RIGHT_NORMALIZATION_FACTOR_AS)
                )
            ]
        else:
            join_conditions += [
                Column(self.based_column, self.LEFT_NORMALIZATION_FACTOR_AS).ne(
                    Column(self.based_column, self.RIGHT_NORMALIZATION_FACTOR_AS)
                )
            ]

        similarity.join(select_from, JoinTypes.INNER, join_conditions, alias=self.RIGHT_NORMALIZATION_FACTOR_AS)

        similarity.group_by(Column(self.based_column, table_name=self.LEFT_NORMALIZATION_FACTOR_AS))
        similarity.group_by(Column(self.based_column, table_name=self.RIGHT_NORMALIZATION_FACTOR_AS))

        similarity.select(
            Column(self.based_column, table_name=self.LEFT_NORMALIZATION_FACTOR_AS), alias=f"{self.based_column}_1"
        )
        similarity.select(
            Column(self.based_column, table_name=self.RIGHT_NORMALIZATION_FACTOR_AS), alias=f"{self.based_column}_2"
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
                    mode=None,
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

    def _build_sum_of_similarity_scores(
        self, top_n, normalization_factor, top_n_as="_top_n", normalization_factor_as="_normalization_factor"
    ):
        cf_scores = SelectQuery()
        cf_scores.select_from(top_n, alias=top_n_as)

        join_condition = Column(f"{self.based_column}_2", top_n_as).eq_null_unsafe(
            Column(self.based_column, normalization_factor_as)
        )
        cf_scores.join(normalization_factor, JoinTypes.INNER, join_condition, alias=normalization_factor_as)

        cf_scores.group_by(Column(f"{self.based_column}_1", table_name=top_n_as))
        cf_scores.group_by(Column(self.pivot_column, table_name=normalization_factor_as))

        cf_scores.select(Column(f"{self.based_column}_1", table_name=top_n_as), alias=self.based_column)
        cf_scores.select(Column(self.pivot_column, table_name=normalization_factor_as))

        cf_scores.select(
            self._get_user_item_similarity_formula(top_n_as, normalization_factor_as), alias=constants.SCORE_COLUMN_NAME
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
        samples_cast = self._cast_table(samples_dataset, cast_mapping, alias="_raw_input_dataset")
        visit_count = self._build_visit_count(samples_cast)
        normalization_factor = self._build_normalization_factor(visit_count)
        if self.timestamp_filtering:
            timestamp_filtered = self._build_timestamp_filtered(normalization_factor)
        else:
            timestamp_filtered = normalization_factor
        return timestamp_filtered

    def _build_collaborative_filtering(self, similarity, normalization_factor):
        row_numbers = self._build_row_numbers(similarity)
        top_n = self._build_top_n(row_numbers)
        cf_scores = self._build_sum_of_similarity_scores(top_n, normalization_factor)
        return cf_scores

    def _get_normalization_factor_formula(self, partition_column, rating_column):
        logger.debug("Using L2 normalization")
        return Constant(1).div(
            rating_column.times(rating_column).sum().over(Window(partition_by=[partition_column])).sqrt()
        )

    def _get_similarity_formula(self):
        rounding_decimals = 15
        rounding_expression = Constant(10 ** rounding_decimals)
        logger.debug(f"Rounding similarity to {rounding_decimals} decimals")

        if self.use_explicit:
            # compute Pearson correlation
            rating_product = (
                Column(self.dku_config.ratings_column_name, table_name=self.LEFT_NORMALIZATION_FACTOR_AS)
                .minus(Column(self.RATING_AVERAGE, table_name=self.LEFT_NORMALIZATION_FACTOR_AS))
                .times(
                    Column(self.dku_config.ratings_column_name, table_name=self.RIGHT_NORMALIZATION_FACTOR_AS).minus(
                        Column(self.RATING_AVERAGE, table_name=self.RIGHT_NORMALIZATION_FACTOR_AS)
                    )
                )
            )
        else:
            rating_product = Constant(1)

        return (
            rating_product.times(Column(self.NORMALIZATION_FACTOR_AS, table_name=self.LEFT_NORMALIZATION_FACTOR_AS))
            .times(Column(self.NORMALIZATION_FACTOR_AS, table_name=self.RIGHT_NORMALIZATION_FACTOR_AS))
            .sum()
            .times(rounding_expression)
            .round()
            .div(rounding_expression)
        )

    def _get_user_item_similarity_formula(self, similarity_table, samples_table):
        if self.use_explicit:
            return (
                Column(constants.SIMILARITY_COLUMN_NAME, table_name=similarity_table)
                .times(
                    Column(self.dku_config.ratings_column_name, table_name=samples_table).minus(
                        Column(self.RATING_AVERAGE, table_name=samples_table)
                    )
                )
                .sum()
                .div(Column(constants.SIMILARITY_COLUMN_NAME, table_name=similarity_table).abs().sum())
            )
        else:
            return (
                Column(constants.SIMILARITY_COLUMN_NAME, table_name=similarity_table)
                .sum()
                .div(Constant(self.dku_config.top_n_most_similar))
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
