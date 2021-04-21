from query_handlers import QueryHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2
import dku_constants as constants


class ScoringHandler(QueryHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _build_visit_count(self, select_from):
        # total user and item visits
        visit_count = SelectQuery()
        visit_count.select_from(select_from, alias="input_dataset")
        visit_count.select(Column("*"))
        visit_count.select(
            Column("*").count().over(Window(partition_by=[Column(self.dku_config.users_column_name)])),
            alias="nb_visit_user",
        )
        visit_count.select(
            Column("*").count().over(Window(partition_by=[Column(self.dku_config.items_column_name)])),
            alias="nb_visit_item",
        )
        return visit_count

    def _build_normed_count(self, select_from):
        # normalise total visits
        normed_count = SelectQuery()
        normed_count.select_from(select_from, alias="visit_count")

        normed_count.select(Column(self.dku_config.users_column_name, table_name="visit_count"))
        normed_count.select(Column(self.dku_config.items_column_name, table_name="visit_count"))
        normed_count.select(Column("nb_visit_user", table_name="visit_count"))
        normed_count.select(Column("nb_visit_item", table_name="visit_count"))

        normed_count.select(self._get_visit_normalization("nb_visit_user"), alias="visit_user_normed")
        normed_count.select(self._get_visit_normalization("nb_visit_item"), alias="visit_item_normed")

        # keep only items and users with enough visits
        normed_count.where(
            Column("nb_visit_user", table_name="visit_count").ge(Constant(self.dku_config.user_visit_threshold))
        )
        normed_count.where(
            Column("nb_visit_item", table_name="visit_count").ge(Constant(self.dku_config.item_visit_threshold))
        )
        return normed_count

    def _build_similarity(self, select_from):
        similarity = SelectQuery()
        similarity.select_from(select_from, alias="c1")

        join_condition = Column(self.pivot_column, "c1").eq_null_unsafe(Column(self.pivot_column, "c2"))

        similarity.join(select_from, JoinTypes.INNER, join_condition, alias="c2")

        similarity.where(Column(self.based_column, table_name="c1").ne(Column(self.based_column, table_name="c2")))

        similarity.group_by(Column(self.based_column, table_name="c1"))
        similarity.group_by(Column(self.based_column, table_name="c2"))

        similarity.select(Column(self.based_column, table_name="c1"), alias=f"{self.based_column}_1")
        similarity.select(Column(self.based_column, table_name="c2"), alias=f"{self.based_column}_2")

        similarity_formula = (
            Column("visit_item_normed", table_name="c1").times(Column("visit_item_normed", table_name="c2")).sum()
        )
        similarity.select(similarity_formula, alias=constants.SIMILARITY_COLUMN_NAME)
        return similarity

    def _build_row_numbers(self, select_from):
        row_numbers = SelectQuery()
        row_numbers.select_from(select_from, alias="sim")

        row_numbers.select(Column(f"{self.based_column}_1", table_name="sim"))
        row_numbers.select(Column(f"{self.based_column}_2", table_name="sim"))
        row_numbers.select(Column(constants.SIMILARITY_COLUMN_NAME, table_name="sim"))

        row_number_expression = (
            Expression()
            .rowNumber()
            .over(
                Window(
                    partition_by=[Column(f"{self.based_column}_1", table_name="sim")],
                    order_by=[Column(constants.SIMILARITY_COLUMN_NAME, table_name="sim")],
                    order_types=["DESC"],
                )
            )
        )
        row_numbers.select(row_number_expression, alias="row_number")
        return row_numbers

    def _build_top_n(self, select_from):
        top_n = SelectQuery()
        top_n.select_from(select_from, alias="row_nb")

        top_n.select(Column(f"{self.based_column}_1", table_name="row_nb"))
        top_n.select(Column(f"{self.based_column}_2", table_name="row_nb"))
        top_n.select(Column(constants.SIMILARITY_COLUMN_NAME, table_name="row_nb"))

        top_n.where(Column("row_number", table_name="row_nb").le(Constant(self.dku_config.top_n_most_similar)))
        return top_n

    def _build_collaborative_filtering(self, top_n, normed_count):
        cf_scores = SelectQuery()
        cf_scores.select_from(top_n, alias="top_n")

        join_condition = Column(f"{self.based_column}_2", "top_n").eq_null_unsafe(Column(self.based_column, "events"))
        cf_scores.join(normed_count, JoinTypes.INNER, join_condition, alias="events")

        cf_scores.group_by(Column(f"{self.based_column}_1", table_name="top_n"))
        cf_scores.group_by(Column(self.pivot_column, table_name="events"))

        cf_scores.select(Column(f"{self.based_column}_1", table_name="top_n"), alias=self.based_column)
        cf_scores.select(Column(self.pivot_column, table_name="events"))

        cf_scores.select(
            Column(constants.SIMILARITY_COLUMN_NAME, table_name="top_n").sum(), alias=constants.SCORE_COLUMN_NAME
        )

        cf_scores.order_by(Column(self.based_column))
        cf_scores.order_by(Column(constants.SCORE_COLUMN_NAME), direction="DESC")
        return cf_scores

    def execute(self, output_dataset):
        sql_executor = SQLExecutor2(dataset=self.file_manager.samples_dataset)
        sql_executor.exec_recipe_fragment(output_dataset, self.query)

    def _get_visit_normalization(self, column_to_norm):
        if self.dku_config.normalization_method == constants.NORMALIZATION_METHOD.L1.value:
            return Constant(1).div(Column(column_to_norm, table_name="visit_count").sqrt())
        else:
            return Constant(1).div(Column(column_to_norm, table_name="visit_count").sqrt())
