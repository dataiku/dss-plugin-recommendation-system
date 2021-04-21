from query_handlers import ScoringHandler
from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2
import dku_constants as constants
import logging

logger = logging.getLogger(__name__)


class AutoScoringHandler(ScoringHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(f"self.__dict__: {self.__dict__}")

    def build(self):
        # inputs
        samples_dataset = self.file_manager.samples_dataset

        # total user and item visits
        visit_count = SelectQuery()
        visit_count.select_from(samples_dataset)
        visit_count.select(Column("*"))
        visit_count.select(
            Column("*").count().over(Window(partition_by=[Column(constants.USER_ID_COLUMN_NAME)])),
            alias="nb_visit_user",
        )
        visit_count.select(
            Column("*").count().over(Window(partition_by=[Column(constants.ITEM_ID_COLUMN_NAME)])),
            alias="nb_visit_item",
        )

        # normalise total visits
        normed_count = SelectQuery()
        normed_count.select_from(visit_count, alias="visit_count")

        normed_count.select(Column(constants.USER_ID_COLUMN_NAME, table_name="visit_count"))
        normed_count.select(Column(constants.ITEM_ID_COLUMN_NAME, table_name="visit_count"))
        normed_count.select(Column("nb_visit_user", table_name="visit_count"))
        normed_count.select(Column("nb_visit_item", table_name="visit_count"))

        normed_count.select(
            Constant(1).div(Column("nb_visit_user", table_name="visit_count").sqrt()), alias="visit_user_normed"
        )
        normed_count.select(
            Constant(1).div(Column("nb_visit_item", table_name="visit_count").sqrt()), alias="visit_item_normed"
        )

        # keep only items and users with enough visits
        normed_count.where(
            Column("nb_visit_user", table_name="visit_count").ge(Constant(self.dku_config.user_visit_threshold))
        )
        normed_count.where(
            Column("nb_visit_item", table_name="visit_count").ge(Constant(self.dku_config.item_visit_threshold))
        )

        # normed_count.order_by(Column("constants.USER_ID_COLUMN_NAME", table_name="visit_count"))
        # normed_count.order_by(Column(constants.ITEM_ID_COLUMN_NAME, table_name="visit_count"))

        items_similarity = SelectQuery()
        items_similarity.select_from(normed_count, alias="c1")

        join_condition = Column(constants.USER_ID_COLUMN_NAME, "c1").eq_null_unsafe(
            Column(constants.USER_ID_COLUMN_NAME, "c2")
        )

        items_similarity.join(normed_count, JoinTypes.INNER, join_condition, alias="c2")

        items_similarity.where(
            Column(constants.ITEM_ID_COLUMN_NAME, table_name="c1").ne(
                Column(constants.ITEM_ID_COLUMN_NAME, table_name="c2")
            )
        )

        items_similarity.group_by(Column(constants.ITEM_ID_COLUMN_NAME, table_name="c1"))
        items_similarity.group_by(Column(constants.ITEM_ID_COLUMN_NAME, table_name="c2"))

        items_similarity.select(
            Column(constants.ITEM_ID_COLUMN_NAME, table_name="c1"), alias=f"{constants.ITEM_ID_COLUMN_NAME}_1"
        )
        items_similarity.select(
            Column(constants.ITEM_ID_COLUMN_NAME, table_name="c2"), alias=f"{constants.ITEM_ID_COLUMN_NAME}_2"
        )

        similarity_formula = (
            Column("visit_item_normed", table_name="c1").times(Column("visit_item_normed", table_name="c2")).sum()
        )
        items_similarity.select(similarity_formula, alias=constants.SIMILARITY_COLUMN_NAME)

        row_numbers = SelectQuery()
        row_numbers.select_from(items_similarity, alias="item_sim")

        row_numbers.select(Column(f"{constants.ITEM_ID_COLUMN_NAME}_1", table_name="item_sim"))
        row_numbers.select(Column(f"{constants.ITEM_ID_COLUMN_NAME}_2", table_name="item_sim"))
        row_numbers.select(Column(constants.SIMILARITY_COLUMN_NAME, table_name="item_sim"))

        row_number_expression = (
            Expression()
            .rowNumber()
            .over(
                Window(
                    partition_by=[Column(f"{constants.ITEM_ID_COLUMN_NAME}_1", table_name="item_sim")],
                    order_by=[Column(constants.SIMILARITY_COLUMN_NAME, table_name="item_sim")],
                    order_types=["DESC"],
                )
            )
        )
        row_numbers.select(row_number_expression, alias="row_number")

        top_items = SelectQuery()
        top_items.select_from(row_numbers, alias="row_nb")

        top_items.select(Column(f"{constants.ITEM_ID_COLUMN_NAME}_1", table_name="row_nb"))
        top_items.select(Column(f"{constants.ITEM_ID_COLUMN_NAME}_2", table_name="row_nb"))
        top_items.select(Column(constants.SIMILARITY_COLUMN_NAME, table_name="row_nb"))

        top_items.where(Column("row_number", table_name="row_nb").le(Constant(self.dku_config.top_n_most_similar)))

        item_cf = SelectQuery()
        item_cf.select_from(top_items, alias="top_items")

        join_condition = Column(f"{constants.ITEM_ID_COLUMN_NAME}_2", "top_items").eq_null_unsafe(
            Column(constants.ITEM_ID_COLUMN_NAME, "events")
        )
        item_cf.join(normed_count, JoinTypes.INNER, join_condition, alias="events")

        item_cf.group_by(Column(f"{constants.ITEM_ID_COLUMN_NAME}_1", table_name="top_items"))
        item_cf.group_by(Column(constants.USER_ID_COLUMN_NAME, table_name="events"))

        item_cf.select(
            Column(f"{constants.ITEM_ID_COLUMN_NAME}_1", table_name="top_items"), alias=constants.ITEM_ID_COLUMN_NAME
        )
        item_cf.select(Column(constants.USER_ID_COLUMN_NAME, table_name="events"))

        item_cf.select(
            Column(constants.SIMILARITY_COLUMN_NAME, table_name="top_items").sum(), alias=constants.SCORE_COLUMN_NAME
        )

        item_cf.order_by(Column(constants.ITEM_ID_COLUMN_NAME))
        item_cf.order_by(Column(constants.SCORE_COLUMN_NAME), direction="DESC")

        # final sql query

        self.query = toSQL(item_cf, dataset=samples_dataset)
        logger.warning("self.query : ", self.query)

    def execute(self):
        sql_executor = SQLExecutor2(dataset=self.file_manager.samples_dataset)
        sql_executor.exec_recipe_fragment(self.file_manager.scored_samples_dataset, self.query)
