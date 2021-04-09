

class SQLDatasetSimilarity:
    def __init__(self, input_dataset):
        self.input_dataset = input_dataset

    def process_similarity(self, user_col, item_col):
        user_and_item_visits = self.compute_user_and_item_visits(self.input_dataset, user_col, item_col)
        normalised_total_visits = self.compute_normalise_total_visits(user_and_item_visits, user_col, item_col)
        similarity = self.compute_similarity(normalised_total_visits, user_col, item_col)
        similarity_order = self.compute_similarity_order(similarity, user_col, item_col)
        similarity_order_filtered = self.filter_similarity_results(similarity_order, user_col, item_col)
        cf_scores = self.compute_cf_scores(similarity_order_filtered, user_col, item_col)
        return cf_scores

    def compute_user_and_item_visits(self, select_from, user_col, item_col):
        # total user and item visits
        visit_count = SelectQuery().select_from(select_from)
        visit_count.select(Column("*"))
        visit_count.select(Column("*").count().over(Window(partition_by=[Column(user_col)])), alias="nb_visit_user")
        visit_count.select(Column("*").count().over(Window(partition_by=[Column(item_col)])), alias="nb_visit_item")
        return visit_count

    def compute_normalise_total_visits(self, select_from, user_col, item_col):
        # normalise total visits
        normed_count = SelectQuery().select_from(select_from)

        normed_count.select(Column(user_col, table_name="visit_count"))
        normed_count.select(Column(item_col, table_name="visit_count"))
        normed_count.select(Column("nb_visit_user", table_name="visit_count"))
        normed_count.select(Column("nb_visit_item", table_name="visit_count"))

        ratings = Column("rating", table_name="visit_count")
        normed_count.select(ratings.div(ratings.times(ratings).sum().over(Window(partition_by=[Column(user_col, table_name="visit_count")])).sqrt()), alias="visit_user_normed")
        normed_count.select(ratings.div(ratings.times(ratings).sum().over(Window(partition_by=[Column(item_col, table_name="visit_count")])).sqrt()), alias="visit_item_normed")

        # keep only items and users with enough visits
        normed_count.where(Column("nb_visit_user", table_name="visit_count").ge(Constant(10)))
        normed_count.where(Column("nb_visit_item", table_name="visit_count").ge(Constant(10)))

        normed_count.order_by(Column(user_col, table_name="visit_count"))
        normed_count.order_by(Column(item_col, table_name="visit_count"))
        
        return normed_count
        
    def compute_similarity(self, select_from, user_col, item_col):
        items_similarity = SelectQuery().select_from(select_from)

        join_condition = Column("user_id", "c1").eq_null_unsafe(Column("user_id", "c2"))

        items_similarity.join(self.input_dataset, JoinTypes.INNER, join_condition, alias="c2")

        items_similarity.where(Column("item_id", table_name="c1").ne(Column("item_id", table_name="c2")))

        items_similarity.group_by(Column("item_id", table_name="c1"))
        items_similarity.group_by(Column("item_id", table_name="c2"))

        items_similarity.select(Column("item_id", table_name="c1"), alias="item_1")
        items_similarity.select(Column("item_id", table_name="c2"), alias="item_2")

        similarity_formula = Column("visit_item_normed", table_name="c1").times(Column("visit_item_normed", table_name="c2")).sum()
        items_similarity.select(similarity_formula, alias="similarity_item")
        
        return items_similarity

    def compute_similarity_order(self, select_from, user_col, item_col)
        row_numbers = SelectQuery().select_from(select_from, alias="item_sim")

        row_numbers.select(Column("item_1", table_name="item_sim"))
        row_numbers.select(Column("item_2", table_name="item_sim"))
        row_numbers.select(Column("similarity_item", table_name="item_sim"))

        row_number_expression = Expression().rowNumber().over(
            Window(
                partition_by=[Column("item_1", table_name="item_sim")],
                order_by=[Column("similarity_item", table_name="item_sim")],
                order_types=['DESC']
            )
        )
        row_numbers.select(row_number_expression, alias="row_number")
        return row_numbers

    def filter_similarity_results(self, select_from, user_col, item_col)
        top_items = SelectQuery().select_from(select_from, alias="row_nb")

        top_items.select(Column("item_1", table_name="row_nb"))
        top_items.select(Column("item_2", table_name="row_nb"))
        top_items.select(Column("similarity_item", table_name="row_nb"))

        top_items.where(Column("row_number", table_name="row_nb").le(Constant(top_n_items)))

    def compute_cf_scores(self, select_from, user_col, item_col)
        item_cf = SelectQuery().select_from(select_from, alias="top_items")

        join_condition = Column("item_2", "top_items").eq_null_unsafe(Column("item_id", "events"))
        item_cf.join(self.input_dataset, JoinTypes.INNER, join_condition, alias="events")

        item_cf.group_by(Column("item_1", table_name="top_items"))
        item_cf.group_by(Column("user_id", table_name="events"))

        item_cf.select(Column("item_1", table_name="top_items"), alias="item_id")
        item_cf.select(Column("user_id", table_name="events"))

        item_cf.select(Column("similarity_item", table_name="top_items").sum(), alias="item_based_cf_score")

        item_cf.order_by(Column("item_id"))
        item_cf.order_by(Column("item_based_cf_score"), direction="DESC")

        return item_cf
        
