from dku_config import DkuConfig
from dku_constants import RECIPE, NORMALIZATION_METHOD, SIMILARITY_TYPE, SAMPLING_METHOD, CF_METHOD
from dku_utils import list_enum_values


def create_dku_config(recipe_id, config):
    dku_config = DkuConfig()
    if recipe_id == RECIPE.AFFINITY_SCORE:
        add_affinity_score_config(dku_config, config)
    elif recipe_id == RECIPE.SAMPLING:
        add_sampling_config(dku_config, config)
    elif recipe_id == RECIPE.COLLABORATIVE_FILTERING:
        add_collaborative_filtering_config(dku_config, config)
    return dku_config


def add_sampling_config(dku_config, config):
    dku_config.add_param(name="score_columns_name", value=config.get("score_columns_name"), required=True)
    dku_config.add_param(
        name="sampling_method",
        value=config.get("sampling_method"),
        required=True,
        checks=[{"type": "in", "op": list_enum_values(SAMPLING_METHOD)}],
    )
    dku_config.add_param(
        name="negative_samples_percentage",
        value=config.get("negative_samples_percentage"),
        checks=[{"type": "between", "op": [0, 100]}],
    )


def add_scoring_config(dku_config, config):
    dku_config.add_param(name="users_column_name", value=config.get("users_column_name"), required=True)
    dku_config.add_param(name="items_column_name", value=config.get("items_column_name"), required=True)
    dku_config.add_param(
        name="ratings_column_name",
        value=config.get("ratings_column_name"),
    )
    dku_config.add_param(
        name="top_n_most_similar",
        value=config.get("top_n_most_similar"),
        required=True,
        checks=[{"type": "sup", "op": 0}],
    )
    dku_config.add_param(
        name="user_visit_threshold",
        value=config.get("user_visit_threshold"),
        required=True,
        checks=[{"type": "sup", "op": 0}],
    )
    dku_config.add_param(
        name="item_visit_threshold",
        value=config.get("item_visit_threshold"),
        required=True,
        checks=[{"type": "sup", "op": 0}],
    )
    dku_config.add_param(
        name="normalization_method",
        value=config.get("normalization_method"),
        required=True,
        checks=[{"type": "in", "op": list_enum_values(NORMALIZATION_METHOD)}],
    )


def add_affinity_score_config(dku_config, config):
    add_scoring_config(dku_config, config)
    dku_config.add_param(
        name="similarity_scores_type",
        value=config.get("similarity_scores_type"),
        required=True,
        checks=[{"type": "in", "op": list_enum_values(SIMILARITY_TYPE)}],
    )


def add_collaborative_filtering_config(dku_config, config):
    add_scoring_config(dku_config, config)
    dku_config.add_param(
        name="collaborative_filtering_method",
        value=config.get("collaborative_filtering_method"),
        required=True,
        checks=[{"type": "in", "op": list_enum_values(CF_METHOD)}],
    )
