from dku_config import DkuConfig
from dku_constants import (
    RECIPE,
    NORMALIZATION_METHOD,
    SIMILARITY_TYPE,
    SAMPLING_METHOD,
    CF_METHOD,
    NEGATIVE_SAMPLES_GENERATION_MODE,
)
from dku_utils import list_enum_values


def create_dku_config(recipe_id, config):
    dku_config = DkuConfig()
    if recipe_id == RECIPE.AFFINITY_SCORE:
        add_custom_collaborative_filtering_config(dku_config, config)
    elif recipe_id == RECIPE.SAMPLING:
        add_sampling_config(dku_config, config)
    elif recipe_id == RECIPE.COLLABORATIVE_FILTERING:
        add_auto_collaborative_filtering_config(dku_config, config)
    return dku_config


def add_sampling_config(dku_config, config):
    dku_config.add_param(
        name="users_column_name",
        value=config.get("scored_samples_users_column_name"),
        required=True
    )
    dku_config.add_param(
        name="items_column_name",
        value=config.get("scored_samples_items_column_name"),
        required=True
    )
    dku_config.add_param(name="score_column_names", value=config.get("score_column_names"), required=True)

    dku_config.add_param(
        name="training_samples_users_column_name",
        value=config.get("training_samples_users_column_name"),
        required=True
    )
    dku_config.add_param(
        name="training_samples_items_column_name",
        value=config.get("training_samples_items_column_name"),
        required=True
    )

    dku_config.add_param(
        name="historical_samples_users_column_name",
        value=config.get("historical_samples_users_column_name"),
        required=False
    )
    dku_config.add_param(
        name="historical_samples_items_column_name",
        value=config.get("historical_samples_items_column_name"),
        required=False
    )

    dku_config.add_param(
        name="sampling_method",
        value=config.get("sampling_method"),
        required=True,
        cast_to=SAMPLING_METHOD
    )
    dku_config.add_param(
        name="negative_samples_percentage",
        value=config.get("negative_samples_percentage"),
        checks=[{"type": "between", "op": [0, 100]}]
    )
    dku_config.add_param(
        name="negative_samples_generation_mode",
        value=config.get("negative_samples_generation_mode"),
        cast_to=NEGATIVE_SAMPLES_GENERATION_MODE
    )


def add_scoring_config(dku_config, config):
    dku_config.add_param(name="users_column_name", value=config.get("users_column_name"), required=True)
    dku_config.add_param(name="items_column_name", value=config.get("items_column_name"), required=True)
    dku_config.add_param(
        name="ratings_column_name",
        value=config.get("ratings_column_name")
    )
    dku_config.add_param(
        name="top_n_most_similar",
        value=config.get("top_n_most_similar"),
        required=True,
        checks=[{"type": "sup", "op": 0}]
    )
    dku_config.add_param(
        name="user_visit_threshold",
        value=config.get("user_visit_threshold"),
        required=True,
        checks=[{"type": "sup", "op": 0}]
    )
    dku_config.add_param(
        name="item_visit_threshold",
        value=config.get("item_visit_threshold"),
        required=True,
        checks=[{"type": "sup", "op": 0}]
    )
    dku_config.add_param(
        name="normalization_method",
        value=config.get("normalization_method"),
        required=True,
        cast_to=NORMALIZATION_METHOD
    )


def add_custom_collaborative_filtering_config(dku_config, config):
    add_scoring_config(dku_config, config)
    dku_config.add_param(
        name="similarity_scores_type",
        value=config.get("similarity_scores_type"),
        required=True,
        cast_to=SIMILARITY_TYPE
    )
    dku_config.add_param(name="similarity_column_1_name", value=config.get("similarity_column_1_name"), required=True)
    dku_config.add_param(name="similarity_column_2_name", value=config.get("similarity_column_2_name"), required=True)
    dku_config.add_param(
        name="similarity_score_column_name", value=config.get("similarity_score_column_name"), required=True
    )


def add_auto_collaborative_filtering_config(dku_config, config):
    add_scoring_config(dku_config, config)
    dku_config.add_param(
        name="collaborative_filtering_method",
        value=config.get("collaborative_filtering_method"),
        required=True,
        cast_to=CF_METHOD
    )
