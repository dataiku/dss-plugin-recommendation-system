from dku_config import DkuConfig
from dku_constants import (
    RECIPE,
    NORMALIZATION_METHOD,
    SIMILARITY_TYPE,
    SAMPLING_METHOD,
    CF_METHOD,
    NEGATIVE_SAMPLES_GENERATION_MODE,
)
import logging

logger = logging.getLogger(__name__)


def create_dku_config(recipe_id, config):
    dku_config = DkuConfig()
    if recipe_id == RECIPE.AFFINITY_SCORE:
        add_custom_collaborative_filtering_config(dku_config, config)
    elif recipe_id == RECIPE.SAMPLING:
        add_sampling_config(dku_config, config)
    elif recipe_id == RECIPE.COLLABORATIVE_FILTERING:
        add_auto_collaborative_filtering_config(dku_config, config)
    logger.info(f"Created dku_config:\n{dku_config}")
    return dku_config


def add_sampling_config(dku_config, config):
    dku_config.add_param(name="users_column_name", value=config.get("scored_samples_users_column_name"), required=True)
    dku_config.add_param(name="items_column_name", value=config.get("scored_samples_items_column_name"), required=True)
    dku_config.add_param(name="score_column_names", value=config.get("score_column_names"), required=True)

    dku_config.add_param(
        name="training_samples_users_column_name", value=config.get("training_samples_users_column_name"), required=True
    )
    dku_config.add_param(
        name="training_samples_items_column_name", value=config.get("training_samples_items_column_name"), required=True
    )

    dku_config.add_param(
        name="historical_samples_users_column_name",
        value=config.get("historical_samples_users_column_name"),
        required=False,
    )
    dku_config.add_param(
        name="historical_samples_items_column_name",
        value=config.get("historical_samples_items_column_name"),
        required=False,
    )

    dku_config.add_param(
        name="sampling_method", value=config.get("sampling_method"), required=True, cast_to=SAMPLING_METHOD
    )
    dku_config.add_param(
        name="negative_samples_percentage",
        value=config.get("negative_samples_percentage"),
        checks=[{"type": "between", "op": [0, 100]}],
    )
    dku_config.add_param(
        name="negative_samples_generation_mode",
        value=config.get("negative_samples_generation_mode"),
        cast_to=NEGATIVE_SAMPLES_GENERATION_MODE,
    )


def add_scoring_config(dku_config, config):
    dku_config.add_param(name="users_column_name", value=config.get("users_column_name"), required=True)
    dku_config.add_param(name="items_column_name", value=config.get("items_column_name"), required=True)
    dku_config.add_param(name="ratings_column_name", value=config.get("ratings_column_name"))
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
        cast_to=NORMALIZATION_METHOD,
    )
    dku_config.add_param(name="expert_mode", value=config.get("expert_mode", False), required=True)


def add_timestamp_filtering(dku_config, config):
    if dku_config.expert_mode:
        based_threshold = dku_config["user_visit_threshold"]
        dku_config.add_param(
            name="top_n_most_recent",
            value=config.get("top_n_most_recent"),
            checks=[
                {"type": "sup", "op": 0},
                {
                    "type": "sup_eq",
                    "op": based_threshold,
                    "err_msg": "The timestamp filtering value should be superior to the User visit threshold",
                },
            ],
        )
        dku_config.add_param(name="timestamps_column_name", value=config.get("timestamps_column_name"), required=True)


def add_custom_collaborative_filtering_config(dku_config, config):
    add_scoring_config(dku_config, config)
    dku_config.add_param(
        name="similarity_scores_type",
        value=config.get("similarity_scores_type"),
        required=True,
        cast_to=SIMILARITY_TYPE,
    )
    dku_config.add_param(name="similarity_column_1_name", value=config.get("similarity_column_1_name"), required=True)
    dku_config.add_param(name="similarity_column_2_name", value=config.get("similarity_column_2_name"), required=True)
    dku_config.add_param(
        name="similarity_score_column_name", value=config.get("similarity_score_column_name"), required=True
    )

    add_timestamp_filtering(dku_config, config)


def add_auto_collaborative_filtering_config(dku_config, config):
    add_scoring_config(dku_config, config)
    dku_config.add_param(
        name="collaborative_filtering_method",
        value=config.get("collaborative_filtering_method"),
        cast_to=CF_METHOD,
    )

    add_timestamp_filtering(dku_config, config)
