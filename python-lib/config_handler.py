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


def create_dku_config(recipe_id, config, file_manager=None):
    dku_config = DkuConfig()
    if recipe_id == RECIPE.AFFINITY_SCORE:
        add_custom_collaborative_filtering_config(dku_config, config, file_manager)
    elif recipe_id == RECIPE.SAMPLING:
        add_sampling_config(dku_config, config, file_manager)
    elif recipe_id == RECIPE.COLLABORATIVE_FILTERING:
        add_auto_collaborative_filtering_config(dku_config, config, file_manager)
    logger.info(f"Created dku_config:\n{dku_config}")
    return dku_config


def add_sampling_config(dku_config, config, file_manager):
    scored_samples_dataset_columns = get_column_names(file_manager.scored_samples_dataset)

    dku_config.add_param(
        name="users_column_name",
        value=config.get("scored_samples_users_column_name"),
        checks=[
            {"type": "is_type", "op": str},
            {
                "type": "in",
                "op": scored_samples_dataset_columns,
                "err_msg": f"Invalid users column of scored samples selection: {config.get('scored_samples_users_column_name')}.",
            },
        ],
        required=True,
    )
    dku_config.add_param(
        name="items_column_name",
        value=config.get("scored_samples_items_column_name"),
        checks=[
            {"type": "is_type", "op": str},
            {
                "type": "in",
                "op": scored_samples_dataset_columns,
                "err_msg": f"Invalid items column of scored samples selection: {config.get('scored_samples_items_column_name')}.",
            },
        ],
        required=True,
    )
    dku_config.add_param(
        name="score_column_names",
        value=config.get("score_column_names"),
        checks=[
            {"type": "is_type", "op": list},
            {
                "type": "in",
                "op": scored_samples_dataset_columns,
                "err_msg": f"Invalid affinity scores column(s) of scored samples selection: {config.get('score_column_names')}.",
            },
        ],
        required=True,
    )

    training_samples_dataset_columns = get_column_names(file_manager.training_samples_dataset)

    dku_config.add_param(
        name="training_samples_users_column_name",
        value=config.get("training_samples_users_column_name"),
        checks=[
            {"type": "is_type", "op": str},
            {
                "type": "in",
                "op": training_samples_dataset_columns,
                "err_msg": f"Invalid users column of training samples selection: {config.get('training_samples_users_column_name')}.",
            },
        ],
        required=True,
    )
    dku_config.add_param(
        name="training_samples_items_column_name",
        value=config.get("training_samples_items_column_name"),
        checks=[
            {"type": "is_type", "op": str},
            {
                "type": "in",
                "op": training_samples_dataset_columns,
                "err_msg": f"Invalid items column of training samples selection: {config.get('training_samples_items_column_name')}.",
            },
        ],
        required=True,
    )

    dku_config.add_param(name="historical_samples", value=config.get("historical_samples", False), required=True)

    if dku_config.historical_samples:
        historical_samples_dataset_columns = get_column_names(file_manager.historical_samples_dataset)

        dku_config.add_param(
            name="historical_samples_users_column_name",
            value=config.get("historical_samples_users_column_name"),
            checks=[
                {"type": "is_type", "op": str},
                {
                    "type": "in",
                    "op": historical_samples_dataset_columns,
                    "err_msg": f"Invalid users column of historical samples selection: {config.get('historical_samples_users_column_name')}.",
                },
            ],
            required=True,
        )
        dku_config.add_param(
            name="historical_samples_items_column_name",
            value=config.get("historical_samples_items_column_name"),
            checks=[
                {"type": "is_type", "op": str},
                {
                    "type": "in",
                    "op": historical_samples_dataset_columns,
                    "err_msg": f"Invalid items column of historical samples selection: {config.get('historical_samples_items_column_name')}.",
                },
            ],
            required=True,
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


def add_scoring_config(dku_config, config, file_manager):
    samples_dataset_columns = get_column_names(file_manager.samples_dataset)

    dku_config.add_param(
        name="users_column_name",
        value=config.get("users_column_name"),
        checks=[
            {"type": "is_type", "op": str},
            {
                "type": "in",
                "op": samples_dataset_columns,
                "err_msg": f"Invalid users column selection: {config.get('users_column_name')}.",
            },
        ],
        required=True,
    )
    dku_config.add_param(
        name="items_column_name",
        value=config.get("items_column_name"),
        checks=[
            {"type": "is_type", "op": str},
            {
                "type": "in",
                "op": samples_dataset_columns,
                "err_msg": f"Invalid items column selection: {config.get('items_column_name')}.",
            },
        ],
        required=True,
    )
    dku_config.add_param(
        name="ratings_column_name",
        value=config.get("ratings_column_name"),
        checks=[
            {"type": "is_type", "op": str},
            {
                "type": "in",
                "op": samples_dataset_columns,
                "err_msg": f"Invalid ratings column selection: {config.get('ratings_column_name')}.",
            },
        ],
        required=False,
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
        cast_to=NORMALIZATION_METHOD,
    )
    dku_config.add_param(name="timestamp_filtering", value=config.get("timestamp_filtering", False), required=True)


def add_timestamp_filtering(dku_config, config):
    if dku_config.timestamp_filtering:
        dku_config.add_param(
            name="top_n_most_recent",
            value=config.get("top_n_most_recent"),
            checks=[
                {"type": "sup", "op": 0},
            ],
            required=True,
        )
        dku_config.add_param(name="timestamps_column_name", value=config.get("timestamps_column_name"), required=True)


def add_custom_collaborative_filtering_config(dku_config, config, file_manager):
    add_scoring_config(dku_config, config, file_manager)
    dku_config.add_param(
        name="similarity_scores_type",
        value=config.get("similarity_scores_type"),
        required=True,
        cast_to=SIMILARITY_TYPE,
    )

    similarity_scores_dataset_columns = get_column_names(file_manager.similarity_scores_dataset)

    similarity_pairs = "users" if dku_config.similarity_scores_type == SIMILARITY_TYPE.USER_SIMILARITY else "items"
    similarity_column_1_name = f"similarity_{similarity_pairs}_column_1_name"
    similarity_column_2_name = f"similarity_{similarity_pairs}_column_2_name"

    dku_config.add_param(
        name="similarity_column_1_name",
        value=config.get(similarity_column_1_name),
        checks=[
            {"type": "is_type", "op": str},
            {
                "type": "in",
                "op": similarity_scores_dataset_columns,
                "err_msg": f"Invalid {similarity_pairs} column 1 selection: {config.get(similarity_column_1_name)}.",
            },
        ],
        required=True,
    )
    dku_config.add_param(
        name="similarity_column_2_name",
        value=config.get(similarity_column_2_name),
        checks=[
            {"type": "is_type", "op": str},
            {
                "type": "in",
                "op": similarity_scores_dataset_columns,
                "err_msg": f"Invalid {similarity_pairs} column 2 selection: {config.get(similarity_column_2_name)}.",
            },
        ],
        required=True,
    )

    dku_config.add_param(
        name="similarity_score_column_name",
        value=config.get("similarity_score_column_name"),
        checks=[
            {"type": "is_type", "op": str},
            {
                "type": "in",
                "op": similarity_scores_dataset_columns,
                "err_msg": f"Invalid similarity column selection: {config.get('similarity_score_column_name')}.",
            },
        ],
        required=True,
    )

    add_timestamp_filtering(dku_config, config)


def add_auto_collaborative_filtering_config(dku_config, config, file_manager):
    add_scoring_config(dku_config, config, file_manager)
    dku_config.add_param(
        name="collaborative_filtering_method",
        value=config.get("collaborative_filtering_method"),
        cast_to=CF_METHOD,
    )

    add_timestamp_filtering(dku_config, config, file_manager)


def get_column_names(dataset):
    dataset_columns = [column["name"] for column in dataset.read_schema()]
    return dataset_columns