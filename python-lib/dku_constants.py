from enum import Enum


class RECIPE(Enum):
    AFFINITY_SCORE = "affinity_score"
    COLLABORATIVE_FILTERING = "collaborative_filtering"
    SAMPLING = "sampling"


class SIMILARITY_TYPE(Enum):
    USER_SIMILARITY = "user_similarity"
    ITEM_SIMILARITY = "item_similarity"


class SAMPLING_METHOD(Enum):
    NO_SAMPLING = "no_sampling"
    NEGATIVE_SAMPLING_PERC = "negative_samples_percentage"


class CF_METHOD(Enum):
    USER_BASED = "user_based"
    ITEM_BASED = "item_based"


USER_ID_COLUMN_NAME = "user_id"
ITEM_ID_COLUMN_NAME = "item_id"
RATING_COLUMN_NAME = "rating"
SCORE_COLUMN_NAME = "score"
SIMILARITY_COLUMN_NAME = "similarity"
TARGET_COLUMN_NAME = "target"

DSS_TO_SQL_TYPES = {
    "date": "date",
    "tinyint": "int",
    "smallint": "int",
    "int": "int",
    "bigint": "int",
    "float": "double",
    "double": "double",
}
