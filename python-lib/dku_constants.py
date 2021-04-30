from enum import Enum


class RECIPE(Enum):
    AFFINITY_SCORE = "affinity_score"
    COLLABORATIVE_FILTERING = "collaborative_filtering"
    SAMPLING = "sampling"


class NORMALIZATION_METHOD(Enum):
    L1 = "l1_normalization"
    L2 = "l2_normalization"


class SIMILARITY_TYPE(Enum):
    USER_SIMILARITY = "user_similarity"
    ITEM_SIMILARITY = "item_similarity"


class NEGATIVE_SAMPLES_GENERATION_MODE(Enum):
    REMOVE_HISTORICAL_SAMPLES = "remove_historical_samples"


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
