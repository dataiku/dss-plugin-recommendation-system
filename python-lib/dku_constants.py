from enum import Enum


class RECIPE(Enum):
    AFFINITY_SCORE = "affinity_score"
    COLLABORATIVE_FILTERING = "collaborative_filtering"
    SAMPLING = "sampling"


class NORMALIZATION_METHOD(Enum):
    L1 = "l1"
    L2 = "l2"
    

class SIMILARITY_TYPE(Enum):
    USER_SIMILARITY = "user_similarity"
    ITEM_SIMILARITY = "item_similarity"


class SAMPLING_METHOD(Enum):
    NO_SAMPLING = "no_sampling"
    NEGATIVE_SAMPLING_PERC = "negative_sampling_percentage"


class CF_METHOD(Enum):
    USER_BASED = "user_based"
    ITEM_BASED = "item_based"
