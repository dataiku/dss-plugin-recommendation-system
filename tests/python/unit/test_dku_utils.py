# -*- coding: utf-8 -*-
import dku_utils
from dku_constants import RECIPE


def test_list_enum_values():
    recipe_enum_values = dku_utils.list_enum_values(RECIPE)
    assert recipe_enum_values == ["affinity_score", "collaborative_filtering", "sampling"]
