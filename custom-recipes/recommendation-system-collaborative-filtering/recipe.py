from dku_config import create_dku_config
from dataiku.customrecipe import get_recipe_config
from dku_constants import RECIPE
from dku_file_manager import DkuFileManager


def create_dku_file_manager():
    file_manager = DkuFileManager()
    file_manager.add_input_dataset('samples_dataset')
    file_manager.add_output_dataset('scored_samples_dataset')
    file_manager.add_output_dataset('similarity_scores_dataset', required=False)
    return file_manager


def run():
    recipe_config = get_recipe_config()
    dku_config = create_dku_config(recipe_config, RECIPE.COLLABORATIVE_FILTERING)
    file_manager = create_dku_file_manager()
    # TODO