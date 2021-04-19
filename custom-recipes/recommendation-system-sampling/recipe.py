from dku_config import create_dku_config
from dataiku.customrecipe import get_recipe_config
from dku_constants import RECIPE
from dku_file_manager import DkuFileManager


def create_dku_file_manager():
    dku_file_manager = DkuFileManager()
    dku_file_manager.add_input_dataset('scored_samples_dataset')
    dku_file_manager.add_input_dataset('training_samples_dataset')
    dku_file_manager.add_input_dataset('historical_samples_dataset', required=False)
    dku_file_manager.add_output_dataset('positive_negative_samples_dataset')
    return dku_file_manager


def run():
    recipe_config = get_recipe_config()
    dku_config = create_dku_config(recipe_config, RECIPE.SAMPLING)
    dku_file_manager = create_dku_file_manager()
    # TODO