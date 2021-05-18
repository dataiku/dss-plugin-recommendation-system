from config_handler import create_dku_config
from dataiku.customrecipe import get_recipe_config
from dku_constants import RECIPE
from dku_file_manager import DkuFileManager
from query_handlers import SamplingHandler
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_dku_file_manager():
    dku_file_manager = DkuFileManager()
    dku_file_manager.add_input_dataset("scored_samples_dataset")
    dku_file_manager.add_input_dataset("training_samples_dataset")
    dku_file_manager.add_input_dataset("historical_samples_dataset", required=False)
    dku_file_manager.add_output_dataset("positive_negative_samples_dataset")
    return dku_file_manager


def run():
    logger.info("Running recipe Sampling")
    recipe_config = get_recipe_config()
    dku_config = create_dku_config(RECIPE.SAMPLING, recipe_config)
    file_manager = create_dku_file_manager()
    query_handler = SamplingHandler(dku_config, file_manager)
    query_handler.build()
    logger.info("Recipe done !")


run()