from config_handler import create_dku_config
from dataiku.customrecipe import get_recipe_config
from dku_constants import RECIPE
from dku_file_manager import DkuFileManager
from query_handlers import CustomScoringHandler
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_dku_file_manager():
    file_manager = DkuFileManager()
    file_manager.add_input_dataset("samples_dataset")
    file_manager.add_input_dataset("similarity_scores_dataset")
    file_manager.add_output_dataset("scored_samples_dataset")
    return file_manager


def run():
    logger.info("Running recipe Custom collaborative filtering")
    recipe_config = get_recipe_config()
    file_manager = create_dku_file_manager()
    dku_config = create_dku_config(RECIPE.AFFINITY_SCORE, recipe_config, file_manager=file_manager)
    query_handler = CustomScoringHandler(dku_config, file_manager)
    query_handler.build()
    logger.info("Recipe done !")


run()