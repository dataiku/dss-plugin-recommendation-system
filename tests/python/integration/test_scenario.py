# -*- coding: utf-8 -*-
from dku_plugin_test_utils import dss_scenario

TEST_PROJECT_KEY = "RECOTEST"

def add_integration_test(user_dss_clients, scenario_id):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id=scenario_id)


def test_all_builds(user_dss_clients):
    add_integration_test(user_dss_clients, "all_build")