# -*- coding: utf-8 -*-
from dku_plugin_test_utils import dss_scenario

TEST_PROJECT_KEY = "RECOTEST"


def add_integration_test(user_dss_clients, scenario_id):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id=scenario_id)


def test_auto_cf_scores(user_dss_clients):
    add_integration_test(user_dss_clients, "AUTOCFSCORES")


def test_cf_scores_explicit_feedbacks(user_dss_clients):
    add_integration_test(user_dss_clients, "CFSCORESEXPLICITFEEDBACKS")


def test_cf_scores_timestamp_filtering(user_dss_clients):
    add_integration_test(user_dss_clients, "CFSCORESTIMESTAMPFILTERING")


def test_custom_cf_scores(user_dss_clients):
    add_integration_test(user_dss_clients, "CUSTOMCFSCORES")


def test_negative_sampling(user_dss_clients):
    add_integration_test(user_dss_clients, "NEGATIVESAMPLING")


def test_stratified_negative_sampling(user_dss_clients):
    add_integration_test(user_dss_clients, "STRATIFIEDNEGATIVESAMPLING")
