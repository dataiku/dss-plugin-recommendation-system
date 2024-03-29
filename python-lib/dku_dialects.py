from dataiku.sql import Dialects

SUPPORTS_FULL_OUTER_JOIN = "supports_full_outer_join"
SUPPORTS_WITH_CLAUSE = "supports_with_clause"


SUPPORTED_DIALECTS = {
    Dialects.POSTGRES: {SUPPORTS_FULL_OUTER_JOIN: True, SUPPORTS_WITH_CLAUSE: True},
    Dialects.SNOWFLAKE: {SUPPORTS_FULL_OUTER_JOIN: True, SUPPORTS_WITH_CLAUSE: True},
    Dialects.BIGQUERY: {SUPPORTS_FULL_OUTER_JOIN: True, SUPPORTS_WITH_CLAUSE: True},
    Dialects.SQLSERVER: {SUPPORTS_FULL_OUTER_JOIN: True, SUPPORTS_WITH_CLAUSE: False},
    Dialects.SYNAPSE: {SUPPORTS_FULL_OUTER_JOIN: True, SUPPORTS_WITH_CLAUSE: False},
}
