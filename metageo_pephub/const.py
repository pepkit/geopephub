STATUS_TABLE_NAME = "geo_sample_status"
CYCLE_TABLE_NAME = "geo_cycle_status"

STATUS_OPTIONS = [
    "queued",
    "processing",
    "success",
    "failure",
    "warning",
    "initial",
]

# number of last days used in Finder
LAST_UPDATE_DATES = 1

# db_dialects
POSTGRES_DIALECT = "postgresql"
