STATUS_TABLE_NAME = "geo_sample_status"
CYCLE_TABLE_NAME = "geo_cycle_status"
__name__ = "geopephub"

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

DEFAULT_POSTGRES_USER = "postgres"
DEFAULT_POSTGRES_PASSWORD = "docker"
DEFAULT_POSTGRES_HOST = "localhost"
DEFAULT_POSTGRES_DB = "pep-db"
DEFAULT_POSTGRES_PORT = 5432

# db_dialects
POSTGRES_DIALECT = "postgresql+psycopg"
