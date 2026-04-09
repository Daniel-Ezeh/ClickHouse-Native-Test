import clickhouse_connect
import time
from dotenv import load_dotenv
from pathlib import Path
import os
from queries import example
from loguru import logger
from functools import wraps
import polars as pl

def get_repo_dir():
    """
    Get the repo root directory
    """
    path = Path(os.path.realpath(__file__)).parents[0]
    return path
    


path = f"{get_repo_dir()}/.env"
load_dotenv(path)
print(path)

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT")

connection_settings = {
                    'max_threads': 8,
                    'max_block_size': 100000
                }

query_settings= {
                    "max_threads": 8,                      # or your server CPU count
                    "max_block_size": 262144,              # larger output blocks
                    "optimize_move_to_prewhere": 1,        # push filters earlier
                    "max_bytes_before_external_group_by": 2_000_000_000,  # reduce disk spill
                    "max_memory_usage": 8_000_000_000,     # enough RAM for agg/sort
                }

def _build_client():
    return  clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        username=CLICKHOUSE_USER,
        database=CLICKHOUSE_DB,
        password=CLICKHOUSE_PASSWORD,
        port=CLICKHOUSE_PORT,
        secure=True,
        settings=connection_settings,
        verify=False,
        compress=True,
    )


def log_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"{func.__name__} executed in {duration:.4f} seconds")
        
        return result
    return wrapper


client = _build_client()


@log_execution_time
def run_native_query():
    try:
        with client.query_df_arrow_stream(
                example.year2,
                dataframe_library="polars",
                settings=query_settings,
                transport_settings={
                    "enable_http_compression": "1",        # faster over network if bandwidth is bottleneck
                },
            ) as stream:
            df= pl.concat(stream,rechunk=False)
        return df

    except Exception as e:
        print(f"Connection failed: {e}")


if __name__ == "__main__":
    print(run_native_query())
