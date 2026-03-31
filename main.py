from clickhouse_driver import Client
import time
from dotenv import load_dotenv
from pathlib import Path
import os
import sys
from queries import example
from loguru import logger
from functools import wraps


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

settings = {
    'max_threads': 8,
    'use_numpy': False,
    'max_block_size': 100000
}

def _build_client():
    try:
        return Client(
            host=CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            settings=settings,
            secure=True,
            verify=False,
            compression='lz4',
            # tcp_keepalive=(60, 5, 2)
        )
    except RuntimeError as err:
        if "clickhouse-cityhash" in str(err):
            logger.warning(
                "LZ4 compression dependency is missing in interpreter: {}. "
                "Install with 'poetry install' and run via 'poetry run python main.py'. "
                "Falling back to compression=False.",
                sys.executable,
            )
            return Client(
                host=CLICKHOUSE_HOST,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                port=CLICKHOUSE_PORT,
                database=CLICKHOUSE_DB,
                settings=settings,
                secure=True,
                verify=False,
                compression=False
            )
        raise


def log_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        duration = end_time - start_time
        
        log = logger.info(f"{func.__name__} executed in {duration:.4f} seconds")
        
        return result
    return wrapper


client = _build_client()

@log_execution_time
def run_native_query():
    try:
        result = client.execute(example.year,columnar=True,with_column_types=False)
        return len(result)
    
    except Exception as e:
        print(f"Connection failed: {e}")


if __name__ == "__main__":
    print(run_native_query())
