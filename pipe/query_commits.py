import asyncio
import polars as pl
import time
import logging
from typing import Dict
from hypermanager.events import EventConfig
from hypermanager.manager import HyperManager
from hypermanager.protocols.mev_commit import mev_commit_config
from data_processing import (
    load_data_from_duckdb,
    join_dataframes,
    get_latest_block_number,
    write_to_duckdb,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

# Define your event configurations globally
opened_commits_config = EventConfig(
    name=mev_commit_config["OpenedCommitmentStored"].name,
    signature=mev_commit_config["OpenedCommitmentStored"].signature,
    column_mapping=mev_commit_config["OpenedCommitmentStored"].column_mapping,
)

unopened_commits_config = EventConfig(
    name=mev_commit_config["UnopenedCommitmentStored"].name,
    signature=mev_commit_config["UnopenedCommitmentStored"].signature,
    column_mapping=mev_commit_config["UnopenedCommitmentStored"].column_mapping,
)

commits_processed_config = EventConfig(
    name=mev_commit_config["CommitmentProcessed"].name,
    signature=mev_commit_config["CommitmentProcessed"].signature,
    column_mapping=mev_commit_config["CommitmentProcessed"].column_mapping,
)


async def get_events():
    """
    Fetch event logs from the MEV-Commit system and store them in DuckDB tables.
    Then load the data from DuckDB, perform joins, and return the commitments DataFrame.
    """
    manager = HyperManager(url="https://mev-commit.hypersync.xyz")
    db_filename = "mev_commit.duckdb"

    # Get the latest block numbers from each table
    latest_blocks = {}

    # List of tables with their event configurations and block number column names
    tables = [
        {
            "table_name": "commit_stores",
            "block_column": "block_number",
            "event_config": opened_commits_config,
        },
        {
            "table_name": "encrypted_stores",
            "block_column": "block_number",
            "event_config": unopened_commits_config,
        },
        {
            "table_name": "commits_processed",
            "block_column": "block_number",
            "event_config": commits_processed_config,
        },
    ]

    # Get the latest block numbers and aggregate logs
    latest_blocks_info = []
    for table in tables:
        latest_block = get_latest_block_number(
            table["table_name"], table["block_column"], db_filename
        )
        latest_blocks[table["table_name"]] = latest_block
        latest_blocks_info.append(f"{table['table_name']}: {latest_block}")
    logging.info("Latest blocks - " + "; ".join(latest_blocks_info))

    # Query events and get Polars DataFrames
    fetched_records_info = []
    dataframes: Dict[str, pl.DataFrame] = {}

    for table in tables:
        table_name = table["table_name"]
        event_config = table["event_config"]
        from_block = latest_blocks.get(table_name, 0) + 1

        try:
            df = await manager.execute_event_query(
                event_config,
                tx_data=True,
                from_block=from_block,
            )
            record_count = len(df)
            dataframes[table_name] = df
            fetched_records_info.append(f"{table_name}: {record_count} new records")
        except ValueError as e:
            dataframes[table_name] = pl.DataFrame()  # Empty DataFrame
            fetched_records_info.append(f"{table_name}: 0 new records")
    logging.info("Fetched records - " + "; ".join(fetched_records_info))

    # Write each DataFrame to its own DuckDB table
    write_info = []
    for table_name, df in dataframes.items():
        write_result = write_to_duckdb(df, table_name, db_filename)
        write_info.append(write_result)
    logging.info("Write to DuckDB - " + "; ".join(write_info))

    # Load data from DuckDB and perform joins
    loaded_dataframes = load_data_from_duckdb(db_filename)
    commitments_df = join_dataframes(loaded_dataframes)
    logging.info(f"Total commitments: {len(commitments_df)}")

    print(commitments_df.shape)

    # Return the commitments DataFrame
    return commitments_df


if __name__ == "__main__":
    # Run the async function in a loop every 30 seconds
    while True:
        asyncio.run(get_events())
        time.sleep(30)  # Wait for 30 seconds before fetching new data
