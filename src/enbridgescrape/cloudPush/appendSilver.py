import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import os
from datetime import datetime

# Assume this DataFrame contains the new data you want to append
new_data = {
    'id': [1, 2, 3],
    'value': ['A', 'B', 'C'],
    'date': [datetime(2025, 12, 1), datetime(2025, 12, 1), datetime(2025, 12, 2)]
}
df_to_append = pd.DataFrame(new_data)

# Convert pandas DataFrame to PyArrow Table
table_to_append = pa.Table.from_pandas(df_to_append)

# Define the base directory within the container
target_base_dir = "/app/data/my_parquet_dataset"

# Define the partitioning columns (e.g., 'date')
partition_cols = ['date']

# Write the data to the partitioned dataset
# existing_data_behavior='append_by_name' ensures new files are added to existing partitions
# or new partitions are created as needed.
ds.write_dataset(
    table_to_append,
    target_base_dir,
    format="parquet",
    partitioning=partition_cols,
    existing_data_behavior='append_by_name'
)

print(f"Data successfully appended to {target_base_dir}")
