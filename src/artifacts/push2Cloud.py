import os


from contextlib import contextmanager

import pandas as pd

from azure.data.tables import TableServiceClient


@contextmanager
def getTable(tableName: str):
    deltaConstr = os.getenv('DELTA_STORAGE_CONSTR', '')
    try:
        with TableServiceClient.from_connection_string(conn_str=deltaConstr) as client:
            yield client.create_table_if_not_exists(table_name=tableName)
    finally:
        pass


# def upsertEntities(entittyFrame: pd.DataFrame, tableName: str):
#     entityList = entittyFrame.to_dict()

#     with getTable(tableName) as table_client:

#         operations = [
#             ("create", entity1),
#             # ("delete", entity2),
#             ("upsert", entity3),
#             # ("update", entity4, {"mode": "replace"}),
#         ]
#         try:
#             table_client.submit_transaction(operations)
#         except Exception as e:
#             print("There was an error with the transaction operation")
#             print(f"Error: {e}")
