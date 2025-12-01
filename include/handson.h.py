# Databricks notebook source
# 環境固有パラメータ定義
your_identifier = "koichino1218" # 参加者全体で一意となるようあなたに固有の識別子をアルファベットで入力してください
your_catalog = "uccatalog" # 講師から提示されるカタログ名を入力してください（このカタログは参加者全員で共有します）

your_schema = your_identifier + "_schema"
print("your_identifier = " + your_identifier)
print("your_catalog = " + your_catalog)
print("your_schema = " + your_schema)

# COMMAND ----------

# 環境共通パラメータ定義
your_volume = "sample_dataset_volume"
volume_path = "/Volumes/" + your_catalog + "/" + your_schema + "/" + your_volume
sample_dataset_path = volume_path
print("sample_dataset_path = " + sample_dataset_path)

# COMMAND ----------

# 参加者固有スキーマ作成 ＆ コンテキスト設定
spark.sql(f"USE CATALOG {your_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {your_catalog}.{your_schema}")
spark.sql(f"USE SCHEMA {your_schema}")

# COMMAND ----------

# ユーティリティ関数定義

def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1

# Structured Streaming
streaming_dir = f"{sample_dataset_path}/orders-streaming"
raw_dir = f"{sample_dataset_path}/orders-raw"

def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.parquet"
    print(f"Loading {latest_file} file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")

    
def load_new_data(all=False):
    index = get_index(raw_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_file(index)
            index += 1
    else:
        load_file(index)
        index += 1

# DLT
streaming_orders_dir = f"{sample_dataset_path}/orders-json-streaming"
streaming_books_dir = f"{sample_dataset_path}/books-streaming"

raw_orders_dir = f"{sample_dataset_path}/orders-json-raw"
raw_books_dir = f"{sample_dataset_path}/books-cdc"

def load_json_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.json"
    print(f"Loading {latest_file} orders file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_orders_dir}/{latest_file}", f"{raw_orders_dir}/{latest_file}")
    print(f"Loading {latest_file} books file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_books_dir}/{latest_file}", f"{raw_books_dir}/{latest_file}")

    
def load_new_json_data(all=False):
    index = get_index(raw_orders_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_json_file(index)
            index += 1
    else:
        load_json_file(index)
        index += 1

# COMMAND ----------

print("Preparation is successfully completed!")
