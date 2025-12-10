# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow ジョブ

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Lakeflow ジョブ設定
# MAGIC
# MAGIC 1. サイドバーの **ジョブとパイプライン** をクリックします。
# MAGIC 1. **作成**をクリックし**ジョブ**を選択します。
# MAGIC 1. **ジョブ名**を入力します。名称は参加者全体で一意となるようあなたに固有の識別子を含めてください。(後述のラボへの影響のため、文字またはアンダースコア(_)から始まる名前とし、数字から始まる名前は避けます。)
# MAGIC 1. 以下の手順で`ノートブック タスク`を設定します。
# MAGIC    1. **タスク名**：`Ingest_Data`（これはデモ用にデータの新着を疑似する処理です）
# MAGIC    1. **種類**：`ノートブック`
# MAGIC    1. **ソース**：`ワークスペース`
# MAGIC    1. **パス**：ナビゲーターを使いこのノートブック（`04_Lakeflow Jobs`）選択
# MAGIC    1. **クラスター**：`Job＿Cluster`（`新しいジョブクラスターを追加`により任意のスペックを指定可能）
# MAGIC    1. `タスクを作成`を押下
# MAGIC </br><img src="../images/sdp.10.png" width="600"/>
# MAGIC 1. 以下の手順で`ETL パイプライン タスク`を設定します。
# MAGIC    1. `タスクを追加`を押下し`ETL パイプライン`を選択
# MAGIC    1. **タスク名**：`Transform_Data_SDP`
# MAGIC    1. **パイプライン**：**「02_Lakeflow Spark Declarative Pipelines(SDP)」**のラボで作成した SDP パイプラインを選択
# MAGIC    1. **依存先** 前のステップで作成した`Ingest_Data`
# MAGIC    1. `タスクを作成`を押下
# MAGIC </br><img src="../images/sdp.11.png" width="600"/>
# MAGIC 1. 以下の手順で`ETL パイプライン タスク`を設定します。
# MAGIC    1. `タスクを追加`を押下し`ETL パイプライン`を選択
# MAGIC    1. **タスク名**：`Transform_Data_CDC`
# MAGIC    1. **パイプライン**：**「03_Change Data Capture(CDC) in SDP」**のラボで作成した SDP パイプラインを選択
# MAGIC    1. **依存先** 前のステップで作成した`Ingest_Data`
# MAGIC    1. `タスクを作成`を押下
# MAGIC </br><img src="../images/sdp.12.png" width="600"/>
# MAGIC 1. 必要に応じて**ジョブ通知**を設定します。
# MAGIC    * **通知を編集**をクリック
# MAGIC    * **配信先**にメールアドレスを入力
# MAGIC    * **起動**、**成功**、**失敗** にチェック
# MAGIC    * **保存**をクリック
# MAGIC </br><img src="../images/sdp.13.png" width="600"/>
# MAGIC 1. **今すぐ実行**を押下します。**スケジュールとトリガー**によってスケジュール実行やファイル到着実行を構成することも可能です。

# COMMAND ----------

# MAGIC %run .././include/handson.h

# COMMAND ----------

# MAGIC %md
# MAGIC ## 新しい Raw データの到着を疑似

# COMMAND ----------

load_new_json_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debug 用：Raw データの確認

# COMMAND ----------

files = dbutils.fs.ls(f"{sample_dataset_path}/orders-json-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * from json.`${sample.dataset}/orders-json-streaming/02.json` where customer_id = "C00788"

# COMMAND ----------

files = dbutils.fs.ls(f"{sample_dataset_path}/books-cdc")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * from json.`${sample.dataset}/books-cdc/05.json`
