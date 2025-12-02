# Databricks notebook source
# MAGIC %md
# MAGIC # Workflow

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ワークフロー設定
# MAGIC
# MAGIC 1. サイドバーの **ワークフロー** をクリックします。
# MAGIC 1. **ジョブを作成**をクリックします。
# MAGIC 1. **ジョブ名**を入力します。名称は参加者全体で一意となるようあなたに固有の識別子を含めてください。
# MAGIC 1. 以下の手順で`ノートブック タスク`を設定します。
# MAGIC    1. **タスク名**：`Ingest_Data`（これはデモ用にデータの新着を疑似する処理です）
# MAGIC    1. **種類**：`ノートブック`
# MAGIC    1. **ソース**：`ワークスペース`
# MAGIC    1. **パス**：ナビゲーターを使いこのノートブック（`04_Workflow`）選択
# MAGIC    1. **クラスター**：`Job＿Cluster`（`新しいジョブクラスターを追加`により任意のスペックを指定可能）
# MAGIC    1. `タスクを作成`を押下
# MAGIC </br><img src="../images/dlt.2.png" width="600"/>
# MAGIC 1. 以下の手順で`DLT タスク`を設定します。
# MAGIC    1. `Ingest_Data` タスクの下の`タスクを追加`を押下し`パイプライン`を選択
# MAGIC    1. **タスク名**：`Transform_Data_in_DLT`
# MAGIC    1. **パイプライン**：前のラボで作成した DLT パイプラインを選択
# MAGIC    1. `タスクを作成`を押下
# MAGIC 1. 必要に応じて**ジョブ通知**を設定します。
# MAGIC    * **メール（カンマ区切り）**にメールアドレスを入力
# MAGIC    * **更新時**をすべてチェック
# MAGIC    * **フロー**をすべてチェック
# MAGIC </br><img src="../images/dlt.3.png" width="600"/>
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
