# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## はじめに
# MAGIC
# MAGIC このラボは プロビジョニング された汎用クラスタを利用します。
# MAGIC
# MAGIC 利用する ER 図 は以下の通りです。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run .././include/handson.h

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze レイヤーの構築（Orders Bronze テーブル の作成）

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 生データ（Orders Raw ファイル群）の確認

# COMMAND ----------

files = dbutils.fs.ls(f"{sample_dataset_path}/orders-raw")
display(files)

# COMMAND ----------

df = spark.read.parquet(f"{sample_dataset_path}/orders-raw")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 生データ（Orders Raw ファイル群）をストリームソースとして定義
# MAGIC
# MAGIC データが順次流れてくるデータソース（新しいデータファイルが随時追記されるデータソース）を Auto Loader を利用したストリームソースとして定義します。

# COMMAND ----------

(spark.readStream # ストリーム Read（増分取り込み）を宣言
    .format("cloudFiles") # Auto Loader の利用を宣言（増分識別の機能有効化）
    .option("cloudFiles.format", "parquet") # 入力データの Foramat
    .option("cloudFiles.schemaLocation", f"{sample_dataset_path}/schema/orders_raw") # スキーマ推論の有効化
    .option("checkpointLocation", f"{sample_dataset_path}/checkpoints/orders_raw") #チェックポイントロケーション
    .load(f"{sample_dataset_path}/orders-raw") # 入力データのパス
    .createOrReplaceTempView("01_raw_orders_temp")) # 一時ビューを作成（Bronze にロードする前にメタデータを付与するため）

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze 用のデータ加工（メタデータの付与）
# MAGIC
# MAGIC 今回は Bronze 向けのデータ加工としてメタデータ（タイムスタンプと追加したユーザー）を付与します。
# MAGIC
# MAGIC これらのメタデータは「レコードがいつ挿入されたか」、「誰が挿入したか」を理解するのに役立つ情報を提供します。これは、ソースデータの問題をトラブルシューティングする必要がある場合に特に役立ちます。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW 01_raw_orders AS (
# MAGIC   SELECT *, 
# MAGIC     CURRENT_TIMESTAMP() arrival_time, 
# MAGIC     CURRENT_USER() AS arrived_by
# MAGIC   FROM 01_raw_orders_temp
# MAGIC )
# MAGIC -- https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.input_file_name.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Option] ストリームソース（エンリッチされた Orders Raw テーブル）の読み取り

# COMMAND ----------

# MAGIC %md
# MAGIC ストリームソースに対する読み取りがどのような挙動になるのか試してみましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ストリームソースに対する読み取り
# MAGIC SELECT * FROM 01_raw_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM 01_raw_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC クエリも結果が返却されたあともセルの実行は継続されていることが確認できます。
# MAGIC
# MAGIC このようにデータが順次流れてくることを意味するストリームソースに対するクエリは新着データの到着をポーリング（ストリーミング）し続ける動作になります。
# MAGIC
# MAGIC **注意**：この時点で読み取られたデータ件数が 1,000 件であることを覚えておいてください。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze レイヤーの構築（Orders Bronze テーブル の作成）

# COMMAND ----------

# MAGIC %md
# MAGIC ではストリームソースを入力とした Bronze テーブル を作成してみましょう。

# COMMAND ----------

(spark.table("01_raw_orders") # Bronze の入力元（＝ Order Raw ストリームソース）
      .writeStream # ストリームソースの出力指示
      .format("delta") # 出力フォーマット
      .option("checkpointLocation", f"{sample_dataset_path}/checkpoints/orders_bronze") # チェックポイント格納先（増分に対する Exactly-Once Ingest 保証のため）
      .outputMode("append") # 読み取った増分データを追記することを指示
#     .trigger(processingTime='500 milliseconds' # ストリームソースの読み取りとターゲットへの出力を 500 ms ごとに再実行（既定値）
      .table("01_bronze_orders")) # 出力先テーブル

# COMMAND ----------

# MAGIC %md
# MAGIC 先ほどと同様にストリームソースに対するクエリであるため新着データの到着をポーリング（ストリーミング）し続ける動作になります。
# MAGIC
# MAGIC ストリームソースから Bronze テーブル に読み込まれたデータ件数を確認してみましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM 01_bronze_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### テスト：新着データの到着
# MAGIC 下記のコードは Orders Raw に対して新しいデータを書き込むことで新着データの到着を疑似しています。
# MAGIC
# MAGIC 新着データの到着によって上のストリーミング処理の結果の変化を確認してみましょう。

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC 先ほど実行したセルを見てると、新しいデータが自動的に取り込まれテーブル内のレコード件数が 1,000 件から 2,000 件に増加している様子が確認できます。
# MAGIC </br><img src="../images/medallion.1.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver レイヤーの構築（Orders Silver テーブル の作成）

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders Bronze をストリームソースとして定義
# MAGIC Raw データの読み取りと比較し Delta テーブルに対しては Auto Loader 利用（増分識別） や スキーマ推論の有効化 は不要

# COMMAND ----------

(spark.readStream # ストリーム Read（増分取り込みを宣言）
# .format("cloudFiles") # Auto Loader 利用宣言（増分識別の機能有効化）
# .option("cloudFiles.format", "parquet") # Foramat 指定
# .option("cloudFiles.schemaLocation", f"{sample_dataset}/checkpoints/orders_raw") # スキーマ推論の有効化
  .table("01_bronze_orders") # Silver の入力元
  .createOrReplaceTempView("01_bronze_orders_tmp")) # 一時ビュー作成

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver 用のデータ加工（マスターデータの付与）
# MAGIC
# MAGIC 今回は Silver 向けのデータ加工として顧客マスターのユーザー情報を付与します。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 顧客マスタをロードします。

# COMMAND ----------

(spark.read # 通常 Read（全件読み取り）
      .format("json") # フォーマット指定
      .load(f"{sample_dataset_path}/customers-json") # 入力元
      .createOrReplaceTempView("01_lookup_customers")) # 一時ビュー作成

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 01_lookup_customers

# COMMAND ----------

# MAGIC %md
# MAGIC Orders Silver テーブルをデータ加工（Orders Bronze ストリーム と Customers 静的マスターテーブルとの結合）

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW 01_enriched_orders_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC          cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC   FROM 01_bronze_orders_tmp o
# MAGIC   INNER JOIN 01_lookup_customers c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Option] ストリームソース（エンリッチされた Orders Bronze テーブル）の読み取り

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 01_enriched_orders_tmp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM 01_enriched_orders_tmp;

# COMMAND ----------

# MAGIC %md
# MAGIC 先ほどと同様に結果が返却されたあともセルの実行は継続されていることが確認できます。
# MAGIC
# MAGIC **注意**：この時点で読み取られたデータ件数が 2,000 件であることを覚えておいてください。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver レイヤーの構築（Orders Silver テーブル の作成）
# MAGIC ストリームソースを入力とした Silver テーブル を作成しましょう。

# COMMAND ----------

(spark.table("01_enriched_orders_tmp") # Silver の入力元（＝ Order Bronze ストリームソース）
      .writeStream # ストリームソースの出力指示
      .format("delta") # 出力フォーマット
      .option("checkpointLocation", f"{sample_dataset_path}/checkpoints/orders_silver") # チェックポイント格納先（増分に対する Exactly-Once Ingest 保証のため）
      .outputMode("append") # 読み取った増分データを追記することを指示
#     .trigger(processingTime='500 milliseconds' # ストリームソースの読み取りとターゲットへの出力を 500 ms ごとに再実行（既定値）
      .table("01_silver_orders")) # 出力先テーブル

# COMMAND ----------

# MAGIC %md
# MAGIC ストリームソースから Silver テーブル に読み込まれたデータ件数を確認してみましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM 01_silver_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### テスト：新着データの到着
# MAGIC 下記のコードは Orders Raw に対して新しいデータを書き込むことで新着データの到着を疑似しています。
# MAGIC
# MAGIC 新着データの到着によって上のストリーミング処理の挙動を確認してみましょう。

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold レイヤーの構築（Orders Gold テーブル の作成）

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders Silver をストリームソースとして定義
# MAGIC Raw データの読み取りと比較し Delta テーブルに対しては Auto Loader 利用（増分識別） や スキーマ推論の有効化 は不要
# MAGIC

# COMMAND ----------

(spark.readStream # ストリーム Read（増分取り込みを宣言）
# .format("cloudFiles") # Auto Loader 利用宣言（増分識別の機能有効化）
# .option("cloudFiles.format", "parquet") # Foramat 指定
# .option("cloudFiles.schemaLocation", f"{sample_dataset}/checkpoints/orders_raw") # スキーマ推論の有効化
  .table("01_silver_orders") # 入力元
  .createOrReplaceTempView("01_silver_orders_tmp")) # 一時ビュー作成

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Gold 用のデータ加工（分析用の集計処理）
# MAGIC
# MAGIC 今回は Gold 向けのデータ加工として分析用の集計処理を行います。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW 01_gold_daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM 01_silver_orders_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold レイヤーの構築（Orders Silver テーブル の作成）
# MAGIC ストリームソースを入力とした Gold テーブル を作成しましょう。
# MAGIC
# MAGIC これまでの Bronze や Silver とは異なる以下のオプションに注目してください。
# MAGIC - **outputMode("complete")** ： 追記型である Bronze や Silver とは異なり Gold は集計処理であるため都度全件を洗い替え（上書き）する  
# MAGIC - **.trigger(availableNow=True)** : 集計処理が完了したらジョブを終了

# COMMAND ----------

(spark.table("01_gold_daily_customer_books_tmp") # Gold の入力元（＝ Order Silver ストリームソース）
      .writeStream # ストリームソースの出力指示
      .format("delta") # 出力フォーマット
      .outputMode("complete") # 洗い替え指定（Bronze や Silver とは異なり Gold は集計処理のため追記ではなく全件上書き）
      .option("checkpointLocation", f"{sample_dataset_path}/checkpoints/daily_customer_books_gold") # チェックポイント格納先（増分に対する Exactly-Once Ingest 保証のため）
      .trigger(availableNow=True) # 集計処理が完了したらジョブを終了（既定は処理を継続し新着入力をポーリング）
      .table("01_gold_daily_customer_books")) # 出力先テーブル

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 01_gold_daily_customer_books

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## クリーンアップ

# COMMAND ----------

# MAGIC %md
# MAGIC アクティブなストリーミングジョブをすべて終了します。

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC このノートブックを再実行する（環境をリセットする）場合は以下を実行してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS 01_bronze_orders;
# MAGIC DROP TABLE IF EXISTS 01_silver_orders;
# MAGIC DROP TABLE IF EXISTS 01_gold_daily_customer_books;

# COMMAND ----------

dbutils.fs.rm(f"{sample_dataset_path}/checkpoints/", True)
