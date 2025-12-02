-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lakeflow Spark 宣言型パイプライン （Spark Declarative Pipelines：SDP）
-- MAGIC (旧称：Delta Live Tables（DLT））

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## はじめに
-- MAGIC
-- MAGIC このラボはノートブック内のセルを順次実行していくようなインタラクティブな形式ではなく、以下の「SDP パイプライン設定と実行」の手順に従って SDP ジョブを構成＆実行します。
-- MAGIC
-- MAGIC 利用する ER 図 は以下の通りです。

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## SDP パイプライン設定と実行
-- MAGIC
-- MAGIC 1. サイドバーの **ジョブとパイプライン** をクリックします。
-- MAGIC 1. **作成**をクリックし**ETLパイプライン**を選択します。
-- MAGIC 1. **パイプライン名**を入力します。名称は参加者全体で一意となるようあなたに固有の識別子を含めてください。
-- MAGIC 1. **サーバレス**は`チェックせず`、**製品エディション**は `Advanced` を選択します。
-- MAGIC 1. **パイプラインモード**は `Trigger` を選択します。本ラボではファイルの取り込みを1回のみ行うため `Trigger` を選択しています。
-- MAGIC 1. **パス**はナビゲーターを使いこのノートブック（`02_Delta Live Tables`）選択します。
-- MAGIC </br><img src="../images/dlt.1.png" width="600"/>
-- MAGIC </br><img src="../images/dlt.4.png" width="600"/>  
-- MAGIC 1. **ストレージオプション**は `Unity Catalog` を選択し、ラボで利用している `カタログ` と `スキーマ` を選択します。
-- MAGIC 1. **クラスターポリシー**は `なし` を選択し下記の 3 つを設定します。
-- MAGIC </br><img src="../images/dlt.5.png" width="600"/>
-- MAGIC    * **クラスターモード**は `固定サイズ`を選択
-- MAGIC    * **ワーカ**は `1` を入力
-- MAGIC    * **Photonアクセラレータを使用**に `チェック`します。  
-- MAGIC 1. （任意）**通知**で`設定を追加`を押下し下記の 3 つを設定します。
-- MAGIC    * **メール（カンマ区切り）**にメールアドレスを入力
-- MAGIC    * **更新時**をすべてチェック
-- MAGIC    * **フロー**をすべてチェック
-- MAGIC 1. **設定**で`設定を追加`を押下し**キー**に `sample.dataset` を入力し **値**に `handson.h`で定義された `sample_dataset_path`のパス文字列 を入力します。
-- MAGIC </br><img src="../images/dlt.6.png" width="600"/>
-- MAGIC 1. **作成**を押下します。
-- MAGIC 1. **開始**を押下します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT 定義

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Bronze Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 02_bronze_orders
-- MAGIC - Raw データに対して Auot Loader で増分読み取り

-- COMMAND ----------

USE CATALOG your_catalog;
USE SCHEMA your_schema;

CREATE OR REFRESH STREAMING TABLE 02_bronze_orders -- ストリームテーブル（増分取り込みテーブル）
COMMENT "The raw books orders, ingested from orders-raw" -- コメント
AS SELECT * FROM read_files( -- Auto Loader 利用宣言（増分識別の機能有効化）
                             "${sample.dataset}/orders-json-raw", -- 入力元
                             "json", -- Format 指定
                             map("cloudFiles.inferColumnTypes", "true")); -- スキーマ推論の有効化

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 02_lookup_customers
-- MAGIC - Bronze エンリッチ用のマスターデータの定義

-- COMMAND ----------

USE CATALOG your_catalog;
USE SCHEMA your_schema;

CREATE OR REFRESH MATERIALIZED VIEW 02_lookup_customers -- マテリアライズドビュー（毎回洗い替え）
COMMENT "The customers lookup table, ingested from customers-json" -- コメント
AS SELECT * FROM read_files(
  "${sample.dataset}/customers-json",
  "json"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ### 2. Silver Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 02_silver_orders
-- MAGIC
-- MAGIC - 入力元が Delta テーブルであるため Auto Loader 利用（増分識別） や スキーマ推論 は不要
-- MAGIC - Silver 向けのデータ加工として顧客マスターのユーザー情報を付与
-- MAGIC - 入力データに対する品質チェックを導入しデータ品質を担保
-- MAGIC </br><img src="https://learn.microsoft.com/ja-jp/azure/databricks/_static/images/delta-live-tables/expectations/expectations-flow-graph.png" width="1000"/>
-- MAGIC | アクション     | SQL 構文                             | Python 構文       | 結果                                                                                   |
-- MAGIC |----------------|--------------------------------------|-------------------|----------------------------------------------------------------------------------------|
-- MAGIC | warn (default) | EXPECT                               | dlt.expect        | 無効なレコードがターゲットに書き込まれます。有効なレコードと無効なレコードの数は、他のデータセットメトリクスとともに記録されます。 |
-- MAGIC | drop         | EXPECT ... ON VIOLATION DROP ROW     | dlt.expect_or_drop| 無効なレコードは、データがターゲットに書き込まれる前に削除されます。ドロップされたレコードの数は、他のデータセットメトリクスとともにログに記録されます。 |
-- MAGIC | fail       | EXPECT ... ON VIOLATION FAIL UPDATE  | dlt.expect_or_fail| レコードが無効な場合、更新は成功しません。再処理の前に手動による介入が必要です。この予想により、1 つのフローが失敗し、パイプライン内の他のフローが失敗することはありません。 |

-- COMMAND ----------

USE CATALOG your_catalog;
USE SCHEMA your_schema;

CREATE OR REFRESH STREAMING TABLE 02_silver_orders ( -- ストリーム Read（増分取り込みを宣言）
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW -- 品質制約定義
)
COMMENT "The cleaned books orders with valid order_id" -- コメント
AS
  -- Silver 向けのデータ加工として顧客マスターのユーザー情報を付与(Orders Bronze ストリーム と Customers 静的マスターテーブルの JOIN)
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
  FROM STREAM(02_bronze_orders) o
  LEFT JOIN 02_lookup_customers c
    ON o.customer_id = c.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ### 3. Gold Table
-- MAGIC
-- MAGIC - 入力元が Delta テーブルであるため Auto Loader 利用（増分識別） や スキーマ推論 は不要
-- MAGIC - Gold 向けデータ加工として分析用集計処理を実施
-- MAGIC - 集計処理は国別に行い国別に Gold テーブルを作成

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 02_gold_cn_daily_customer_books

-- COMMAND ----------

USE CATALOG your_catalog;
USE SCHEMA your_schema;

CREATE OR REFRESH MATERIALIZED VIEW 02_gold_cn_daily_customer_books -- マテリアライズドビュー（毎回洗い替え）
COMMENT "Daily number of books per customer in China" -- コメント
AS
  -- Orders Gold テーブル用のデータ加工（分析用の集計処理）
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.02_silver_orders
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 02_gold_fr_daily_customer_books

-- COMMAND ----------

USE CATALOG your_catalog;
USE SCHEMA your_schema;

CREATE OR REFRESH MATERIALIZED VIEW 02_gold_fr_daily_customer_books -- マテリアライズドビュー（毎回洗い替え）
COMMENT "Daily number of books per customer in France" -- コメント
AS
  -- Orders Gold テーブル用のデータ加工（分析用の集計処理）
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM 02_silver_orders
  WHERE country = "France"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
