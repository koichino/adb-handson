-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Change Data Capture in SDP

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## はじめに
-- MAGIC
-- MAGIC このラボはノートブック内のセルを順次実行していくようなインタラクティブな形式ではなく、以下の「SDP パイプライン設定と実行」の手順に従って DLT ジョブを構成＆実行します。
-- MAGIC
-- MAGIC このノートブックでは SDP における CDC ソースからの継続マージロードのサンプルコードを示します。
-- MAGIC
-- MAGIC **CDC ソース**
-- MAGIC | userid | name     | city        | operation | sequenceNum |
-- MAGIC |--------|----------|-------------|-----------|-------------|
-- MAGIC | 124    | Raul     | Oaxaca      | INSERT    | 1           |
-- MAGIC | 123    | Isabel   | Monterrey   | INSERT    | 1           |
-- MAGIC | 125    | Mercedes | Tijuana     | INSERT    | 2           |
-- MAGIC | 126    | Lily     | Cancun      | INSERT    | 2           |
-- MAGIC | 123    | null     | null        | DELETE    | 6           |
-- MAGIC | 125    | Mercedes | Guadalajara | UPDATE    | 6           |
-- MAGIC | 125    | Mercedes | Mexicali    | UPDATE    | 5           |
-- MAGIC | 123    | Isabel   | Chihuahua   | UPDATE    | 5           |
-- MAGIC
-- MAGIC **① SCD Type-1(更新履歴を表現しない) によるマージ**
-- MAGIC
-- MAGIC **_SQL_**  
-- MAGIC ```SQL
-- MAGIC CREATE OR REFRESH STREAMING TABLE target;
-- MAGIC
-- MAGIC APPLY CHANGES INTO live.target
-- MAGIC FROM stream(cdc_data.users)
-- MAGIC KEYS (userId)
-- MAGIC APPLY AS DELETE WHEN
-- MAGIC   operation = "DELETE"
-- MAGIC SEQUENCE BY sequenceNum
-- MAGIC COLUMNS * EXCEPT (operation, sequenceNum)
-- MAGIC STORED AS SCD TYPE 1;
-- MAGIC ```
-- MAGIC
-- MAGIC **_結果_**
-- MAGIC | userId | name     | city        |
-- MAGIC |--------|----------|-------------|
-- MAGIC | 124    | Raul     | Oaxaca      |
-- MAGIC | 125    | Mercedes | Guadalajara |
-- MAGIC | 126    | Lily     | Cancun      |
-- MAGIC
-- MAGIC **② SCD Type-2(更新履歴を表現する) によるマージ**
-- MAGIC
-- MAGIC **_SQL_** 
-- MAGIC ```SQL
-- MAGIC CREATE OR REFRESH STREAMING TABLE target;
-- MAGIC
-- MAGIC APPLY CHANGES INTO live.target
-- MAGIC FROM stream(cdc_data.users)
-- MAGIC KEYS (userId)
-- MAGIC APPLY AS DELETE WHEN
-- MAGIC   operation = "DELETE"
-- MAGIC SEQUENCE BY sequenceNum
-- MAGIC COLUMNS * EXCEPT (operation, sequenceNum)
-- MAGIC STORED AS SCD TYPE 2;
-- MAGIC ```
-- MAGIC
-- MAGIC **_結果_**
-- MAGIC | userId | name     | city       | __START_AT | __END_AT |
-- MAGIC |--------|----------|------------|------------|----------|
-- MAGIC | 123    | Isabel   | Monterrey  | 1          | 5        |
-- MAGIC | 123    | Isabel   | Chihuahua  | 5          | 6        |
-- MAGIC | 124    | Raul     | Oaxaca     | 1          | null     |
-- MAGIC | 125    | Mercedes | Tijuana    | 2          | 5        |
-- MAGIC | 125    | Mercedes | Mexicali   | 5          | 6        |
-- MAGIC | 125    | Mercedes | Guadalajara| 6          | null     |
-- MAGIC | 126    | Lily     | Cancun     | 2          | null     |
-- MAGIC
-- MAGIC **参考**
-- MAGIC - [変更データ キャプチャ (CDC) とは](https://learn.microsoft.com/ja-jp/azure/databricks/delta-live-tables/what-is-change-data-capture)
-- MAGIC - [APPLY CHANGES API: Delta Live Tables を使用した変更データ キャプチャの簡略化](https://learn.microsoft.com/ja-jp/azure/databricks/delta-live-tables/cdc)
-- MAGIC
-- MAGIC それでは実際に試してみましょう。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## DLT パイプライン設定
-- MAGIC
-- MAGIC 1. サイドバーの **パイプライン** をクリックします。
-- MAGIC 1. 前のラボで作成したパイプラインを選択します。
-- MAGIC 1. `設定`を押下しパイプライン編集画面に遷移します。
-- MAGIC 1. **ソースコード**で`ソースコードを追加`を押下しナビゲーターを使いこのノートブック（`03_Change Data Capture in DLT`）選択します。
-- MAGIC </br><img src="../images/dlt.7.png" width="800"/>
-- MAGIC 1. **保存**を押下します。
-- MAGIC 1. **開始**を押下します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Bronze Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03_bronze_books

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE 03_bronze_books -- ストリーム Read（増分取り込みを宣言）
COMMENT "The raw books data, ingested from CDC feed" -- コメント
AS SELECT * FROM cloud_files( -- Auto Loader 利用宣言（増分識別の機能有効化）
                             "${sample.dataset}/books-cdc", -- 入力元
                             "json") -- Foramat 指定

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 入力元となる CDC ソースは以下のデータです。
-- MAGIC </br><img src="../images/cdc.1.png" width="1200"/>
-- MAGIC マージを行うために必要な以下の情報を持っていることが確認できます。
-- MAGIC - 一意キー（book_id） ← スキーマ定義でなければわかりませんがここでは book_id が一意キーであることが前提です
-- MAGIC - 時系列を判定するためのシーケンス（row_time）
-- MAGIC - キーで示されるレコードに対して行われた操作（row_status）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Silver Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03_silver_books

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE 03_silver_books ( -- ストリーム Read（増分取り込みを宣言）
  CONSTRAINT valid_book_number EXPECT (book_id IS NOT NULL) ON VIOLATION DROP ROW -- 品質制約定義
);
-- CDF テーブルの増分データに対してロジックで差分処理を判定しターゲットテーブルを更新
APPLY CHANGES INTO LIVE.03_silver_books -- ターゲットテーブル
  FROM STREAM(LIVE.03_bronze_books) -- CDF テーブル
  KEYS (book_id) -- 比較キー
  APPLY AS DELETE WHEN row_status = "DELETE"  -- 増分データの条件に応じたターゲットへの振る舞いを定義
  SEQUENCE BY row_time -- トランザクションの順番判定キー
  COLUMNS * EXCEPT (row_status, row_time) -- ターゲットへ出力する列の指定（* EXCEPT で出力しない列指定も可能）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Gold Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03_gold_author_counts_state

-- COMMAND ----------

CREATE LIVE TABLE 03_gold_author_counts_state -- マテリアライズドビュー（毎回洗い替え）
  COMMENT "Number of books per author" -- コメント
AS
  -- Books Gold テーブル用のデータ加工（分析用の集計処理）
   SELECT author, count(*) as books_count, current_timestamp() updated_time
  FROM LIVE.03_silver_books
  GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Option. DLT View

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03_books_sales_tmp

-- COMMAND ----------

CREATE LIVE VIEW 03_books_sales_tmp  -- 通常ビュー
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(books) AS book 
      FROM LIVE.02_silver_orders) o
    INNER JOIN LIVE.03_silver_books b
    ON o.book.book_id = b.book_id;
