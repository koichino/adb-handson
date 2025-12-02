CREATE OR REFRESH STREAMING TABLE 02_bronze_orders -- ストリームテーブル（増分取り込みテーブル）
COMMENT "The raw books orders, ingested from orders-raw" -- コメント
AS SELECT * FROM STREAM read_files(
  "${sample.dataset}/orders-json-raw", -- 入力元
  format => "json", -- Format 指定
  inferColumnTypes => "true" -- スキーマ推論の有効化
);

CREATE OR REFRESH MATERIALIZED VIEW 02_lookup_customers -- マテリアライズドビュー（毎回洗い替え）
COMMENT "The customers lookup table, ingested from customers-json" -- コメント
AS SELECT * FROM read_files(
  "${sample.dataset}/customers-json",
  format => "json"
);

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
;

CREATE OR REFRESH MATERIALIZED VIEW 02_gold_cn_daily_customer_books -- マテリアライズドビュー（毎回洗い替え）
COMMENT "Daily number of books per customer in China" -- コメント
AS
  -- Orders Gold テーブル用のデータ加工（分析用の集計処理）
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM 02_silver_orders
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
;


CREATE OR REFRESH MATERIALIZED VIEW 02_gold_fr_daily_customer_books -- マテリアライズドビュー（毎回洗い替え）
COMMENT "Daily number of books per customer in France" -- コメント
AS
  -- Orders Gold テーブル用のデータ加工（分析用の集計処理）
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM 02_silver_orders
  WHERE country = "France"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)