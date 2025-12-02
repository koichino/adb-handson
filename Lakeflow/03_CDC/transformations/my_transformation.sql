-- 1. Bronze Table
-- 03_bronze_books
CREATE OR REFRESH STREAMING TABLE 03_bronze_books -- ストリーム Read（増分取り込みを宣言）
COMMENT "The raw books data, ingested from CDC feed" -- コメント
AS SELECT * FROM 
 STREAM read_files( -- Auto Loader 利用宣言（増分識別の機能有効化）
"${sample.dataset}/books-cdc", -- 入力元
format => "json") -- Foramat 指定
;

--2. Silver Table
-- 03_silver_books
CREATE OR REFRESH STREAMING TABLE 03_silver_books ( -- ストリーミングテーブル定義
  CONSTRAINT valid_book_number EXPECT (book_id IS NOT NULL) ON VIOLATION DROP ROW -- 品質制約定義
);

-- AUTO CDC API を利用した差分適用
CREATE FLOW books_cdc_flow AS AUTO CDC INTO 03_silver_books -- ターゲットテーブル
FROM STREAM(03_bronze_books) -- CDF テーブル
KEYS (book_id) -- 比較キー
APPLY AS DELETE WHEN row_status = "DELETE"  -- 増分データの条件に応じたターゲットへの振る舞いを定義
SEQUENCE BY row_time -- トランザクションの順番判定キー
COLUMNS * EXCEPT (row_status, row_time) -- ターゲットへ出力する列の指定（* EXCEPT で出力しない列指定も可能）
;

-- 3. Gold Table
-- 03_gold_author_counts_state
CREATE MATERIALIZED VIEW 03_gold_author_counts_state -- マテリアライズドビュー（毎回洗い替え）
  COMMENT "Number of books per author" -- コメント
AS
  -- Books Gold テーブル用のデータ加工（分析用の集計処理）
   SELECT author, count(*) as books_count, current_timestamp() updated_time
  FROM 03_silver_books
  GROUP BY author
;

-- Option. DLT View
--03_books_sales_tmp
CREATE VIEW 03_books_sales_tmp  -- 通常ビュー
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(books) AS book 
      FROM 02_silver_orders) o
    INNER JOIN 03_silver_books b
    ON o.book.book_id = b.book_id;