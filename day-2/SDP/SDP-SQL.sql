-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE 02_bronze_orders -- ストリームテーブル（増分取り込みテーブル）
COMMENT "The raw books orders, ingested from orders-raw" -- コメント
AS SELECT * FROM read_files( -- Auto Loader 利用宣言（増分識別の機能有効化）
                             "${sample.dataset}/orders-json-raw", -- 入力元
                             "json", -- Format 指定
                             map("cloudFiles.inferColumnTypes", "true")); -- スキーマ推論の有効化