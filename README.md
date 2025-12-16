# 事前準備
事務局の方はラボの事前に以下の準備を行ってください。
- Databricks ワークスペース
- Databricks カタログ
- ラボユーザーへの権限割り当て
- Power BI Desktop のインストール
- ラボ受講者への案内

## Databricks ワークスペース
Unity Catalog が有効化された Azure Databricks ワークスペースを用意してください（現在 Unity Catalog は既定有効です）。

- 参考：[はじめに: アカウントとワークスペースのセットアップ - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/getting-started)

## Databricks カタログ
ハンズオン参加者間で共有する標準カタログを用意してください。

- 参考：[カタログを作成する - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/catalogs/create-catalog)

留意：メタストアにストレージがアタッチされていない構成の場合はカタログ作成の前にマネージドストレージを用意する必要があります。

- [Unity カタログを使用してクラウド ストレージへのアクセスを管理する - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/connect/unity-catalog/cloud-storage/)

任意：既存メタストアとは別のリージョンにハンズオン専用のメタストアを作成する場合は以下を参考にしてください。

- [Unity Catalog メタストアを作成する - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/data-governance/unity-catalog/create-metastore)

## ラボユーザーへの権限割り当て
一般ユーザーでハンズオンを行う場合は一般ユーザー既定の権限に対して以下の権限を追加してください。
- カタログに対する **`USE CATALOG`**
- カタログに対する **`CREATE SCHEMA`**
- カタログに対する **`CREATE VOLUME`**
</br><img src="images/readme.1.png" width="600"/>
- 外部ロケーションに対する **`READ FILES`**
</br><img src="images/readme.2.png" width="600"/>
- クラスタに対する **`CREATE CLUSTER`** （ユーザー管理画面で`Unrestricted cluster creation`にチェック）
</br><img src="images/readme.3.png" width="600"/>
- (サーバーレス環境をご利用の場合)： 以下のコマンドを実行し、ユーザまたはグループに対して ANY FILE への SELECT 権限を付与
    ```SQL
    %sql
    GRANT SELECT ON ANY FILE TO `<user@domain-name>` | `<Group名>`
    ```

## ラボ受講者への案内
以下の情報を受講者へ案内してください。
- Databricks ワークスペースの URL
- Unity Catalog カタログ名
- ラボコンテンツの Repo アドレス
  * https://github.com/koichino/adb-handson/blob/main/01_Setup.ipynb

# Next Action:
ラボ当日を迎えましょう！ラボ受講者は<a href="$./01_Setup">こちらのノートブック</a>からスタートします。