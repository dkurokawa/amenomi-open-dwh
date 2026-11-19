# amenomi-open-dwh

日本全国の公開データを、共通の軸（市区町村・法人・暦）でつないで引けるようにする DWH。

- 取り込み（EL）: Python（`python -m aod`）
- 変換（T）: dbt（dbt-duckdb）
- 最初の対象: 市区町村の廃置分合（1970〜）と国勢調査の市区町村別人口（1980〜2020）

進め方は [issues](https://github.com/dkurokawa/amenomi-open-dwh/issues) を参照。
