# amenomi-open-dwh

日本全国の公開データを、共通の軸（市区町村・法人・暦）でつないで引けるようにする DWH。

- 取り込み（EL）: Python（`python -m aod`）
- 変換（T）: dbt（dbt-duckdb）
- 最初の対象: 市区町村の廃置分合（1970〜）と国勢調査の市区町村別人口（1980〜2020）

進め方は [issues](https://github.com/dkurokawa/amenomi-open-dwh/issues) を参照。

## 使い方（廃置分合）

```
uv sync
uv run python -m aod haichi fetch   # e-Stat の「廃置分合等情報」CSV を bronze に置く
uv run python -m aod haichi build   # bronze の最新 1 本から silver の Parquet 3 本を作る
uv run pytest                        # テスト（ネットワークには触れない）
```

appId は要らない。キャッシュの置き場は環境変数 `AOD_CACHE`（既定 `~/.cache/amenomi-open-dwh`）。

- `fetch`: 直前の原本と sha256 が同じなら何も置かず、台帳に `skipped_nochange` を書く。原本は上書きしない。HTML のエラーページやヘッダが違う CSV は置かずに止まる
- `build`: 検査に落ちたら終了コード 1 で silver を書かない。落ちた項目は全部出す。検査は 1 ヘッダ、2 事由パターン、3 transitions を作れない event、4 市町村の単位の分割（同じ施行日に 1 つの from_code が自分以外の 2 つ以上の to_code に行くのは、全部 `partial=true` のときだけ。`partial=true` の行は行き先が 2 つ以上必要）、5 コードが 5 桁、6 行数が前回を下回らない、7 区に編入された町（ward_* 以外で、区でないコードから区へ行く行）に市町村の単位の行がある
- bronze: `$AOD_CACHE/bronze/estat/haichi/fetched=<日付>/haichi_<sha256 先頭 12>.csv`（`_manifest.jsonl` に 1 行ずつ記録）
- 実行台帳: `$AOD_CACHE/_ledger/runs.jsonl`（1 行 = 1 実行。失敗した実行も書く）

### silver（`$AOD_CACHE/silver/haichi/`）

| ファイル | 1 行の単位 | 主な列 |
|---|---|---|
| `haichi_rows.parquet` | CSV の 1 行 | 元の 11 列（文字列のまま）、`row_uid`（`<sha12>#<データ行の通し番号>`）、`source_key` |
| `haichi_events.parquet` | 1 つの廃置分合 | `event_id`、`effective_date`、`pattern`、`reason_text`、`notice_date`、`notice_no`、`codes` |
| `haichi_transitions.parquet` | 旧コード → 新コード | `event_id`、`effective_date`、`pattern`、`kind`、`grain`、`partial`、`from_code`、`from_name`、`to_code`、`to_name` |

- event は「施行日・事由パターン・改正事由の 1 行目」で 1 つにまとめる。`event_id` はその 3 つの sha256 先頭 16 文字
- `kind`: `absorb`（編入）/ `merge_new_code` / `merge_reuse_code`（コード追加なしの新設合併。自分自身への行も作る）/ `status_change`（市制・町制施行）/ `rename` / `recode`（郡・支庁の区域変更）/ `designated_city` / `ward_create` / `ward_separate` / `ward_split` / `ward_reorg`
- `grain`: from か to が政令市の区なら `ward`、それ以外は `municipality`
- `partial`: 元のコードの一部（大字）だけがその行き先に動く行。名前の直後に「大字…が」が続くコードから出る行が `true`。1970 年以降で市町村の単位の 1 → 多はこれだけ（2006-03-01 の上九一色村 19341 → 甲府市 19201・富士河口湖町 19430。2 つの編入合併の event に分かれて載っている）。按分の重みはここでは持たない
- 区に編入された町（早良町・京北町・湯来町・蒲原町・由比町）は、`grain=ward` の行（町 → 区）に加えて、`grain=municipality` の行（町 → その政令市のコード）も作る。政令市は「政令市･郡…」列と都道府県から引く
- 境界変更・都市種別変更は events には残るが、transitions は作らない
- 浜松市の区の再編は、全体をまとめた event（「７区から３区へ再編」）が、3 つの個別の event の ward_reorg の組を重ねて持つ（既知。grain は ward だけ）
- `ward_create` の `from_code` は、政令指定都市施行と同じ文の下に付く区なら旧市のコード、区だけの新設なら政令市のコード
