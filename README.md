# Corporate Foundation Research Grants Database

企業財団による研究助成に特化した分析用データベース。  
メインのGrant DB（3,669件）から、企業が設立した財団の研究助成プログラムのみを抽出・構造化。

## Data Summary

| Item | Count |
|------|-------|
| Corporate foundations | 82 |
| Research grant programs | 258 |
| Grant calls | 258 |
| Eligibility criteria | 132 |
| Required documents | 193 |
| Document sections | 464 |
| Evaluation criteria | 161 |
| Budget categories | 207 |
| Source PDFs | 170 |

## Key Tables

- **organizations** — 企業財団（`corporate_parent` で親企業を推定）
- **grant_programs** — 研究助成プログラム
- **grant_calls** — 個別の公募（金額・締切・ステータス）
- **eligibility_criteria** — 応募資格
- **required_documents / document_sections** — 申請書類の構造
- **evaluation_criteria** — 審査基準
- **budget_categories** — 助成対象経費

## Analysis Views

- `v_grants_overview` — 全公募の一覧（財団名・プログラム・金額・締切）
- `v_foundation_summary` — 財団ごとの集計（プログラム数・最大金額等）
- `v_amount_ranking` — 助成額ランキング

## Usage

```bash
# Summary
sqlite3 -header -column corporate_research_grants.sqlite "SELECT * FROM v_foundation_summary;"

# Open/upcoming grants
sqlite3 -header -column corporate_research_grants.sqlite \
  "SELECT * FROM v_grants_overview WHERE status IN ('open','upcoming') ORDER BY application_deadline;"

# Grants by parent company
sqlite3 -header -column corporate_research_grants.sqlite \
  "SELECT * FROM v_grants_overview WHERE foundation_name LIKE '%トヨタ%';"
```

## Build

```bash
python3 build_db.py
```

Source: `~/projects/apps/grant-db/grant_db.sqlite`

## Data Source

Grant DB (https://grant-db-seven.vercel.app/) — jGrants API + 助成財団センター + CANPAN + 省庁公募
