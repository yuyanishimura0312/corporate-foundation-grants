# 過去公募・採択者情報収集プラン

## 目的
研究助成財団の過去採択者・受賞者情報を構造化し、研究者が「どの財団に応募すれば成果が出やすいか」を判断できる基盤を提供する。さらに、採択者ネットワーク分析（所属機関集中度・分野配分・性別比・若手比率）の素材とする。

## スキーマ（既存）
Migration 002で `grant_results` テーブルが追加済み。

```sql
CREATE TABLE grant_results (
    id TEXT PRIMARY KEY,
    call_id TEXT REFERENCES grant_calls(id),
    fiscal_year INTEGER,
    awardee_name TEXT,
    awardee_affiliation TEXT,
    awardee_career_stage TEXT,    -- 'PhD candidate' | 'postdoc' | 'PI' | 'senior'
    project_title TEXT,
    project_keywords TEXT,         -- JSON array
    award_amount INTEGER,
    award_period_months INTEGER,
    awardee_gender TEXT,           -- 任意（公開情報のみ）
    metadata TEXT,
    created_at TEXT
);
```

## 優先度別収集対象

### Priority 1（着手必須・JFC top12以内・公開採択リストあり）
| 財団 | URL | フォーマット | 推定件数/年 |
|--|--|--|--|
| 武田科学振興財団 | takeda-sci.or.jp/business/award.php | PDF + HTML | 80-120 |
| 三菱財団 | mitsubishi-zaidan.jp/grants/results/ | PDF | 60-80 |
| 稲盛財団 | inamori-f.or.jp/research_grant/results/ | HTML | 50-80 |
| 旭硝子財団 | af-info.or.jp/research/result.html | HTML | 40-60 |
| 住友財団 | sumitomo.or.jp/result.html | PDF | 40-60 |

### Priority 2（5-10億円規模・部分公開）
| 財団 | URL |
|--|--|
| トヨタ財団 | toyotafound.or.jp/grant_results/ |
| セコム科学技術振興財団 | secomzaidan.jp/result.html |
| 鹿島学術振興財団 | kajima-f.or.jp/grant-projects/research-grant/result/ |
| 上原記念生命科学財団 | ueharazaidan.or.jp/result/ |
| 中谷医工計測技術振興財団 | nakatani-foundation.jp/result.html |

### Priority 3（中規模・実装次第）
- テルモ生命科学振興財団
- 旭硝子財団（人社系）
- 村田学術振興財団
- 同仁化学学術振興財団
- 持田記念医学薬学振興財団

## 技術構成

```
scripts/
├── scrape_awardees.py      # オーケストレータ（実装済み・スカフォールド）
├── parsers/
│   ├── __init__.py
│   ├── takeda.py           # 武田科学振興財団
│   ├── mitsubishi.py       # 三菱財団
│   ├── inamori.py          # 稲盛財団
│   ├── asahi_glass.py      # 旭硝子財団
│   ├── sumitomo.py         # 住友財団
│   └── ...
├── lib/
│   ├── http.py             # ETag/Last-Modified付きHTTPクライアント
│   ├── pdf.py              # pdfplumberラッパー
│   ├── normalize.py        # 機関名正規化
│   └── upsert.py           # grant_results upsert
└── cache/
    └── awardees/<slug>/<fiscal_year>.html or .pdf
```

## パーサ実装テンプレート

```python
# parsers/takeda.py
from typing import List, Dict
from ..lib.http import fetch
from ..lib.pdf import extract_text

def parse() -> List[Dict]:
    """Return list of awardee records."""
    html = fetch("https://www.takeda-sci.or.jp/business/award.php")
    pdf_links = extract_pdf_links(html)
    results = []
    for url in pdf_links:
        text = extract_text(url)
        records = parse_takeda_pdf(text)  # foundation-specific
        results.extend(records)
    return results
```

## 法的・倫理的留意点
- 公開情報のみ（採択結果ページは公益法人として公開義務あり）
- robots.txtを尊重、レート制限（1req/3sec）
- 個人情報（性別・年齢）は財団が公開している範囲のみ
- スクレイピングしたPDFはローカルキャッシュのみ、再配布しない

## 段階実装ロードマップ
- **Week 1**: lib/http, lib/pdf, lib/upsert基盤実装
- **Week 2**: takeda + mitsubishi + inamori（Priority 1の3団体）
- **Week 3**: 旭硝子 + 住友（Priority 1完了）→ 推定250-400件採択者
- **Week 4-5**: Priority 2（5団体追加）→ 推定+300件
- **Month 2**: Priority 3（5団体）→ 推定+200件、合計800-900件採択者
- **Month 3**: 採択者ネットワーク分析ダッシュボード追加（所属機関集中度・分野配分）
