#!/usr/bin/env python3
"""
未来洞察分析: Foresight KBと企業財団助成金の整合性分析
"""
import json
import sqlite3
from collections import defaultdict

DB_PATH = "/Users/nishimura+/projects/research/foresight-knowledge-base/foresight.db"
GRANT_JSON = "/Users/nishimura+/projects/research/corporate-foundation-grants/field_analysis.json"
OUTPUT_PATH = "/Users/nishimura+/projects/research/corporate-foundation-grants/data/analysis_foresight.json"

def get_academic_fields(domains, name):
    """ドメインリストから関連学術分野を推定"""
    field_map = {
        'technology': '情報工学・コンピュータ科学、電気電子工学',
        'science': '自然科学全般（物理学・化学・生物学）',
        'health': '医学・公衆衛生・生命科学',
        'environment': '環境科学・生態学・地球科学',
        'economy': '経済学・経営学・金融工学',
        'society': '社会学・社会福祉学・人口学',
        'governance': '政治学・法学・行政学',
        'geopolitics': '国際関係・地政学・安全保障',
        'energy': '工学・化学工学・物理学',
        'food': '農学・食品科学・生態学',
        'education': '教育学・認知科学',
    }
    fields = set()
    for d in domains:
        if d in field_map:
            fields.add(field_map[d])
    name_lower = name.lower()
    if 'ai' in name_lower or 'artificial' in name_lower or 'digital' in name_lower:
        fields.add('情報工学・AI研究')
    if 'bio' in name_lower or 'gene' in name_lower or 'synthetic' in name_lower:
        fields.add('生命科学・バイオテクノロジー')
    if 'quantum' in name_lower:
        fields.add('量子物理学・量子情報科学')
    if 'neuro' in name_lower or 'brain' in name_lower or 'cognitive' in name_lower:
        fields.add('神経科学・認知科学')
    return list(fields)[:3]

def get_rationale(code, r):
    """機会分野の理由説明"""
    rationales = {
        '0401': '気候変動は全シナリオで最重要課題だが環境科学への助成実績がほぼゼロ',
        '0402': '生物多様性損失は惑星規模リスクだが生態学助成は実績なし',
        '0301': '再生可能エネルギーは全エネルギーシナリオで必須だが工学系助成に含まれていない',
        '0601': 'パンデミックは高信頼度予測上位だが公衆衛生助成は非常に少ない',
        '0204': '神経科学・BCIは2035年以降の主要技術だが助成が極めて少ない',
        '0302': 'エネルギー貯蔵・水素は脱炭素の鍵だが化学工学助成は微小',
        '0801': '人口動態変化は全予測に通底する基盤要因だが社会学助成は実績なし',
        '0603': '高齢化は確実なメガトレンドだが高齢者福祉助成は少額',
        '0604': 'メンタルヘルスは全シナリオで社会課題として台頭するが助成少額',
        '1001': '民主主義の危機は多数シナリオで言及されるが政治学助成なし',
        '0103': '量子技術は安全保障・経済で game changer だが助成は基礎物理学留まり',
        '0104': 'サイバーセキュリティは全シナリオで重要インフラ課題だが専門助成なし',
    }
    return rationales.get(code, f'未来重要度(スコア{r["foresight_score_normalized"]})に対し助成シェア({r["current_grant_share_pct"]}%)が低い')

def get_recommended_increase(r):
    """推奨増加率"""
    if r['current_grant_yen'] == 0:
        return '新規設立を推奨'
    elif r['gap_score'] > 80:
        return '500%以上の増額'
    elif r['gap_score'] > 50:
        return '200-300%の増額'
    elif r['gap_score'] > 20:
        return '50-100%の増額'
    else:
        return '現状維持〜20%増額'

def main():
    # DB接続
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1. 2030-2040予測 L2テーマ別集計
    cur.execute("""
    SELECT tt.code, tt.name_en, tt.name_ja,
           COUNT(pt.prediction_id) as total,
           SUM(CASE WHEN p.confidence_level='high' THEN 1 ELSE 0 END) as high_conf,
           SUM(CASE WHEN p.confidence_level='medium' THEN 1 ELSE 0 END) as med_conf
    FROM theme_taxonomy tt
    JOIN prediction_themes pt ON tt.id = pt.theme_id
    JOIN predictions p ON pt.prediction_id = p.id
    WHERE tt.level = 2
      AND p.time_horizon_year BETWEEN 2030 AND 2040
    GROUP BY tt.code, tt.name_en, tt.name_ja
    ORDER BY high_conf DESC, total DESC
    """)
    l2_predictions = {r['code']: dict(r) for r in cur.fetchall()}

    # 2. メガトレンド・新興トレンド
    cur.execute("""
    SELECT name, name_ja, trend_type, domains, mention_count, vesteg_category
    FROM trends
    WHERE trend_type IN ('megatrend', 'emerging_trend')
    ORDER BY mention_count DESC
    LIMIT 40
    """)
    all_trends = [dict(r) for r in cur.fetchall()]

    # 3. シナリオ（探索型・予測型）
    cur.execute("""
    SELECT name, name_ja, scenario_type, key_assumptions, description
    FROM scenarios
    WHERE scenario_type IN ('exploratory', 'backcasting', 'predictive')
      AND key_assumptions IS NOT NULL AND key_assumptions != '' AND key_assumptions != '[]'
      AND length(key_assumptions) > 50
    """)
    scenarios = [dict(r) for r in cur.fetchall()]

    conn.close()

    # === シナリオ横断ロバスト分析 ===
    l2_keyword_mapping = {
        '0101': ['AI', 'artificial intelligence', 'AGI', 'generative AI', 'machine learning', 'LLM'],
        '0102': ['digital infrastructure', '5G', '6G', 'cloud computing', 'internet of things'],
        '0103': ['quantum'],
        '0104': ['cyber', 'cybersecurity', 'cryptography'],
        '0105': ['robot', 'automation', 'autonomous systems', 'autonomous driving'],
        '0201': ['CRISPR', 'genetic', 'genome', 'gene editing'],
        '0202': ['synthetic biology', 'biotech', 'bio-digital'],
        '0204': ['BCI', 'brain-computer', 'neuroscience', 'cognitive enhancement'],
        '0301': ['renewable energy', 'solar', 'wind energy'],
        '0302': ['hydrogen', 'energy storage', 'battery storage'],
        '0303': ['nuclear power', 'nuclear energy'],
        '0401': ['climate', 'emissions', 'carbon', 'net-zero', 'net zero', 'temperature'],
        '0402': ['biodiversity', 'ecosystem', 'nature-based'],
        '0501': ['food security', 'food system', 'agriculture', 'protein'],
        '0601': ['pandemic', 'infectious disease', 'biosecurity', 'pathogen'],
        '0602': ['digital health', 'health data', 'telemedicine'],
        '0603': ['aging', 'longevity', 'elderly'],
        '0604': ['mental health'],
        '0701': ['GDP', 'macroeconomic', 'economic growth', 'inflation'],
        '0702': ['finance', 'fintech', 'cryptocurrency', 'CBDC', 'DeFi', 'capital'],
        '0704': ['green finance', 'ESG', 'sustainable finance', 'climate finance'],
        '0801': ['demographics', 'population', 'urbanization'],
        '0804': ['inequality', 'poverty', 'social fragmentation'],
        '0901': ['future of work', 'job displacement', 'labor market'],
        '0902': ['skills', 'talent', 'workforce'],
        '0903': ['education', 'learning', 'school'],
        '1001': ['democracy', 'democratic institutions', 'democratic'],
        '1002': ['digital governance', 'algorithmic governance', 'data governance'],
        '1101': ['geopolitics', 'US-China', 'great power', 'superpower'],
        '1102': ['conflict', 'war', 'military', 'regional rivalry'],
        '1201': ['space', 'satellite', 'orbital'],
        '1401': ['AI ethics', 'technology ethics', 'alignment', 'ethical'],
    }

    scenario_field_counts = defaultdict(int)
    for scenario in scenarios:
        ka = scenario.get('key_assumptions', '') or ''
        name = scenario.get('name', '') or ''
        text = (name + ' ' + ka).lower()
        for code, keywords in l2_keyword_mapping.items():
            for kw in keywords:
                if kw.lower() in text:
                    scenario_field_counts[code] += 1
                    break

    # === 助成金データ ===
    with open(GRANT_JSON, 'r', encoding='utf-8') as f:
        grant_data = json.load(f)

    total_grant = sum(item['total_amount'] for item in grant_data.get('l1_distribution', []))

    # フォーサイトL2コードと現在の助成金マッピング
    foresight_grant_map = {
        '0101': {'field_ja': 'AI・機械学習', 'current_grant': 1086000000, 'grant_fields': ['情報工学・コンピュータ科学']},
        '0102': {'field_ja': 'デジタルインフラ', 'current_grant': 1086000000, 'grant_fields': ['情報工学・コンピュータ科学']},
        '0103': {'field_ja': '量子技術', 'current_grant': 273000000, 'grant_fields': ['物理学']},
        '0104': {'field_ja': 'サイバーセキュリティ', 'current_grant': 292000000, 'grant_fields': ['情報工学（ネットワーク）']},
        '0105': {'field_ja': 'ロボティクス・自動化', 'current_grant': 716000000, 'grant_fields': ['機械工学']},
        '0201': {'field_ja': '遺伝子工学・ゲノミクス', 'current_grant': 5653000000, 'grant_fields': ['分子生物学・ゲノミクス']},
        '0202': {'field_ja': '合成生物学', 'current_grant': 5653000000, 'grant_fields': ['分子生物学・ゲノミクス']},
        '0204': {'field_ja': '神経科学・ブレインテック', 'current_grant': 100000000, 'grant_fields': ['神経科学']},
        '0301': {'field_ja': '再生可能エネルギー', 'current_grant': 0, 'grant_fields': ['環境科学（実績なし）']},
        '0302': {'field_ja': 'エネルギー貯蔵・水素', 'current_grant': 30000000, 'grant_fields': ['化学工学']},
        '0303': {'field_ja': '原子力エネルギー', 'current_grant': 273000000, 'grant_fields': ['物理学']},
        '0401': {'field_ja': '気候変動', 'current_grant': 0, 'grant_fields': ['環境科学（実績なし）']},
        '0402': {'field_ja': '生物多様性', 'current_grant': 0, 'grant_fields': ['生態学（実績なし）']},
        '0501': {'field_ja': '食料安全保障', 'current_grant': 285000000, 'grant_fields': ['農学・食品科学']},
        '0601': {'field_ja': 'パンデミック・感染症', 'current_grant': 40000000, 'grant_fields': ['公衆衛生']},
        '0602': {'field_ja': 'デジタルヘルス', 'current_grant': 30000000, 'grant_fields': ['臨床医学']},
        '0603': {'field_ja': '高齢化・長寿', 'current_grant': 113000000, 'grant_fields': ['高齢者福祉']},
        '0604': {'field_ja': 'メンタルヘルス', 'current_grant': 50000000, 'grant_fields': ['心理学']},
        '0701': {'field_ja': 'マクロ経済', 'current_grant': 920000000, 'grant_fields': ['経済学']},
        '0702': {'field_ja': '金融イノベーション', 'current_grant': 920000000, 'grant_fields': ['経済学']},
        '0704': {'field_ja': 'グリーンファイナンス', 'current_grant': 920000000, 'grant_fields': ['経済学']},
        '0801': {'field_ja': '人口動態', 'current_grant': 0, 'grant_fields': ['社会学（実績なし）']},
        '0804': {'field_ja': '格差・不平等', 'current_grant': 923000000, 'grant_fields': ['社会福祉学']},
        '0901': {'field_ja': '労働の未来', 'current_grant': 920000000, 'grant_fields': ['経済学']},
        '0902': {'field_ja': 'スキル・人材', 'current_grant': 1929000000, 'grant_fields': ['教育学']},
        '0903': {'field_ja': '教育変革', 'current_grant': 1929000000, 'grant_fields': ['教育学']},
        '1001': {'field_ja': '民主主義の未来', 'current_grant': 0, 'grant_fields': ['政治学（実績なし）']},
        '1002': {'field_ja': 'デジタルガバナンス', 'current_grant': 1086000000, 'grant_fields': ['情報工学・コンピュータ科学']},
        '1101': {'field_ja': '大国間競争', 'current_grant': 1957000000, 'grant_fields': ['国際関係']},
        '1102': {'field_ja': '地域紛争', 'current_grant': 1957000000, 'grant_fields': ['国際関係']},
        '1201': {'field_ja': '宇宙経済', 'current_grant': 0, 'grant_fields': ['(なし)']},
        '1401': {'field_ja': 'AI・テクノロジー倫理', 'current_grant': 1086000000, 'grant_fields': ['情報工学・コンピュータ科学']},
    }

    import math

    # フォーサイトスコア計算
    all_results = []
    for code, mapping in foresight_grant_map.items():
        pred_data = l2_predictions.get(code, {})
        total_pred = pred_data.get('total', 0)
        high_conf = pred_data.get('high_conf', 0)
        med_conf = pred_data.get('med_conf', 0)
        scenario_mentions = scenario_field_counts.get(code, 0)

        # フォーサイト重要度スコア（複合指標）
        foresight_score = (total_pred * 0.3 + high_conf * 2.0 + med_conf * 0.5 + scenario_mentions * 1.5)

        current_grant = mapping['current_grant']
        grant_share = current_grant / total_grant * 100 if total_grant > 0 else 0

        all_results.append({
            'code': code,
            'field_ja': mapping['field_ja'],
            'prediction_count': total_pred,
            'high_confidence_count': high_conf,
            'medium_confidence_count': med_conf,
            'scenario_mentions': scenario_mentions,
            'foresight_score': round(foresight_score, 1),
            'current_grant_yen': current_grant,
            'current_grant_share_pct': round(grant_share, 2),
            'mapped_grant_fields': mapping['grant_fields'],
        })

    all_results.sort(key=lambda x: -x['foresight_score'])

    # スコア正規化（log変換でAIの突出を緩和）
    max_score = max(r['foresight_score'] for r in all_results) or 1
    for r in all_results:
        # log1p変換で分布を平準化
        log_score = math.log1p(r['foresight_score'])
        log_max = math.log1p(max_score)
        r['foresight_score_normalized'] = round(log_score / log_max * 100, 1)

    # ギャップスコア計算
    # 助成金は絶対額でなく「総額に対するシェア」で比較（log変換）
    # grant_share_pct を log変換
    max_share = max(r['current_grant_share_pct'] for r in all_results) or 1
    for r in all_results:
        # grant share: 0%の場合は0、それ以外はlog変換
        if r['current_grant_share_pct'] > 0:
            log_grant = math.log1p(r['current_grant_share_pct'])
            log_max_grant = math.log1p(max_share)
            grant_normalized = log_grant / log_max_grant * 100
        else:
            grant_normalized = 0.0
        r['grant_normalized'] = round(grant_normalized, 1)
        # gap = foresight重要度 - 現在の助成シェア（ともに正規化済み）
        r['gap_score'] = round(r['foresight_score_normalized'] - grant_normalized, 1)

    # === 出力データ構築 ===

    # 1. emerging_fields_2030: トップ20未来重要分野
    emerging_fields_2030 = []
    for i, r in enumerate(all_results[:20]):
        emerging_fields_2030.append({
            'rank': i + 1,
            'code': r['code'],
            'field_ja': r['field_ja'],
            'foresight_score': r['foresight_score_normalized'],
            'prediction_count_2030_2040': r['prediction_count'],
            'high_confidence_predictions': r['high_confidence_count'],
            'scenario_robustness': r['scenario_mentions'],
            'current_grant_yen': r['current_grant_yen'],
            'current_grant_share_pct': r['current_grant_share_pct'],
        })

    # 2. accelerating_trends
    accelerating_trends = []
    for t in all_trends[:20]:
        try:
            domains = json.loads(t['domains']) if t['domains'] else []
        except Exception:
            domains = [t['domains']] if t['domains'] else []

        fields = get_academic_fields(domains, t['name'])
        accelerating_trends.append({
            'name': t['name'],
            'name_ja': t['name_ja'],
            'type': t['trend_type'],
            'domains': domains,
            'mention_count': t['mention_count'],
            'related_academic_fields': fields,
        })

    # 3. robust_fields: シナリオ横断で重要な分野（シナリオ言及数 >= 10）
    robust_fields = []
    robust_codes = sorted(scenario_field_counts.items(), key=lambda x: -x[1])
    for code, count in robust_codes:
        if count >= 5:
            mapping = foresight_grant_map.get(code, {})
            r_entry = next((r for r in all_results if r['code'] == code), None)
            robust_fields.append({
                'code': code,
                'field_ja': mapping.get('field_ja', code),
                'scenario_mentions': count,
                'interpretation': 'シナリオ横断で普遍的に重要',
                'foresight_score': r_entry['foresight_score_normalized'] if r_entry else 0,
                'current_grant_yen': mapping.get('current_grant', 0),
            })

    # 4. future_vs_current_gap: ギャップ分析（上位15）
    gap_results = sorted(all_results, key=lambda x: -x['gap_score'])
    future_vs_current_gap = []
    for r in gap_results[:15]:
        future_vs_current_gap.append({
            'code': r['code'],
            'field_ja': r['field_ja'],
            'foresight_importance': r['foresight_score_normalized'],
            'current_grant_yen': r['current_grant_yen'],
            'current_grant_share_pct': r['current_grant_share_pct'],
            'gap_score': r['gap_score'],
            'interpretation': '未来重要度が高いが現在の助成は少ない' if r['gap_score'] > 30 else '中程度のギャップ',
        })

    # 5. opportunity_fields: 先取り機会分野
    opportunity_fields = []
    opp_results = sorted(all_results, key=lambda x: -(x['gap_score'] * x['foresight_score_normalized']))
    for r in opp_results:
        if r['gap_score'] > 5 and r['foresight_score_normalized'] > 20:
            opportunity_fields.append({
                'code': r['code'],
                'field_ja': r['field_ja'],
                'foresight_importance': r['foresight_score_normalized'],
                'current_grant_yen': r['current_grant_yen'],
                'gap_score': r['gap_score'],
                'opportunity_score': round(r['gap_score'] * r['foresight_score_normalized'] / 100, 1),
                'rationale': get_rationale(r['code'], r),
                'recommended_action': get_recommended_increase(r),
                'mapped_grant_fields': r['mapped_grant_fields'],
            })
        if len(opportunity_fields) >= 12:
            break

    # === メタデータ ===
    meta = {
        'generated_at': '2026-04-21',
        'foresight_db_stats': {
            'total_predictions_2030_2040': sum(r['prediction_count'] for r in all_results),
            'scenarios_analyzed': len(scenarios),
            'trends_analyzed': len(all_trends),
        },
        'grant_db_stats': {
            'total_grant_amount_yen': total_grant,
            'total_grant_amount_oku': round(total_grant / 100000000, 1),
        },
        'methodology': {
            'foresight_score': '予測件数×0.3 + 高信頼度予測×2.0 + 中信頼度予測×0.5 + シナリオ言及数×1.5',
            'normalization': 'log1p変換で突出値を緩和した上で0-100スケールに正規化',
            'gap_score': 'フォーサイトスコア正規化値 - 助成額正規化値（ともにlog正規化0-100スケール）',
            'opportunity_score': 'gap_score × foresight_score_normalized / 100',
        }
    }

    # 最終出力
    output = {
        'meta': meta,
        'emerging_fields_2030': emerging_fields_2030,
        'accelerating_trends': accelerating_trends,
        'robust_fields': robust_fields,
        'future_vs_current_gap': future_vs_current_gap,
        'opportunity_fields': opportunity_fields,
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Output saved to {OUTPUT_PATH}")
    print(f"  emerging_fields_2030: {len(emerging_fields_2030)} fields")
    print(f"  accelerating_trends: {len(accelerating_trends)} trends")
    print(f"  robust_fields: {len(robust_fields)} fields")
    print(f"  future_vs_current_gap: {len(future_vs_current_gap)} fields")
    print(f"  opportunity_fields: {len(opportunity_fields)} fields")

    # サマリー表示
    print("\n=== TOP 10 機会分野 ===")
    for f in opportunity_fields[:10]:
        print(f"  {f['code']} {f['field_ja']}: gap={f['gap_score']}, "
              f"foresight={f['foresight_importance']}, grant={f['current_grant_yen']:,}円")

    return output

if __name__ == '__main__':
    main()
