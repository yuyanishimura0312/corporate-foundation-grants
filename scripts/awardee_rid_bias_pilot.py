#!/usr/bin/env python3
"""助成の偏り分析 パイロット — CFG採択者(grant_results) × RID研究者DB(31.1万) × AGD学問分野分類。
   採択者を名寄せでRIDに連携→学問分野・機関を付与→(機関集中/分野偏り/キャリア)を算出。READ ONLY."""
import sqlite3, re, unicodedata
from collections import Counter, defaultdict
CFG = "corporate_research_grants.sqlite"
RID = "/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
def nn(s): return re.sub(r'[\s　・]', '', unicodedata.normalize("NFKC", s or ""))
def base_inst(a):  # 機関名の大学単位正規化(大学院・研究科等を落とす)
    a = nn(a)
    m = re.match(r'^(.+?大学)', a) or re.match(r'^(.+?(研究所|機構|センター|大学校|高専|病院))', a)
    return m.group(1) if m else a[:12]

cfg = sqlite3.connect(CFG); rid = sqlite3.connect(RID)
# index RID by normalized name -> [(base_id, institute)]
ridmap = defaultdict(list)
for bid, nm, inst in rid.execute("SELECT base_researcher_id,name_ja,institute_name FROM rid_identity WHERE name_ja IS NOT NULL"):
    ridmap[nn(nm)].append((bid, inst or ""))
# agd field per base_researcher_id
field = {}
for bid, fj in rid.execute("SELECT r.base_researcher_id, f.agd_field_ja FROM rid_agd_field f JOIN rid_identity r ON r.rid=f.rid"):
    field.setdefault(bid, fj)

aw = cfg.execute("""SELECT r.awardee_name, r.awardee_affiliation, r.award_amount, r.awardee_position, o.name
    FROM grant_results r JOIN grant_calls gc ON r.call_id=gc.id JOIN grant_programs p ON gc.program_id=p.id
    JOIN organizations o ON p.organization_id=o.id WHERE r.awardee_name IS NOT NULL""").fetchall()

linked = 0; homonym_resolved = 0; unlinked = 0
inst_cnt = Counter(); inst_amt = Counter()
field_cnt = Counter(); field_amt = Counter()
pos_cnt = Counter()
for name, affil, amt, pos, fnd in aw:
    cands = ridmap.get(nn(name), [])
    bid = None
    if len(cands) == 1:
        bid = cands[0][0]; linked += 1
    elif len(cands) > 1:  # homonym: disambiguate by affiliation
        af = base_inst(affil)
        m = [c for c in cands if af and (af in nn(c[1]) or nn(c[1])[:6] in af)]
        if len(m) == 1: bid = m[0][0]; linked += 1; homonym_resolved += 1
        else: unlinked += 1
    else:
        unlinked += 1
    inst_cnt[base_inst(affil)] += 1
    if amt: inst_amt[base_inst(affil)] += amt
    if pos: pos_cnt[re.sub(r'[（(].*', '', nn(pos))[:6]] += 1
    if bid and bid in field:
        field_cnt[field[bid]] += 1
        if amt: field_amt[field[bid]] += amt

N = len(aw)
print("=== 採択者→RID連携 ===")
print(f"採択者総数 {N} / RID連携 {linked} ({100*linked/N:.0f}%) (うち同名消歧 {homonym_resolved}) / 未連携 {unlinked}")
print(f"\n=== 機関集中(偏り) 上位15 / 全{len(inst_cnt)}機関 ===")
top = inst_cnt.most_common(15); topshare = sum(n for _, n in top)
for inst, n in top: print(f"  {inst:16s} 採択{n:4d}件  金額{inst_amt[inst]:>15,}円")
print(f"上位15機関で採択件数の {100*topshare/N:.0f}% を占有")
hhi = sum((100*n/N)**2 for n in inst_cnt.values())
print(f"機関集中度 HHI={hhi:.0f} (1万=独占/1500超=高集中)")
print(f"\n=== 学問分野別(RID連携分 {sum(field_cnt.values())}件) 偏り ===")
for f, n in field_cnt.most_common(12): print(f"  {f:14s} {n:4d}件  金額{field_amt[f]:>15,}円")
print(f"\n=== キャリア段階(職位) ===")
for p, n in pos_cnt.most_common(8): print(f"  {p:12s} {n}件")
