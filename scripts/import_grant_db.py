#!/usr/bin/env python3
"""Import foundation-type organizations from grant-db that are not in CFG."""
from __future__ import annotations
import json, re, sqlite3, uuid
from pathlib import Path

CFG_DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")
GRANT_DB = Path("/Users/nishimura+/projects/apps/grant-db/grant_db.sqlite")


def normalize_name(name: str) -> str:
    if not name:
        return ""
    s = re.sub(r"^(公益|一般|特定非営利活動|特例)?(財団法人|社団法人)?\s*", "", name)
    s = s.replace("　", "").replace(" ", "")
    return s.lower()


def detect_legal_form(name: str) -> str:
    if "公益財団法人" in name:
        return "公益財団法人"
    if "公益社団法人" in name:
        return "公益社団法人"
    if "一般財団法人" in name:
        return "一般財団法人"
    if "一般社団法人" in name:
        return "一般社団法人"
    if "特定非営利活動法人" in name:
        return "特定非営利活動法人"
    return "その他"


def detect_subtype(name: str, desc: str = "") -> str:
    n = (name or "") + " " + (desc or "")
    if any(k in n for k in ["国際", "世界", "アジア", "ユネスコ", "UNESCO"]):
        return "intl"
    if any(k in n for k in ["大学", "学会", "学術振興会", "学院"]):
        return "academic"
    if any(k in n for k in ["記念", "奨学", "篤志", "賞"]):
        return "individual"
    if any(k in n for k in ["市民", "ボランティア", "市民基金"]):
        return "ngo"
    if any(k in n for k in ["振興", "技術", "科学", "産業"]):
        return "corporate"
    return "other"


def main():
    cfg = sqlite3.connect(CFG_DB)
    grant = sqlite3.connect(GRANT_DB)
    grant.row_factory = sqlite3.Row

    # Existing CFG foundations
    cur = cfg.cursor()
    cur.execute("SELECT id, name, url, prefecture FROM organizations")
    existing = {}
    for r in cur.fetchall():
        existing[normalize_name(r[0])] = {"id": r[1] if False else r[0], "url": r[2], "pref": r[3]}
        # wait: r[0]=id, r[1]=name. Fix:
    cur.execute("SELECT id, name, url, prefecture FROM organizations")
    existing = {normalize_name(name): {"id": rid, "url": url, "pref": pref}
                for rid, name, url, pref in cur.fetchall()}

    # Grant DB foundations
    g = grant.cursor()
    g.execute("SELECT name, name_en, prefecture, url, description, contact_phone, contact_email, contact_address FROM organizations WHERE type='foundation'")
    rows = g.fetchall()
    print(f"Grant DB foundations: {len(rows)}")
    print(f"CFG existing: {len(existing)}")

    new_count = update_count = skip_count = 0

    for r in rows:
        name = (r[0] or "").strip()
        if not name:
            continue
        norm = normalize_name(name)
        if norm in existing:
            # Update missing fields
            ex = existing[norm]
            updates = []
            params = []
            if r[3] and not ex.get("url"):
                updates.append("url=?")
                params.append(r[3])
            if r[2] and not ex.get("pref"):
                updates.append("prefecture=?")
                params.append(r[2])
            if r[4]:
                updates.append("description=COALESCE(NULLIF(description,''), ?)")
                params.append(r[4])
            if r[5]:
                updates.append("contact_phone=COALESCE(contact_phone, ?)")
                params.append(r[5])
            if r[6]:
                updates.append("contact_email=COALESCE(contact_email, ?)")
                params.append(r[6])
            if r[7]:
                updates.append("contact_address=COALESCE(contact_address, ?)")
                params.append(r[7])
            if updates:
                params.append(ex["id"])
                cur.execute(
                    f"UPDATE organizations SET {', '.join(updates)}, updated_at=datetime('now','localtime') WHERE id=?",
                    params,
                )
                update_count += 1
            else:
                skip_count += 1
            continue

        # New record
        new_id = str(uuid.uuid4())
        legal = detect_legal_form(name)
        sub = detect_subtype(name, r[4] or "")
        meta = json.dumps({"source": "grant_db_import"}, ensure_ascii=False)
        cur.execute(
            """
            INSERT INTO organizations
            (id, name, name_en, type, foundation_subtype, legal_form,
             prefecture, url, description, contact_phone, contact_email, contact_address,
             metadata, country_code, created_at, updated_at)
            VALUES (?, ?, ?, 'foundation', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'JP',
                    datetime('now','localtime'), datetime('now','localtime'))
            """,
            (new_id, name, r[1], sub, legal, r[2], r[3], r[4], r[5], r[6], r[7], meta),
        )
        new_count += 1
        existing[norm] = {"id": new_id, "url": r[3], "pref": r[2]}

    cfg.commit()
    cur.execute("SELECT COUNT(*) FROM organizations")
    total = cur.fetchone()[0]
    print(f"\nGrant DB import:")
    print(f"  New: {new_count}")
    print(f"  Updated: {update_count}")
    print(f"  Skipped (no diff): {skip_count}")
    print(f"  Total CFG organizations: {total}")
    cfg.close()
    grant.close()


if __name__ == "__main__":
    main()
