#!/usr/bin/env python3
import argparse
import csv
import datetime
import hashlib
import json
import os
import re
import sqlite3

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

def n_hdr(h):
    return re.sub(r'[_\s]+', ' ', h.lower().strip())

def c_phone(raw):
    if not raw or not raw.strip():
        return None, None
    d = re.sub(r'\D', '', raw)
    if d.startswith('91') and len(d) == 12:
        d = d[2:]
    elif d.startswith('0') and len(d) == 11:
        d = d[1:]
    if len(d) == 10 and d[0] in ('6', '7', '8', '9'):
        return f"+91{d}", None
    return None, 'invalid_phone'

def p_dt(raw):
    if not raw or not raw.strip():
        return None, False
    v = raw.strip()
    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y"]:
        try:
            dt = datetime.datetime.strptime(v, fmt)
            if fmt != "%Y-%m-%d":
                parts = re.split(r'[/\-]', v)
                day, month = int(parts[0]), int(parts[1]) if parts[1].isdigit() else dt.month
                if dt.day != day or dt.month != month:
                    return None, True
            return dt.strftime("%Y-%m-%d"), False
        except ValueError:
            continue
    return None, True

def p_amt(raw):
    if not raw or not raw.strip():
        return None, False
    s = re.sub(r'^(?:rs\.?|inr|₹)\s*', '', raw.strip(), flags=re.IGNORECASE).strip().replace(',', '')
    try:
        val = float(s)
        if val < 0:
            return None, True
        return round(val, 2), False
    except ValueError:
        return None, True

def to_ist(s):
    s = s.strip()
    if s.endswith("Z"):
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).astimezone(IST)
    elif "+" in s or ("-" in s and "T" in s):
        return datetime.datetime.fromisoformat(s).astimezone(IST)
    else:
        return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)

def h_pii(v):
    return hashlib.sha256(v.encode('utf-8')).hexdigest() if v else None

def db_init(path):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            workflow_id TEXT, phone_hash TEXT, phone_last4 TEXT, external_ref TEXT,
            batch_id TEXT, name_hash TEXT, policy_expiry TEXT, premium_amount REAL,
            order_date TEXT, product TEXT, preferred_language TEXT,
            PRIMARY KEY (workflow_id, phone_hash)
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ledger_events (
            event_id TEXT PRIMARY KEY, workflow_id TEXT, event_type TEXT,
            call_id TEXT, occurred_at TEXT, occurred_at_ist TEXT, phone_hash TEXT, payload TEXT
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dispositions_catalogue (
            workflow_id TEXT, code TEXT, is_terminal INTEGER, is_success INTEGER,
            PRIMARY KEY (workflow_id, code)
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rejections_log (
            workflow_id TEXT, batch_id TEXT, row_number INTEGER, reason TEXT,
            PRIMARY KEY (workflow_id, batch_id, row_number)
        );
    """)
    conn.execute("CREATE TABLE IF NOT EXISTS ingested_files (file_path TEXT PRIMARY KEY);")
    conn.execute("CREATE TABLE IF NOT EXISTS quality_metrics (key TEXT PRIMARY KEY, val INTEGER);")
    conn.commit()
    return conn

def load_disp(conn, path):
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            conn.execute("""
                INSERT OR REPLACE INTO dispositions_catalogue (workflow_id, code, is_terminal, is_success)
                VALUES (?, ?, ?, ?)
            """, (r['workflow_id'], r['code'].lower().strip(), 1 if r['is_terminal'].lower() == 'true' else 0, 1 if r['is_success'].lower() == 'true' else 0))
    conn.commit()

def run_ingest(conn, drop):
    base = os.path.dirname(os.path.dirname(os.path.abspath(drop)))
    d_csv = os.path.join(base, "schemas", "dispositions.csv")
    if not os.path.exists(d_csv):
        d_csv = os.path.join("assignment-a-data", "schemas", "dispositions.csv")
    load_disp(conn, d_csv)

    keys = ["lead_rows_total", "lead_rows_rejected", "duplicate_lead_rows_merged", "field_warnings", "events_total", "malformed_events", "duplicate_events_dropped", "orphan_events", "unknown_disposition_events"]
    for k in keys:
        if not conn.execute("SELECT 1 FROM quality_metrics WHERE key = ?", (k,)).fetchone():
            conn.execute("INSERT INTO quality_metrics (key, val) VALUES (?, 0)", (k,))
    conn.commit()

    def bump(k, n=1):
        conn.execute("UPDATE quality_metrics SET val = val + ? WHERE key = ?", (n, k))

    l_dir = os.path.join(drop, "leads")
    if os.path.exists(l_dir):
        for wf in sorted(os.listdir(l_dir)):
            wf_p = os.path.join(l_dir, wf)
            if not os.path.isdir(wf_p):
                continue
            s_file = os.path.join(base, "schemas", f"{wf}.json")
            if not os.path.exists(s_file):
                s_file = os.path.join("assignment-a-data", "schemas", f"{wf}.json")
            if not os.path.exists(s_file):
                continue
            with open(s_file, 'r', encoding='utf-8') as sf:
                sch = json.load(sf)
            fds = sch['fields']
            ref_c = next((f['name'] for f in fds if f.get('reference')), None)

            for bf in sorted([f for f in os.listdir(wf_p) if f.endswith('.csv')]):
                cf_path = os.path.join(wf_p, bf)
                abs_f = os.path.abspath(cf_path)
                if conn.execute("SELECT 1 FROM ingested_files WHERE file_path = ?", (abs_f,)).fetchone():
                    continue
                bid = os.path.splitext(bf)[0]
                with open(cf_path, 'r', encoding='utf-8', errors='replace') as cf:
                    rdr = csv.reader(cf)
                    try:
                        hdrs = next(rdr)
                    except StopIteration:
                        conn.execute("INSERT INTO ingested_files (file_path) VALUES (?)", (abs_f,))
                        continue
                    n_hdrs = [n_hdr(h) for h in hdrs]
                    a_map = {}
                    for fd in fds:
                        a_map[n_hdr(fd['name'])] = fd['name']
                        for al in fd.get('aliases', []):
                            a_map[n_hdr(al)] = fd['name']

                    r_num = 0
                    for row in rdr:
                        if not row or all(not c.strip() for c in row):
                            continue
                        r_num += 1
                        bump("lead_rows_total")
                        cells = {}
                        for i, hn in enumerate(n_hdrs):
                            if i < len(row) and hn in a_map:
                                cells[a_map[hn]] = row[i].strip()

                        out, warns, rej = {}, 0, None
                        for fd in fds:
                            fn, ft, req = fd['name'], fd['type'], fd.get('required', False)
                            val = cells.get(fn, "")
                            if ft == 'phone':
                                cp, perr = c_phone(val)
                                if req and (not val or perr):
                                    rej = "missing_required:phone" if not val else "invalid_phone"
                                    break
                                elif not req and val and perr:
                                    warns += 1
                                    out[fn] = None
                                else:
                                    out[fn] = cp
                            elif ft == 'string':
                                if req and not val:
                                    rej = f"missing_required:{fn}"
                                    break
                                out[fn] = val if val else None
                            elif ft == 'date':
                                dt_v, w = p_dt(val)
                                if w and val:
                                    warns += 1
                                out[fn] = dt_v
                            elif ft == 'decimal':
                                am_v, w = p_amt(val)
                                if w and val:
                                    warns += 1
                                out[fn] = am_v
                            elif ft == 'enum':
                                vl = val.lower() if val else ""
                                allowed = [x.lower() for x in fd.get('values', [])]
                                if vl in allowed:
                                    out[fn] = vl
                                else:
                                    if val:
                                        warns += 1
                                    out[fn] = None

                        if rej:
                            bump("lead_rows_rejected")
                            conn.execute("INSERT OR IGNORE INTO rejections_log (workflow_id, batch_id, row_number, reason) VALUES (?, ?, ?, ?)", (wf, bid, r_num, rej))
                            continue
                        if warns > 0:
                            bump("field_warnings", warns)

                        p_can = out.get('phone')
                        if not p_can:
                            bump("lead_rows_rejected")
                            conn.execute("INSERT OR IGNORE INTO rejections_log (workflow_id, batch_id, row_number, reason) VALUES (?, ?, ?, ?)", (wf, bid, r_num, "invalid_phone"))
                            continue

                        ph = h_pii(p_can)
                        pl4 = p_can[-4:]
                        nh = h_pii(out.get('customer_name'))
                        eref = out.get(ref_c) if ref_c else None

                        exist = conn.execute("SELECT name_hash, policy_expiry, premium_amount, order_date, product, preferred_language, external_ref FROM leads WHERE workflow_id = ? AND phone_hash = ?", (wf, ph)).fetchone()
                        if exist:
                            bump("duplicate_lead_rows_merged")
                            conn.execute("""
                                UPDATE leads SET name_hash = ?, policy_expiry = ?, premium_amount = ?,
                                order_date = ?, product = ?, preferred_language = ?, external_ref = ?
                                WHERE workflow_id = ? AND phone_hash = ?
                            """, (nh or exist[0], out.get('policy_expiry') or exist[1], out.get('premium_amount') if out.get('premium_amount') is not None else exist[2], out.get('order_date') or exist[3], out.get('product') or exist[4], out.get('preferred_language') or exist[5], eref or exist[6], wf, ph))
                        else:
                            conn.execute("""
                                INSERT INTO leads (workflow_id, phone_hash, phone_last4, external_ref, batch_id, name_hash, policy_expiry, premium_amount, order_date, product, preferred_language)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (wf, ph, pl4, eref, bid, nh, out.get('policy_expiry'), out.get('premium_amount'), out.get('order_date'), out.get('product'), out.get('preferred_language')))
                conn.execute("INSERT INTO ingested_files (file_path) VALUES (?)", (abs_f,))

    conn.execute("UPDATE leads SET preferred_language = 'en' WHERE preferred_language IS NULL OR preferred_language = ''")

    e_dir = os.path.join(drop, "events")
    if os.path.exists(e_dir):
        for ef in sorted([f for f in os.listdir(e_dir) if f.endswith('.jsonl')]):
            ef_p = os.path.join(e_dir, ef)
            abs_e = os.path.abspath(ef_p)
            if conn.execute("SELECT 1 FROM ingested_files WHERE file_path = ?", (abs_e,)).fetchone():
                continue
            with open(ef_p, 'r', encoding='utf-8', errors='replace') as emf:
                for line in emf:
                    sl = line.strip()
                    if not sl:
                        continue
                    bump("events_total")
                    try:
                        ev = json.loads(sl)
                    except json.JSONDecodeError:
                        bump("malformed_events")
                        continue
                    if not isinstance(ev, dict) or not ev.get("event_id"):
                        bump("malformed_events")
                        continue
                    eid = ev["event_id"]
                    if conn.execute("SELECT 1 FROM ledger_events WHERE event_id = ?", (eid,)).fetchone():
                        bump("duplicate_events_dropped")
                        continue
                    wf, etype, cid = ev.get("workflow_id"), ev.get("event_type"), ev.get("call_id")
                    occ_r = ev.get("occurred_at", "")
                    payload = ev.get("payload", {})
                    try:
                        ist_s = to_ist(occ_r).strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        ist_s = occ_r
                    raw_ph = ev.get("phone", "")
                    cp, _ = c_phone(raw_ph) if raw_ph else (None, None)
                    ph = h_pii(cp) if cp else None
                    if etype and etype.startswith("disposition."):
                        code = str(payload.get("disposition_code", "")).lower().strip()
                        if not conn.execute("SELECT 1 FROM dispositions_catalogue WHERE workflow_id = ? AND code = ?", (wf, code)).fetchone():
                            bump("unknown_disposition_events")
                    conn.execute("""
                        INSERT INTO ledger_events (event_id, workflow_id, event_type, call_id, occurred_at, occurred_at_ist, phone_hash, payload)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (eid, wf, etype, cid, occ_r, ist_s, ph, json.dumps(payload)))
            conn.execute("INSERT INTO ingested_files (file_path) VALUES (?)", (abs_e,))
    conn.commit()

def build_reports(conn, out):
    os.makedirs(out, exist_ok=True)
    cur = conn.cursor()
    leads = cur.fetchall() if cur.execute("SELECT workflow_id, phone_hash, phone_last4, batch_id, external_ref FROM leads") else []
    v_leads = {(r[0], r[1]): r for r in leads}

    events = cur.fetchall() if cur.execute("SELECT event_id, workflow_id, event_type, call_id, occurred_at, occurred_at_ist, phone_hash, payload FROM ledger_events") else []
    events.sort(key=lambda e: to_ist(e[4]) if e[4] else datetime.datetime.min.replace(tzinfo=IST))

    c_evs, c_lead = {}, {}
    for ev in events:
        eid, wf, etype, cid, occ, occ_ist, ph, pstr = ev
        if not cid:
            continue
        c_evs.setdefault(cid, []).append(ev)
        if ph and (wf, ph) in v_leads:
            c_lead[cid] = (wf, ph)

    orph = 0
    for cid, evs in c_evs.items():
        if cid not in c_lead:
            orph += len(evs)
    for ev in events:
        if not ev[3]:
            orph += 1
    conn.execute("UPDATE quality_metrics SET val = ? WHERE key = 'orphan_events'", (orph,))
    conn.commit()

    disp_lk = {(r[0], r[1]): (r[2] == 1, r[3] == 1) for r in cur.execute("SELECT workflow_id, code, is_terminal, is_success FROM dispositions_catalogue")}

    l_calls, c_init_dt, c_conn, c_fail, c_eff = {}, {}, set(), set(), {}
    for cid, evs in c_evs.items():
        lk = c_lead.get(cid)
        if not lk:
            continue
        wf, ph = lk
        has_init = False
        sets, ovrs = [], []
        for ev in evs:
            _, _, etype, _, occ, occ_ist, _, pstr = ev
            p = json.loads(pstr)
            dt = to_ist(ev[4]) if ev[4] else datetime.datetime.min.replace(tzinfo=IST)
            if etype == "call.initiated":
                has_init = True
                l_calls.setdefault((wf, ph), set()).add(cid)
                c_init_dt[cid] = dt
            elif etype == "call.connected":
                c_conn.add(cid)
            elif etype == "call.telephony_failed":
                c_fail.add(cid)
            elif etype == "disposition.set":
                code = str(p.get("disposition_code", "")).lower().strip()
                if (wf, code) in disp_lk:
                    sets.append((dt, code))
            elif etype == "disposition.override":
                code = str(p.get("disposition_code", "")).lower().strip()
                if (wf, code) in disp_lk:
                    ovrs.append((dt, code))
        if has_init:
            if ovrs:
                ovrs.sort(key=lambda x: x[0])
                c_eff[cid] = ovrs[-1][1]
            elif sets:
                sets.sort(key=lambda x: x[0])
                c_eff[cid] = sets[-1][1]

    wf_cnts, l_out = {}, []
    for wf, ph, pl4, bid, eref in leads:
        wf_cnts.setdefault((wf, bid), {"total_leads": 0, "yet_to_reach_out": 0, "in_progress_telephony_failed": 0, "in_progress_dialed": 0, "completed": 0})
        wf_cnts[(wf, bid)]["total_leads"] += 1
        cids = sorted(list(l_calls.get((wf, ph), set())), key=lambda c: c_init_dt[c])
        tot = len(cids)
        conn = sum(1 for c in cids if c in c_conn)
        last_at, curr_disp, ever_s = "", "", False

        if cids:
            last_at = c_init_dt[cids[-1]].isoformat()
            for c in reversed(cids):
                dc = c_eff.get(c)
                if dc and not curr_disp:
                    curr_disp = dc
                if dc and disp_lk.get((wf, dc), (False, False))[1]:
                    ever_s = True

        bkt = "yet_to_reach_out"
        if tot > 0:
            any_t = any(c_eff.get(c) and disp_lk.get((wf, c_eff[c]), (False, False))[0] for c in cids)
            all_f = all(c in c_fail for c in cids)
            if any_t:
                bkt = "completed"
            elif all_f:
                bkt = "in_progress_telephony_failed"
            else:
                bkt = "in_progress_dialed"

        wf_cnts[(wf, bid)][bkt] += 1
        l_out.append({
            "workflow_id": wf, "external_ref": eref or "", "batch_id": bid,
            "phone_masked": f"+91******{pl4}", "bucket": bkt, "total_calls": tot,
            "connected_calls": conn, "last_call_at": last_at, "current_disposition": curr_disp,
            "ever_success": "true" if ever_s else "false"
        })

    with open(os.path.join(out, "batch_summary.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["workflow_id", "batch_id", "total_leads", "yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed"])
        w.writeheader()
        for (wf, bid), cnts in sorted(wf_cnts.items()):
            row = {"workflow_id": wf, "batch_id": bid}
            row.update(cnts)
            w.writerow(row)

    daily = {}
    for cid, dt in c_init_dt.items():
        lk = c_lead.get(cid)
        if not lk:
            continue
        wf, ph = lk
        ds = dt.strftime("%Y-%m-%d")
        daily.setdefault((wf, ds), {"tot": 0, "conn": 0, "succ": set()})
        daily[(wf, ds)]["tot"] += 1
        if cid in c_conn:
            daily[(wf, ds)]["conn"] += 1
        dc = c_eff.get(cid)
        if dc and disp_lk.get((wf, dc), (False, False))[1]:
            daily[(wf, ds)]["succ"].add(ph)

    with open(os.path.join(out, "daily_overview.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"])
        w.writeheader()
        for (wf, ds), dv in sorted(daily.items()):
            rate = f"{dv['conn'] / dv['tot']:.4f}" if dv['tot'] > 0 else "0.0000"
            w.writerow({"workflow_id": wf, "date": ds, "total_calls": dv["tot"], "connected_calls": dv["conn"], "connectivity_rate": rate, "success_leads": len(dv["succ"])})

    l_out.sort(key=lambda x: (x["workflow_id"], x["external_ref"]))
    with open(os.path.join(out, "lead_report.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls", "connected_calls", "last_call_at", "current_disposition", "ever_success"])
        w.writeheader()
        w.writerows(l_out)

    with open(os.path.join(out, "rejections.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["workflow_id", "batch_id", "row_number", "reason"])
        for r in cur.execute("SELECT workflow_id, batch_id, row_number, reason FROM rejections_log ORDER BY workflow_id, batch_id, row_number"):
            w.writerow(r)

    qm = {k: v for k, v in cur.execute("SELECT key, val FROM quality_metrics")}
    rejs = {r[0]: r[1] for r in cur.execute("SELECT reason, COUNT(*) FROM rejections_log GROUP BY reason ORDER BY reason")}
    dq = {
        "lead_rows_total": qm.get("lead_rows_total", 0),
        "lead_rows_rejected": qm.get("lead_rows_rejected", 0),
        "rejections_by_reason": rejs,
        "duplicate_lead_rows_merged": qm.get("duplicate_lead_rows_merged", 0),
        "field_warnings": qm.get("field_warnings", 0),
        "events_total": qm.get("events_total", 0),
        "malformed_events": qm.get("malformed_events", 0),
        "duplicate_events_dropped": qm.get("duplicate_events_dropped", 0),
        "orphan_events": qm.get("orphan_events", 0),
        "unknown_disposition_events": qm.get("unknown_disposition_events", 0)
    }
    with open(os.path.join(out, "data_quality.json"), "w", encoding="utf-8") as f:
        json.dump(dq, f, indent=2)

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p1 = sub.add_parser("ingest")
    p1.add_argument("drop_path")
    p2 = sub.add_parser("report")
    p2.add_argument("--out", required=True)
    args = ap.parse_args()
    conn = db_init("pipeline.db")
    if args.cmd == "ingest":
        run_ingest(conn, args.drop_path)
    elif args.cmd == "report":
        build_reports(conn, args.out)
    conn.close()

if __name__ == "__main__":
    main()