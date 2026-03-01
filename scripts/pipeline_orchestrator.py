#!/usr/bin/env python3
import argparse, sqlite3, subprocess, json, time, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / 'data' / 'pipeline_orchestrator.db'
CFG_PATH = ROOT / 'config' / 'pipeline_steps.json'

SCHEMA = '''
CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  video_id TEXT NOT NULL,
  pipeline_dir TEXT NOT NULL,
  status TEXT NOT NULL,
  current_step INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_error TEXT
);
CREATE TABLE IF NOT EXISTS step_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  step_index INTEGER NOT NULL,
  step_name TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  error TEXT,
  FOREIGN KEY(run_id) REFERENCES runs(id)
);
'''

def now():
    return datetime.datetime.utcnow().isoformat() + 'Z'

def db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = db()
    con.executescript(SCHEMA)
    con.commit()
    print(f'OK db initialized: {DB_PATH}')

def load_steps():
    if not CFG_PATH.exists():
        raise SystemExit(f'Missing config: {CFG_PATH}')
    return json.loads(CFG_PATH.read_text())['steps']

def enqueue(video_id, pipeline_dir):
    con = db()
    ts = now()
    pdir = str((ROOT / pipeline_dir).resolve()) if not str(pipeline_dir).startswith('/') else str(Path(pipeline_dir).resolve())
    con.execute('INSERT INTO runs(video_id,pipeline_dir,status,current_step,created_at,updated_at) VALUES(?,?,?,?,?,?)',
                (video_id, pdir, 'queued', 0, ts, ts))
    con.commit()
    rid = con.execute('SELECT last_insert_rowid()').fetchone()[0]
    print(f'OK queued run_id={rid}')

def list_runs(limit=20):
    con = db()
    rows = con.execute('SELECT id,video_id,status,current_step,updated_at,last_error FROM runs ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
    for r in rows:
        print(dict(r))

def run_cmd(cmd, cwd):
    p = subprocess.run(cmd, shell=True, cwd=cwd, text=True, capture_output=True)
    return p.returncode, p.stdout[-2000:], p.stderr[-2000:]

def process_one(max_retry=2):
    con = db()
    row = con.execute("SELECT * FROM runs WHERE status IN ('queued','running') ORDER BY id ASC LIMIT 1").fetchone()
    if not row:
        print('IDLE')
        return False

    rid, pipeline_dir = row['id'], row['pipeline_dir']
    steps = load_steps()

    if row['status'] == 'queued':
        con.execute("UPDATE runs SET status='running', updated_at=? WHERE id=?", (now(), rid))
        con.commit()

    idx = row['current_step']
    while idx < len(steps):
        step = steps[idx]
        attempt, success = 1, False
        while attempt <= max_retry + 1:
            sid = None
            try:
                con.execute('INSERT INTO step_runs(run_id,step_index,step_name,attempt,status,started_at) VALUES(?,?,?,?,?,?)',
                            (rid, idx, step['name'], attempt, 'running', now()))
                sid = con.execute('SELECT last_insert_rowid()').fetchone()[0]
                con.commit()
                rc, out, err = run_cmd(step['command'], pipeline_dir)
                if rc == 0:
                    con.execute('UPDATE step_runs SET status=?, ended_at=?, error=? WHERE id=?', ('done', now(), None, sid))
                    con.execute('UPDATE runs SET current_step=?, updated_at=?, last_error=? WHERE id=?', (idx + 1, now(), None, rid))
                    con.commit()
                    success = True
                    break
                else:
                    con.execute('UPDATE step_runs SET status=?, ended_at=?, error=? WHERE id=?', ('failed', now(), (err or out or 'unknown')[:1500], sid))
                    con.commit()
            except Exception as e:
                if sid:
                    con.execute('UPDATE step_runs SET status=?, ended_at=?, error=? WHERE id=?', ('failed', now(), str(e)[:1500], sid))
                    con.commit()
            attempt += 1
            if attempt <= max_retry + 1:
                time.sleep(3)
        if not success:
            con.execute("UPDATE runs SET status='failed', updated_at=?, last_error=? WHERE id=?", (now(), f"step={step['name']}", rid))
            con.commit()
            print(f"FAILED run_id={rid} step={step['name']}")
            return True
        idx += 1

    con.execute("UPDATE runs SET status='completed', updated_at=? WHERE id=?", (now(), rid))
    con.commit()
    print(f'COMPLETED run_id={rid}')
    return True

def worker(loop=False, sleep_s=8):
    while True:
        worked = process_one()
        if not loop:
            break
        if not worked:
            time.sleep(sleep_s)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    sub.add_parser('init')
    q = sub.add_parser('enqueue'); q.add_argument('--video-id', required=True); q.add_argument('--pipeline-dir', required=True)
    l = sub.add_parser('list'); l.add_argument('--limit', type=int, default=20)
    w = sub.add_parser('worker'); w.add_argument('--loop', action='store_true'); w.add_argument('--sleep', type=int, default=8)
    args = ap.parse_args()
    if args.cmd == 'init': init_db()
    elif args.cmd == 'enqueue': enqueue(args.video_id, args.pipeline_dir)
    elif args.cmd == 'list': list_runs(args.limit)
    elif args.cmd == 'worker': worker(args.loop, args.sleep)
