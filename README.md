# videoproducingapp

## Pipeline Orchestrator

This repo now includes a lightweight video pipeline orchestrator with queue + retries + checkpoints.

### Files
- `scripts/pipeline_orchestrator.py`
- `config/pipeline_steps.json`
- runtime DB: `data/pipeline_orchestrator.db`

### Quick start
```bash
python3 scripts/pipeline_orchestrator.py init
python3 scripts/pipeline_orchestrator.py enqueue --video-id test-001 --pipeline-dir ./your_pipeline_dir
python3 scripts/pipeline_orchestrator.py worker
python3 scripts/pipeline_orchestrator.py list --limit 20
```

### Notes
- `pipeline-dir` should contain expected assets/scripts (`research.md`, `script.md`, `vo_*.mp3`, and `build.sh`/`assemble.py`).
- The orchestrator records run and step status in SQLite for resume/debugging.
