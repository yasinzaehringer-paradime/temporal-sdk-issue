# Reproduce memory leak in temporal Python SDK 1.1.0

1. Create venv using poetry
2. Copy `env.tpl` to `.env` and populate the fields with your temporal cloud credentials (URL + certs) 
3. Start worker via `python main.py worker`
4. Trigger multiple runs via `time poetry run python main.py trigger --timeout=0.1 --sub-activities=0 --sub-workflows=100 &`
5. Every run adds ~20MB according to the MacOS activity monitor
6. According to `tracemalloc` (which prints every 30s( we have a surprising amount of dataclasses hanging around 

Remarks:
- The workflow spans 100 sub-workflows which all wait for `0.1s` - no activity is run (did not see any weird behaviour when there were only sub-activies)
- `gc.collect()` has no effect, these dataclasses really do exist somewhere
- Going higher than 100 sub-workflows leads to `Potential deadlock detected, workflow didn't yield within 2 second(s)` - which is problematic in it's own right