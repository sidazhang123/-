from pathlib import Path
from zsdtdx.simple_api import set_config_path, get_stock_kline

cfg = str(Path('app/zsdtdx_config.yaml').resolve())
resolved = set_config_path(cfg)

task = [{
    'code': '302132',
    'freq': 'd',
    'start_time': '2026-02-01',
    'end_time': '2026-03-13',
}]

try:
    events = get_stock_kline(task=task, mode='sync')
except Exception as e:
    print({'ok': False, 'stage': 'call', 'error': f'{type(e).__name__}: {e}', 'config': resolved})
    raise SystemExit(0)

if not isinstance(events, list):
    print({'ok': False, 'stage': 'result_type', 'type': type(events).__name__, 'config': resolved})
    raise SystemExit(0)

data_events = [e for e in events if isinstance(e, dict) and str(e.get('event','')).lower() == 'data']
err_events = [e for e in data_events if str(e.get('error') or '').strip()]
ok_events = [e for e in data_events if not str(e.get('error') or '').strip()]
rows_total = 0
sample_dt = None
for e in ok_events:
    rows = e.get('rows') if isinstance(e.get('rows'), list) else []
    rows_total += len(rows)
    if sample_dt is None and rows:
        first = rows[0]
        if isinstance(first, dict):
            sample_dt = first.get('datetime')

print({
    'ok': True,
    'config': resolved,
    'events_total': len(events),
    'data_events': len(data_events),
    'ok_events': len(ok_events),
    'error_events': len(err_events),
    'rows_total': rows_total,
    'sample_datetime': sample_dt,
    'error_samples': [str(e.get('error')) for e in err_events[:5]],
})
