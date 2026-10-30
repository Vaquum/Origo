"""A compact email view of the monitor's recorded current evidence."""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from datetime import datetime

from origo.observatory import (
    Document,
    OperatorSummary,
)
from origo.observatory import (
    evidence_number as _number,
)
from origo.observatory import (
    evidence_object as _object,
)
from origo.observatory import (
    evidence_objects as _objects,
)
from origo.observatory import (
    publication_status as _publication_status,
)
from origo.observatory import (
    summary_status as _status,
)

BODY_BYTES = 64 * 1024


def _duration(value: object) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return 'Unknown'
    minutes = round(value / 60)
    if minutes >= 60:
        hours, remainder = divmod(minutes, 60)
        return f'{hours}h{remainder:02d}m' if remainder else f'{hours}h'
    return f'{minutes}m' if value >= 60 else f'{round(value)}s'


def _date(value: object, pattern: str = '%d %b') -> str:
    return datetime.fromisoformat(str(value).replace('Z', '+00:00')).strftime(pattern) if value else 'Unknown'


def _name(value: object) -> str:
    words = str(value or 'Unknown source').replace('_', ' ')
    if words.startswith('binance '):
        words = 'Binance ' + words.removeprefix('binance ')
    return re.sub(r'\bperp\b', 'perpetual', words, flags=re.IGNORECASE).replace('aggtrades', 'aggregate trades').replace(' v1', '')


@dataclass
class Issue:
    key: str
    label: str
    status: str
    lines: list[str] = field(default_factory=list[str])
    limits: list[str] = field(default_factory=list[str])
    target: Document = field(default_factory=Document)
    priority: int = 3
    age: float | None = None
    action: str = 'Check the source failure in Dagit.'


def _issues(facts: Document) -> tuple[list[Issue], list[str], list[str]]:
    issues: dict[str, Issue] = {}
    waits: dict[tuple[str, str, str, str], list[str]] = {}
    notes: list[str] = []
    sources = _objects(facts.get('sources'))
    outputs = _objects(facts.get('outputs'))
    operations = _object(facts.get('operations'))
    for source in sources:
        key = str(source['id'])
        name = _name(source.get('name') or key)
        predicates = _object(source.get('predicates'))
        r1 = _object(predicates.get('R1'))
        for family, raw in predicates.items():
            result = _object(raw)
            status = _status(result.get('status'))
            if status == 'EXPECTED_WAIT' and result.get('deadline'):
                identity = (str(result.get('day') or result.get('hour') or ''), str(result['deadline']),
                            'perpetual' if '_perp_' in key else 'spot' if '_spot_' in key else '', str(result.get('canonical_interval') or ''))
                waits.setdefault(identity, []).append(name)
            if status not in ('FAIL', 'UNKNOWN'):
                continue
            issue = issues.setdefault(key, Issue(key, name, status, target={'view': 'sources', 'source': key}))
            if status == 'FAIL':
                issue.status = 'FAIL'
            if family in ('R1', 'M1'):
                age = _number(result.get('age_seconds'))
                issue.priority = 0 if status == 'FAIL' else 2
                limit = _number(result.get('budget_seconds'))
                issue.age = age if age is not None and limit is not None and age > limit else None
                if result.get('reader_end'):
                    issue.lines.append(f'Readable data ends at {_date(result["reader_end"], "%H:%M")}.')
                if result.get('budget_seconds') is not None:
                    issue.limits.append(f'Reader limit {_duration(result["budget_seconds"])}')
                if family == 'M1':
                    issue.lines.append('The market state cube failed its current-data check.')
                elif age is None or age <= (_number(result.get('budget_seconds')) or 0):
                    issue.lines.append('Live coverage failed validation.' if status == 'FAIL' else 'Live coverage cannot be verified.')
                issue.action = 'Check ingestion and publication failures in Dagit.'
            elif family in ('C1', 'C2'):
                missing = result.get('first_invalid_day') or result.get('first_invalid_hour') or result.get('day') or result.get('hour')
                reason = str(result.get('reason') or '')
                if 'missing' in reason and missing:
                    unit = 'hourly' if 'hour' in reason or result.get('canonical_interval') == 'hour' else 'daily'
                    issue.lines.append(f'The {_date(missing, "%d %b %H:%M" if unit == "hourly" else "%d %b")} {unit} archive is missing.')
                else:
                    issue.lines.append('Archive validation failed.' if status == 'FAIL' else 'Archive evidence is unavailable.')
                issue.priority = min(issue.priority, 1)
                issue.action = 'Check the archive failure and provider availability in Dagit.'
            else:
                label = {'M2': 'Market state history', 'D1': 'Depth minute coverage'}.get(family, 'Data validation')
                issue.lines.append(f'{label} failed validation.' if status == 'FAIL' else f'{label} cannot be verified.')
        if key in issues and r1.get('status') == 'PASS':
            issues[key].lines.append('Live data is current.')
        history = _object(predicates.get('C2'))
        missing_seconds = _number(history.get('missing_seconds'))
        if missing_seconds:
            gap = history.get('first_missing_second') or history.get('first_gap_hour')
            notes.append(f'{name}: {int(missing_seconds)} seconds missing' + (f' on {_date(gap)}.' if gap else '.'))
    for output in outputs:
        status = _publication_status(output)
        if status not in ('FAIL', 'UNKNOWN'):
            continue
        key, _, consumer = str(output['id']).partition(':consumer:')
        name = next((_name(source.get('name') or key) for source in sources if source['id'] == key), _name(key))
        issue = issues.setdefault(key, Issue(key, name, status, target={'view': 'sources', 'source': key}, priority=1))
        if status == 'FAIL':
            issue.status = 'FAIL'
        if output.get('data_through'):
            issue.lines.append(f'{consumer.capitalize()} files end at {_date(output["data_through"], "%H:%M")}.')
        else:
            issue.lines.append(f'{consumer.capitalize()} output cannot be verified.')
        policy = _object(output.get('publication_policy'))
        if policy.get('grace_seconds') is not None:
            issue.limits.append(f'File publication limit {_duration(policy["grace_seconds"])}')
    for condition in _objects(facts.get('conditions')):
        identity = str(condition['id'])
        scope = str(condition.get('scope') or '')
        key = 'binance_perp_trades' if identity == 'collector_capture' else next((str(source['id']) for source in sources if str(source['id']) in scope), identity)
        label = next((_name(source.get('name') or key) for source in sources if source['id'] == key), _name(scope or condition.get('check')))
        status = str(condition['status'])
        overview = {'collectors_serving': 'collectors', 'queue_bounded': 'queue', 'dagster_reachable': 'queue', 'workers_alive': 'workers'}.get(str(condition.get('check')))
        target: Document = {'view': 'overview', **({'overview': overview} if overview else {})}
        issue = issues.setdefault(key, Issue(key, label, status, target=target))
        if status == 'FAIL':
            issue.status = 'FAIL'
        keys = condition.get('keys')
        text = ' '.join(str(item) for item in keys) if isinstance(keys, list) else identity
        if 'CAPTURE_OVERLAP_BREAK' in text:
            issue.lines.insert(0, 'Capture continuity is broken.')
            issue.action = 'Check capture and repair failures in Dagit.'
        elif identity == 'dagster_unreachable':
            issue.label = 'Dagit availability'
            issue.lines.append('Dagit is unreachable.')
            issue.action = 'Check the Dagit service logs in ClickHouse.'
        elif identity.startswith('heartbeat_stale:'):
            issue.lines.append('Worker heartbeat is missing or stale.')
            issue.action = 'Check origo_monitor:workers_alive in Dagit.'
        elif identity.startswith('detector_failed:'):
            issue.lines.append('Monitor evidence is unavailable for this check.')
            issue.action = 'Check origo_monitor:' + str(condition.get('check')) + ' in Dagit.'
        elif identity == 'queue_backlog':
            queue = _object(_object(operations.get('monitor.queue_bounded')).get('evidence'))
            issue.label = 'Run queue'
            issue.lines.append(f'{queue["queued_runs"]} runs are queued.' if queue.get('queued_runs') is not None else 'The run queue exceeds its limit.')
            if queue.get('queue_threshold') is not None:
                issue.limits.append(f'Queue limit {queue["queue_threshold"]} runs')
            issue.action = 'Check queued runs in Dagit.'
        elif identity.startswith('queue_stuck:'):
            issue.lines.append('A run has exceeded its queue wait limit.')
            issue.action = 'Check the queued run in Dagit.'
        elif identity.startswith('daemon_unhealthy:'):
            issue.lines.append(f'The {scope} daemon is unhealthy.')
            issue.action = 'Check daemon health in Dagit.'
        else:
            issue.lines.append(str(condition.get('check')).replace('_', ' ').capitalize() + ': ' + text.replace('_', ' ') + '.')
    # A failed operational check still needs a row when its detailed finding was not retained.
    for check in ('dagster_reachable', 'queue_bounded', 'workers_alive', 'collectors_serving', 'publication_current'):
        result = _object(operations.get('monitor.' + check))
        evidence = _object(result.get('evidence'))
        if check == 'queue_bounded' and evidence.get('queued_runs') is not None and (_number(evidence.get('queued_runs')) or 0) <= (_number(evidence.get('queue_threshold')) or 0):
            continue
        if check == 'workers_alive' and evidence.get('workers_fresh') == evidence.get('workers_expected') and evidence.get('workers_expected') is not None:
            continue
        if result.get('status') in ('FAIL', 'UNKNOWN') and not any(str(row.get('check')) == check for row in _objects(facts.get('conditions'))):
            if check == 'publication_current' and any(_publication_status(output) in ('FAIL', 'UNKNOWN') for output in outputs):
                continue
            issues.setdefault(check, Issue(check, _name(check).capitalize(), str(result['status']),
                                          ['Check the latest origo_monitor evaluation in Dagit.'],
                                          target={'view': 'overview', 'overview': 'queue' if check in ('queue_bounded', 'dagster_reachable') else 'workers' if check == 'workers_alive' else 'collectors'}))
    waiting = [f'The {_date(day, "%d %b %H:%M" if interval == "hour" else "%d %b")} {market + " " if market else ""}{"hourly " if interval == "hour" else "daily " if interval == "day" else ""}archive{"s are" if len(names) > 1 else " is"} due at {_date(deadline, "%H:%M")}. No action required.'
               for (day, deadline, market, interval), names in waits.items() if names]
    return sorted(issues.values(), key=lambda item: (item.status != 'FAIL', item.priority, -(item.age or 0), item.key)), waiting, notes


def _link(base: str | None, target: Document) -> str | None:
    if base is None:
        return None
    from urllib.parse import urlencode
    return base + ('?' + urlencode({key: str(value) for key, value in target.items()}) if target else '')


def render_brief(summary: OperatorSummary, *, dashboard_url: str | None, dagit_written: bool) -> tuple[str, str, str]:
    facts = summary.get('current_evidence', {})
    issues, waits, notes = _issues(facts)
    if summary['status'] == 'FAIL' and not issues:
        issues.append(Issue('data_current', 'Data checks', 'FAIL', ['Check origo_monitor:data_current in Dagit.']))
    bound = facts.get('schema_version') == 1 and all(facts.get(key) == summary.get(key) for key in ('sampling_slot', 'catalog_version', 'observed_at'))
    unknown = summary['status'] == 'UNKNOWN' or not bound or bool(facts.get('reason'))
    if not bound or facts.get('reason') or (unknown and not any(issue.status == 'FAIL' for issue in issues)):
        headline = 'Current status cannot be verified.'
        lead = 'Check origo_monitor in Dagit.'
        state = 'Verification required'
    elif issues:
        first = issues[0]
        short = first.label.removeprefix('Binance ').capitalize()
        plural = short.endswith('s')
        verb = 'need' if plural else 'needs'
        headline = f'{short} {"are" if plural else "is"} {_duration(first.age)} behind.' if first.age is not None else f'{short} {verb} attention.'
        lead = ' '.join(first.lines[:2])
        state = 'Attention required'
    else:
        headline = 'Live data is current.'
        lead = ''
        state = 'Daily digest' if summary.get('digest') else 'Monitor update'
    subject = f'Origo · {state}: {headline.rstrip(".").lower()}'
    color = '#946414' if state == 'Verification required' else '#ac3a2c' if issues else '#317055'
    observed = _date(summary['observed_at'], '%d %b %Y · %H:%M')
    prepared = _date(summary.get('prepared_at'), '%H:%M')
    text: list[str] = ['ORIGO · LAW MONITOR', f'Observed {observed} · prepared {prepared} · All times UTC', state, headline]
    parts = ['<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>',
             '<body style="margin:0;background:#f2f1ed;color:#252a27;font-family:Arial,Helvetica,sans-serif"><main style="max-width:680px;margin:auto;background:white;border:1px solid #e1e3df">',
             '<div style="padding:30px 28px"><p style="margin:0;font-size:17px;font-weight:700;letter-spacing:3px;color:#243d35">◈ ORIGO <span style="float:right;font-size:10px;letter-spacing:1px;font-weight:400">LAW MONITOR</span></p>',
             f'<p style="margin:24px 0 17px;padding-top:18px;border-top:1px solid #e4e7e2;color:#747d76;font-size:10px">{html.escape(text[1])}</p>',
             f'<p style="font-size:10px;letter-spacing:1.6px;font-weight:700;color:{color}">{html.escape(state.upper())}' + (f' · {len(issues)} ISSUES' if issues else '') + '</p>',
             f'<h1 style="font-size:30px;line-height:1.2;letter-spacing:-1px;font-weight:600;margin:12px 0">{html.escape(headline)}</h1>']

    def paragraph(line: str, *, style: str = '') -> None:
        text.append(line)
        parts.append(f'<p style="font-size:13px;line-height:1.65;margin:8px 0;{style}">{html.escape(line)}</p>')

    if lead:
        paragraph(lead, style='color:#667067;font-size:14px')
    previous = _object(facts.get('previous'))
    first = issues[0] if issues else None
    old_age = _number(_object(previous.get('ages')).get(first.key)) if first else None
    if first and first.age is not None and old_age is not None and previous.get('at'):
        change = round((first.age - old_age) / 60)
        direction = f'increased {change}m' if change > 0 else f'decreased {abs(change)}m' if change < 0 else 'unchanged'
        paragraph(f'Since the {_date(previous["at"], "%H:%M")} email: delay {direction}.', style='padding:11px 14px;background:#f9eeeb;border-left:2px solid #b14c3c;color:#7e3f34;font-size:12px;margin:20px 0')
    for issue in issues:
        parts.append(f'<section data-source="{html.escape(issue.key, quote=True)}" style="border-top:1px solid #e4e7e2;padding:18px 0">')
        text.append(issue.label)
        parts.append(f'<h2 style="font-size:15px;font-weight:600;margin:0 0 8px">{html.escape(issue.label)}</h2>')
        for line in dict.fromkeys(issue.lines):
            if issue is first and line in lead:
                continue
            paragraph(line)
        if issue.limits:
            paragraph(' · '.join(dict.fromkeys(issue.limits)), style='font-size:12px;color:#6b746b')
        paragraph('Next: ' + issue.action, style='font-size:12px')
        url = _link(dashboard_url, issue.target)
        if url:
            text.append('View evidence: ' + url)
            parts.append(f'<a style="font-size:11px;color:#2d5750;font-weight:600" href="{html.escape(url, quote=True)}">View evidence ↗</a>')
        parts.append('</section>')
    sources = _objects(facts.get('sources'))
    live_sources = [source for source in sources if 'R1' in _object(source.get('predicates'))]
    operations = _object(facts.get('operations'))
    workers = _object(_object(operations.get('monitor.workers_alive')).get('evidence'))
    queue = _object(_object(operations.get('monitor.queue_bounded')).get('evidence'))
    dagit = _object(_object(operations.get('monitor.dagster_reachable')).get('evidence'))
    cubes = [_object(_object(source.get('predicates')).get('M1')) for source in sources if 'M1' in _object(source.get('predicates'))]
    metrics = [
        (f'{sum(_object(_object(source.get("predicates")).get("R1")).get("status") == "PASS" for source in live_sources)} / {len(live_sources)}' if live_sources and not unknown else 'Unknown', 'Live sources current'),
        ('Current' if cubes and all(cube.get('status') == 'PASS' for cube in cubes) and not unknown else 'Failed' if not unknown and any(cube.get('status') == 'FAIL' for cube in cubes) else 'Unknown', 'Market state cube'),
        (f'{workers["workers_fresh"]} / {workers["workers_expected"]}' if not unknown and workers.get('workers_fresh') is not None and workers.get('workers_expected') is not None else 'Unknown', 'Workers alive'),
        (str(queue['queued_runs']) if not unknown and dagit.get('reachable') is True and queue.get('queued_runs') is not None else 'Unknown', 'Queued runs'),
    ]
    parts.append('<p style="font-size:10px;letter-spacing:1.6px;color:#788077;margin:25px 0 13px">CURRENT CHECKS</p><table role="presentation" style="width:100%;background:#f3f6f1;padding:15px 10px;table-layout:fixed"><tr>')
    text.append('Current checks')
    for value, label in metrics:
        metric_color = '#946414' if value == 'Unknown' else '#ac3a2c' if value == 'Failed' else '#315d48'
        if label == 'Live sources current' and live_sources and sum(_object(_object(source.get('predicates')).get('R1')).get('status') == 'PASS' for source in live_sources) < len(live_sources) and not unknown:
            metric_color = '#ac3a2c'
        if label == 'Queued runs' and (_number(queue.get('queued_runs')) or 0) > (_number(queue.get('queue_threshold')) or 0) and not unknown:
            metric_color = '#ac3a2c'
        if label == 'Workers alive' and value != 'Unknown' and (_number(workers.get('workers_fresh')) or 0) < (_number(workers.get('workers_expected')) or 0):
            metric_color = '#ac3a2c'
        text.append(f'{value} · {label}')
        parts.append(f'<td style="vertical-align:top;padding:0 5px"><b style="font-size:14px;color:{metric_color}">{html.escape(value)}</b><br><span style="font-size:10px;color:#6c796b">{label}</span></td>')
    parts.append('</tr></table>')
    outputs = _objects(facts.get('outputs'))
    states = ['UNKNOWN' if unknown else _publication_status(output) for output in outputs]
    if states:
        failures = [output for output in outputs if _publication_status(output) == 'FAIL']
        label = 'delayed' if failures and all(_object(output.get('publication_policy')).get('reason') == 'stale' for output in failures) else 'failed'
        counts = [f'{states.count("PASS")} / {len(states)} current', f'{states.count("FAIL")} {label}', f'{states.count("EXPECTED_WAIT")} pending']
        if 'UNKNOWN' in states:
            counts.append(f'{states.count("UNKNOWN")} unknown')
        paragraph('Published outputs: ' + ' · '.join(counts) + '.', style='font-size:12px')
    for line in waits:
        paragraph('Expected wait. ' + line, style='font-size:11px;color:#626d61')
    for line in notes:
        paragraph('Historical gap. ' + line, style='font-size:11px;color:#626d61')
    for event in _objects(facts.get('events')):
        paragraph('Failure event recorded' + (f' at {_date(event["at"], "%H:%M")}' if event.get('at') else '') + ': ' + _name(event.get('scope')) + '.', style='font-size:11px;color:#626d61')
    if facts.get('event_groups_omitted'):
        paragraph(f'{facts["event_groups_omitted"]} additional historical event groups in the law monitor.', style='font-size:11px;color:#626d61')
    parts.append('</div><footer style="padding:20px 28px;border-top:1px solid #e4e7e2;background:#fbfcfa">')
    if dashboard_url:
        parts.append(f'<a href="{html.escape(dashboard_url, quote=True)}" style="display:inline-block;padding:11px 17px;background:#294b3c;border-radius:3px;color:white;font-size:12px;text-decoration:none">Open law monitor ↗</a>')
        text.append('Open law monitor: ' + dashboard_url)
    else:
        paragraph('Dashboard link not configured.')
    paragraph('Checks recorded in Dagit.' if dagit_written else 'Dagit check write did not succeed. Check origo_monitor in Dagit.', style='font-size:10px;color:#727d70')
    if not summary['coverage']['complete'] or facts.get('history_incomplete') or any(incident['kind'] == 'event_stream' and incident['status'] == 'UNKNOWN' for incident in summary['incidents']):
        paragraph('History incomplete.', style='font-size:10px;color:#727d70')
    if summary['omitted_groups']:
        paragraph(f'Detail omitted: {summary["omitted_groups"]} recorded groups.', style='font-size:10px;color:#727d70')
    status = summary['delivery_status']
    if status and status not in ('idle', 'Not recorded', 'recorded', 'acknowledged'):
        paragraph(status, style='font-size:10px;color:#727d70')
    parts.append('</footer></main></body></html>')
    body, plain = ''.join(parts), '\n\n'.join(text) + '\n'
    if max(len(body.encode()), len(plain.encode())) > BODY_BYTES:
        raise ValueError('Email brief exceeds 64 KiB; no delivery prepared.')
    return subject[:240], body, plain
