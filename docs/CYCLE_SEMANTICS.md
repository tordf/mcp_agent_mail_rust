# `br` versus `bv` Cycle Semantics

**Bead**: br-97gc6.3.2.2
**Parent**: br-97gc6.3.2 (Reconcile discrepancy between br dep cycles and bv graph health)
**Last verified**: 2026-04-03

---

## 1. Summary

`br dep cycles` and `bv --robot-insights` **disagree** on cycle count because they
operate on **different graph scopes**:

| Tool | Command | Graph scope | Cycles found |
|------|---------|-------------|--------------|
| `br` | `br dep cycles` | All issues (open, in_progress, **closed**, tombstone) | **8** |
| `bv` | `--robot-insights` `.Cycles` | Open + in_progress only | **0** |

The mismatch is **expected and by design**: bv builds an *actionable* graph for
triage (only work that still matters), while br walks the full dependency ledger
(structural audit of the complete history).

**No code fix is needed.** The 8 cycles involve exclusively closed issues and have
no operational impact.

---

## 2. Graph Scope Differences

### What `bv` includes

`bv --robot-insights` builds a directed graph from all issues but **excludes closed
and tombstoned issues from cycle detection**. Its purpose is to surface actionable
blockers — cycles among completed work are irrelevant to triage.

Evidence:
- `Cycles` field: `null` (no cycles in the actionable subgraph)
- `status.Cycles.state`: `"computed"` (analysis ran; it did not skip)
- `advanced_insights.cycle_break.cycle_count`: `0`
- `advanced_insights.cycle_break.advisory`: "No cycles detected — dependency graph is a proper DAG."

### What `br` includes

`br dep cycles` walks **every** dependency edge in the database, regardless of
issue status. It enumerates all elementary cycles in the full historical graph.

Evidence (2026-04-03):
```
$ br dep cycles
Warning: Found 8 dependency cycle(s):
  1. br-246y -> br-2avs -> br-3c7vp -> br-3ibsu -> br-dsdzo -> br-246y
  2. br-246y -> br-2avs -> br-3c7vp -> br-3ibsu -> br-246y
  3. br-246y -> br-2avs -> br-3c7vp -> br-246y
  4. br-2ei.5 -> br-2ei.5.7 -> br-2ei.5.7.1 -> br-2ei.5
  5. br-2ei.5 -> br-2ei.5.7 -> br-2ei.5.7.2 -> br-2ei.5
  6. br-2ei.5 -> br-2ei.5.7 -> br-2ei.5.7.3 -> br-2ei.5
  7. br-2ei.5 -> br-2ei.5.7 -> br-2ei.5.7.4 -> br-2ei.5
  8. br-3vwi.6.2 -> br-3vwi.7 -> br-3vwi.7.5 -> br-3vwi.6.2
```

### All cycle members are CLOSED

Every issue participating in the 8 cycles is in `CLOSED` status:

| Cycle group | Issues | Status |
|-------------|--------|--------|
| br-246y cluster (cycles 1-3) | br-246y, br-2avs, br-3c7vp, br-3ibsu, br-dsdzo | All CLOSED |
| br-2ei.5 cluster (cycles 4-7) | br-2ei.5, br-2ei.5.7, br-2ei.5.7.{1,2,3,4} | All CLOSED |
| br-3vwi cluster (cycle 8) | br-3vwi.6.2, br-3vwi.7, br-3vwi.7.5 | All CLOSED |

This is why bv reports zero cycles — its open-issue subgraph is a proper DAG.

---

## 3. Root Causes of the Historical Cycles

The 8 cycles fall into 3 distinct clusters:

### Cluster A: br-246y (5-node, 3 cycle variants)

Circular `blocks` edges among 5 closed issues. Likely introduced during
aggressive multi-agent bead creation sessions where mutual dependencies
were recorded before the DAG constraint was enforced.

### Cluster B: br-2ei.5 (parent-child, 4 cycle variants)

Parent `br-2ei.5` has child `br-2ei.5.7`, whose own children
(`br-2ei.5.7.{1-4}`) each have a dependency back to `br-2ei.5`.
This is a classic "subtask depends on parent epic" pattern — structurally
cyclic but semantically intentional (subtasks can't start until the epic
scope is defined, and the epic can't close until subtasks complete).

### Cluster C: br-3vwi (3-node, 1 cycle)

`br-3vwi.6.2 -> br-3vwi.7 -> br-3vwi.7.5 -> br-3vwi.6.2`. A mutual
blocking relationship among TUI V2 tasks.

---

## 4. Decision

**The mismatch is expected.** No bug exists in either tool.

| Question | Answer |
|----------|--------|
| Are br and bv using different graph scopes? | **Yes.** br = all issues; bv = open/in_progress only. |
| Is the mismatch expected? | **Yes.** bv filters for triage relevance. |
| Do the cycles affect active work? | **No.** All cycle members are CLOSED. |
| Is a code fix needed? | **No.** |
| Should the cycles be cleaned up? | **Optional.** They are harmless in closed state. If desired, break each cycle by removing one `blocks` edge per cluster. |

---

## 5. Verification Commands

```bash
# br: full-history cycle detection (includes closed issues)
br dep cycles
br dep cycles --json | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'cycles: {d[\"count\"]}')"

# bv: actionable-graph cycle detection (excludes closed issues)
bv --robot-insights | python3 -c "
import sys,json
d=json.load(sys.stdin)
cb = d.get('advanced_insights',{}).get('cycle_break',{})
print(f'cycle_count: {cb.get(\"cycle_count\",\"N/A\")}')
print(f'advisory: {cb.get(\"advisory\",\"N/A\")}')
"

# Verify cycle member status
br dep cycles --json | python3 -c "
import sys,json,subprocess
d=json.load(sys.stdin)
ids = set()
for c in d['cycles']:
    ids.update(c)
for i in sorted(ids):
    r = subprocess.run(['br','show',i], capture_output=True, text=True)
    st = 'UNKNOWN'
    for line in r.stdout.splitlines():
        if 'CLOSED' in line: st='CLOSED'
        elif 'IN_PROGRESS' in line: st='IN_PROGRESS'
        elif 'OPEN' in line: st='OPEN'
    print(f'{i}: {st}')
"
```

---

## 6. If Cycles Recur in Open Issues

If `bv --robot-insights` ever reports non-null `Cycles` or non-zero
`cycle_count`, that means active work has circular dependencies. To fix:

1. Run `bv --robot-suggest --suggest-type cycle` for break suggestions
2. Check `bv --robot-insights | jq '.advanced_insights.cycle_break'`
3. Remove the weakest edge in each cycle (usually convert `blocks` to `related`)
4. Verify with `br dep cycles` that the full-history count decreased
