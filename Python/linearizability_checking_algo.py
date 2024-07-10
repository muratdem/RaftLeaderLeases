"""Linearizability checking algorithm, for inclusion in the paper.

See run_with_params.py for the executable version.
"""

def check_linearizability(log):
  sys.setrecursionlimit(1000000)
  for e in log:
    assert e.start_ts <= e.execution_ts
    assert e.execution_ts <= e.end_ts
  sorted_log = sorted(log, key=lambda e:
    (e.execution_ts, e.start_ts, e.end_ts))
  for k in set(e.key for e in sorted_log):
    # Ignore failed reads.
    filtered_log = [e for e in sorted_log if
      e.key == k and (
        e.op_type is ListAppend or e.success)]
    result = linearize(
      filtered_log, value=[], failed=[])
    if not result:
      raise Exception("not linearizable!")

def linearize(log, value, failed):
  """Try to linearize log for one key."""
  if len(log) == 0:
    return True
  # For simultaneous events, try any of them 1st.
  first_execution_ts = log[0].execution_ts
  for i, e in enumerate(log):
    if e.execution_ts != first_execution_ts:
      # We've tried all entries with min exec
      # times, linearization failed.
      break
    # Try linearizing "entry" at log's start.
    log_prime = log.copy()
    log_prime.pop(i)
    if e.op_type is ListAppend and not e.success:
      # Assume failed write has no effect now,
      # value'=value, but it might happen later.
      if linearize(
          log_prime, value, failed + [e]):
        return True

    # Assume the write succeeded. Even if !success
    # it might eventually commit.
    if e.op_type is ListAppend:
      if linearize(
          log_prime, value + [e.value], failed):
        return True

    if e.op_type is Read:
      # Could this query have run with this value?
      if value != e.value:
        continue  # "e" can't be linearized here.

      # Try to linearize rest of log.
      if linearize(log_prime, value, failed):
        return True

  # Linearization is failing so far, maybe one of
  # the failed writes took effect?
  for f in failed:
    failed_prime = failed.copy()
    failed_prime.remove(f)
    if linearize(
        log, value + [f.value], failed_prime):
      return True

  return False
