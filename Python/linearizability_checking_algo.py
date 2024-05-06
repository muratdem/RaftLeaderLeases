"""Linearizability checking algorithm, for inclusion in the paper.

See run_raft_with_params.py for the executable version.
"""

def check_linearizability(history):
  if linearize(history, {}) is None:
    raise Exception("not linearizable!")

def linearize(history, model):
  if len(history) == 0: return history
  # For simultaneous events, try each order.
  first_entries = [e for e in history
    if e.execution_ts == history[0].execution_ts]
  for i, entry in enumerate(first_entries):
    assert entry.start_ts <= entry.execution_ts
    assert entry.execution_ts <= entry.end_ts
    # Try linearizing "entry" at history's start.
    history_prime = history.copy()
    history_prime.pop(i)
    # Try assuming failed write has no effect.
    if (entry.op_type is ListAppend
        and not entry.success):
      linearization = linearize(history_prime, model)
      if linearization is not None:
        # Omit entry entirely.
        return linearization
    # Try assuming failed write succeeded.
    # Even if !success it might eventually commit.
    if entry.op_type is ListAppend:
      model_prime = deepcopy(model)
      model_prime[entry.key].append(entry.value)
      # Try to linearize the rest of the
      # history with the model in this state.
      linearization = linearize(
        history_prime, model_prime)
      if linearization is not None:
        return [entry] + linearization
    if entry.op_type is Read:
      # What would the query return if we ran it now?
      if model[entry.key] != entry.value:
        # "entry" can't be linearized first.
        continue
      # Try to linearize the rest of the history
      # with the model in this state.
      linearization = linearize(history_prime, model)
      if linearization is not None:
        return [entry] + linearization
  return None
