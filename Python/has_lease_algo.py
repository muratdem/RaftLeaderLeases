"""has_lease algorithm, for inclusion in the paper.

See lease_guard.py for the executable version.
"""

def has_lease(self, for_writes: bool) -> bool:
  if self.role is not LEADER:
    return False
  # Need lease to advance commit index, need to
  # advance it to get a lease! So calc it from
  # match_index. If I heard of a higher commit index
  # while I was a follower, use that.
  commit_idx = max(
    self.commit_idx, median(self.match_index))
  entries = self.log[:commit_idx + 1]
  # Compensate for max clock drift.
  lease_start = (self.clock.now()
   - lease_duration * (1 + max_clock_error))
  # Last committed entry's before lease timeout?
  if entries[-1].local_ts <= lease_start:
    return False
  # I can serve reads with inherited lease from old
  # leader, but must commit my own entry for writes.
  if for_writes:
    if entries[-1].term != self.current_term:
      # I haven't committed an entry yet.
      return False
    # Last entry I got from the previous leader.
    prior_entry = next(
      (e for e in reversed(entries)
       if e.term != self.current_term))
    # local_ts is my clock when I replicated entry.
    if prior_entry.local_ts >= lease_start:
      # Previous leader still has write lease.
      return False
  return True
