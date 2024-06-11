"""has_lease algorithm, for inclusion in the paper.

See lease_guard.py for the executable version.
"""

def has_lease(self, for_commit: bool) -> bool:
  if self.role is not PRIMARY: return False
  # Compensate for max clock drift.
  lease_start = (self.clock.now()
    - lease_timeout * (1 + max_clock_error))
  if for_commit:
    # If I replicated a past term's entry
    # < lease_timeout ago, past leader may
    # have a lease.
    for e in reversed(self.log):
      if e.term == self.current_term:
        continue
      if e.time_when_i_replicated >= lease_start:
        # Past leader may have lease.
        return False
      # Don't need to check older entries.
      break
  else:
    # Check if I have a lease for reads.
    if self.commit_index == -1:
      return False  # Can't read without commits.
    # "Inherited read lease" means I can read
    # while a prior leader's lease is valid.
    entry = self.log[self.commit_index]
    if entry.time_when_i_replicated < lease_start:
      # Last committed entry is too old.
      return False
  return True
