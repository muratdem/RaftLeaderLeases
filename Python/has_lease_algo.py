"""has_lease algorithm, for inclusion in the paper.

See lease_guard.py for the executable version.
"""

def has_lease(self, for_commit: bool) -> bool:
  if self.state != "primary": return False
  if for_commit:
    # Compensate for max clock error.
    lease_start = (self.clock.now()
      - lease_timeout - max_clock_error)
    for e in reversed(self.log):
      if e.term == self.current_term:
        continue
      if e.timestamp >= lease_start:
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
    if entry.timestamp <= (self.clock.now()
        - lease_timeout):
      # Last committed entry is too old.
      return False
  return True
