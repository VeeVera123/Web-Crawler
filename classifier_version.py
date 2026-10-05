"""Single source of truth for the classification-rules version stamped onto
every row of the `jobs` table (jobs.classifier_version).

Deliberately its own dependency-free module: supabase_handler.py (which
writes the stamp) must not import classifier.py/config.py (classifier.py
already imports supabase_handler transitively via groq_coordination, and
config.py hard-requires every provider key at import time).

BUMP THIS whenever the deterministic location/restriction rules or the Rank 4
admission policy change in a way that should be applied to jobs that are
already stored — e.g. a new hard-override regex, a policy change like the
2026-10 "non-curated country is never rescuable" rule. Every stored row
whose classifier_version is lower is re-checked ONCE (see revalidate.py:
deterministic, veto-only, no LLM calls) the next time its job is seen on its
board, then stamped with the new value.

History:
  1 — 2026-10-05: introduced with revalidate.py (after the Insurity/Ping
      Identity/Kenya/India Rank 4 leaks). Every pre-existing row is 0.
"""
CLASSIFIER_VERSION = 1
