# Recent raw-trade evidence

`2026-09-21/` preserves the genuine recent-response bundle and historical crosscheck
from commit `63bc1c3` (`tests/fixtures/steady_state/perp_recent/`). The original
provenance and compressed bytes are unchanged. The added archive extract was
obtained on 2026-09-28 after verifying the complete official ZIP against its
published checksum. Its two complete minutes contain 9,948 and 6,647 rows.
The 499-row historical crosscheck is partial; neither it nor these minutes meets
the >=17,967-row busy acceptance requirement.

`exploratory-2026-09-28/` records every attempt in a preregistered fifteen-minute
local capture using the existing transport with explicit local source binding.
It does not measure the production host's latency or resource limits. Registration
precedes requests; response bytes, hashes, timings and headers are preserved.
The day's archive cannot be checked before publication the next day.

The mandatory busy tests require `acceptance.json` referencing a genuine qualifying
corpus, its full historical rows, checksum-verified archive extract and measured
process/resource evidence. That manifest is deliberately absent until those facts
exist. Small captures, archive-derived responses and skipped tests cannot satisfy
that gate. Original trades are used for unit tests; tests label deliberate faults
separately from provider evidence.
