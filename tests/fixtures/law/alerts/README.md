# Recorded alert evidence

These JSON files preserve genuine public `/law` HTTP response bytes from production.
`manifest.json` records each origin URL, HTTP capture time, interval, full deployed
commit, checksum, byte count and any transformations. No redactions were needed:
the public responses contain no authentication material or recipient addresses.
All deployed commits were verified as Git commit objects reachable from `origin/main`.
CI must fetch repository history before checking these objects.

`current.json` and `catalog.json` are a matched current capture. The two `current-06*`
files are prior investigation captures; their original HTTP request metadata is absent.
The R1 history includes real failures, recurrence, UNKNOWN readings and recovery;
C1 contains expected waits; M1/M2 provide their actual recorded application evidence.
No timestamps, measurements, gate statuses or missing slots have been manufactured.

Legacy gate history has no recorded notification eligibility. It cannot prove that a
particular past notification passed its original hold. These files are neither received
email evidence nor proof of the reported email burst's cause.
