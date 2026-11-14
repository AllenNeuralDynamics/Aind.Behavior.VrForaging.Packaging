# Example: one subject across sessions

!!! allen "Shared Allen Institute data: no install of this package"
    This notebook reads DocDB and the shared S3 dataset only.

Find all of a subject's sessions in DocDB, load their reward sites from the shared `sites.parquet`, and plot the stop rate and rewards per session.
Change `SUBJECT_ID` and `SINCE` at the top to look at another animal.

<iframe class="notebook-frame" src="../notebooks/subject_over_sessions.html" title="One subject notebook"></iframe>

[Open full screen](../notebooks/subject_over_sessions.html){ target=_blank } · run it yourself (dependencies are declared in the file):

```bash
uvx marimo edit --sandbox docs/notebooks/subject_over_sessions.py
```
