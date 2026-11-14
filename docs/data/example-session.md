# Example: one session

!!! allen "Shared Allen Institute data: no install of this package"
    This notebook reads DocDB and the shared S3 dataset only.

Look up a session in DocDB, find it in the catalogue, load its sites and a window of position and velocity from its own asset, and plot them together.
It is [Find sessions](find-sessions.md) and [Load tables](load-tables.md) end to end, in Polars.

<iframe class="notebook-frame" src="../notebooks/session_overview.html" title="One session notebook"></iframe>

[Open full screen](../notebooks/session_overview.html){ target=_blank } · run it yourself (dependencies are declared in the file):

```bash
uvx marimo edit --sandbox docs/notebooks/session_overview.py
```
