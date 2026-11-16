# Example: one raw stream

!!! allen "Shared Allen Institute data: reads the public raw asset"
    This notebook looks up the session's raw asset in the session catalogue and reads it from public S3.
    The contract path needs `aind-behavior-vr-foraging[data]`, which the notebook declares.

Read `behavior/OperationControl/IsStopped.csv` from a session's raw asset, once with the path typed by hand and once with the path taken from the data contract, check that both return the same data, and plot it.
It is [Load raw streams](load-raw.md) end to end, in Polars.

<iframe class="notebook-frame" src="../notebooks/raw_stream.html" title="One raw stream notebook"></iframe>

[Open full screen](../notebooks/raw_stream.html){ target=_blank } · run it yourself (dependencies are declared in the file):

```bash
uvx marimo edit --sandbox docs/notebooks/raw_stream.py
```
