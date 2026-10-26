# Source layout

`apixis/` contains the installable package and `tests/` contains its test suite.
The build configuration and release README are at the repository root.

Run installation, tests, and builds from the repository root:

```bash
python -m pip install -e ".[dev]"
python -m pytest
python -m build
```

See [README](../README.md) and [the API documentation](../docs/core/README.md).
