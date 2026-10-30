# Building Documentation

From the repository root, install the documentation dependencies and build the
HTML documentation:

```bash
python -m pip install -e ".[doc]"
cd docs
make html
```

Outputs to `docs/build/html/index.html` from the repository root.
