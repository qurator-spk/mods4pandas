```
pip install -r requirements-dev.txt
```

To run tests:
```
pip install -e .
pytest
```

To run a test with profiling:

1. Make sure graphviz is installed
2. Run pytest with with profiling enabled:
  ```
  pytest --profile-svg -k test_page_info
  ```

# How to use pre-commit

This project optionally uses [pre-commit](https://pre-commit.com) to check commits. To use it:

- Install pre-commit, e.g. `pip install -r requirements-dev.txt`
- Install the repo-local git hooks: `pre-commit install`
