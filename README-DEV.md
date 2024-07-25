```
pip install -r requirements-test.txt
```

To run tests:
```
pytest
```

To run a test with profiling:

1. Make sure graphviz is installed
2. Run pytest with with profiling enabled:
  ```
  pytest --profile-svg -k test_page_info
  ```
