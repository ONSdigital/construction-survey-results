# Test Structure

When creating unit tests we should try to follow the same structure to simplify maintenance in the future.
These are summarised within the key points below.

Key and commonly used mocked / patched functions have been included as well. If you find yourself consistently patching other functions, consider adding them to the [Commonly Mocked / Patched items](#commonly-mocked--patched-items) section.

## Key Points
- Use classes to group tests and fixtures together, especially when we have multiple functions tested in same file.
- Single functions / tests can still be written as functions, but if you find fixtures are being being repeated think about switching to a class
- Each module has its own data folder fixture to reduce repetition, these can be found in the [conftest.py][fixtures]
- Fix any pandas warning messages before merging
- User warnings and warnings from importlib have been suppressed when running the unit tests (these will still appear when running the main pipeline!)
- Warnings suppressed can be updated in `pyproject.toml`

## Suppressed warnings

There are two warning messages suppressed when running the unit tests, these are:
 - `UserWarning`- This covers all warnings we have coded into the pipeline, tests will still fail if they expect to see a user warning, they will just not be printed to the output console.
 - `DeprecationWarning:importlib` - This only covers deprecation warnings raised by import lib, there is currently one warning which is raised by importlib and this relates to the `raz-client` package. as such we are not responsible for maintaining and dealing with this error.

## Commonly Mocked / Patched items:

### Patch: to_csv()

```python
@patch("pandas.DataFrame.to_csv")
def test_example_layout(mock_to_csv)
```

### Mock: logger:

```python
@patch("mbs_results.module.script.logger")
```


### Combined Example
```python
@patch("pandas.DataFrame.to_csv")
@patch("mbs_results.staging.stage_dataframe.logger")
class TestCheckConstructionLinks:
    def test_check_construction_links(self, mock_logger, mock_to_csv):
```

If you are experiencing errors you might have defined fixtures in the wrong order vs input arguments. Here the fixture we define first goes last in the test function arguments.
