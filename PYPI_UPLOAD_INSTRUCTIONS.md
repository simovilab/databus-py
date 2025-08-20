# PyPI Upload Instructions

## Step 1: Configure PyPI Credentials

Create a file at `~/.pypirc` with your credentials:

```ini
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = pypi-YOUR_API_TOKEN_HERE

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-YOUR_TEST_API_TOKEN_HERE
```

## Step 2: Upload to Test PyPI (Recommended First)

```bash
twine upload --repository testpypi dist/*
```

## Step 3: Test Installation from Test PyPI

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ databus
```

## Step 4: Upload to Production PyPI

Once testing is successful:

```bash
twine upload dist/*
```

## Step 5: Verify Installation

```bash
pip install databus
```

## Current Package Status

- ✅ Package built successfully
- ✅ All source code included
- ✅ CLI entry point configured
- ✅ Dependencies properly specified
- ⚠️  Minor metadata warning (license-file field) - this shouldn't prevent upload
- 🚀 Ready for upload!

The package is ready to be uploaded. The license-file warning is a minor issue that shouldn't prevent successful upload to PyPI.
