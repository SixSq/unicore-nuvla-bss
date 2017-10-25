### Nuvla BSS for UNICORE TSI

Implementation of UNICORE TSI BSS for Nuvla service.

### Development environment

Works with Python `>= 2.7`.

```
pip install -r requirements.txt -r requirements-dev.txt
```

### Testing

```
make test
```

The live tests use Nuvla instance directly and hence require user 
credentials.  The credentials can be set in the test files directly.

```
make test-live
```
