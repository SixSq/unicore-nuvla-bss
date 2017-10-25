### Nuvla BSS for UNICORE TSI

Implementation of UNICORE TSI BSS for Nuvla service.

### Development environment

Works with Python `>= 2.7`. To install requirements run

```
make init
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
