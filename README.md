[![Build Status](https://github.com/saalfeldlab/n5-blosc/actions/workflows/build.yml/badge.svg)](https://github.com/saalfeldlab/n5-blosc/actions/workflows/build.yml)

[![Build Status](https://github.com/saalfeldlab/n5-blosc/actions/workflows/build-main.yml/badge.svg)](https://github.com/saalfeldlab/n5-blosc/actions/workflows/build-main.yml)

# n5-blosc
Blosc compression for N5.

This library wraps the [JBlosc](https://github.com/lasersonlab/jblosc) interface for C-Blosc as an [N5 compression interface](https://github.com/saalfeldlab/n5/blob/master/src/main/java/org/janelia/saalfeldlab/n5/Compression.java).  [JBlosc](https://github.com/lasersonlab/jblosc) depends on `libblosc1`.  On Ubuntu 18.04 or later, install with:
```
sudo apt-get install -y libblosc1
```
On other platforms, please check the [installation instructions](https://github.com/lasersonlab/JBlosc/blob/master/README.md) for [JBlosc](https://github.com/lasersonlab/jblosc).

Build and install:
```
mvn clean install
```

## json serialization

N5's blosc compression is serialized with a json object with

Required name:

* `"type"` must have value `"blosc"`

Optional names:

* `"clevel"` (Default : `6`)
* `"blocksize"` (Default : `0` = auto)
* `"cname"`    (Default: `"blosclz"`)
    * One of ["blosclz", "lz4", "lz4hc", "zlib", "zstd"], [see here](https://blosc.org/c-blosc2/reference/utility_variables.html#compressor-names)
* `"nthreads"` (Default: `1`)
* `"shuffle"` (Default: `0`)
    * 0 = NOSHUFFLE
    * 1 = SHUFFLE
    * 2 = BITSHUFFLE

### Example

```
{
  "compression": {
    "type": "blosc",
    "clevel": 6,
    "blocksize": 0,
    "cname": "blosclz",
    "nthreads": 1,
    "shuffle": 0
  }
}
```
