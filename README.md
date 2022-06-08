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
