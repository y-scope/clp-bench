# MongoDB methodology

## Basics

Version: [6.0.19][download]

## Setup

We use `mongosh`, `mongod`, `mongoexport`, and `mongoimport` to ingest the dataset and perform query benchmarking. No 
special preprocessing is required for the dataset.

## Specifics

We use the [ZStandard][zstandard] compressor with the [default compression level][compression_level]
set to 3.


[compression_level]: https://source.wiredtiger.com/3.1.0/compression.html#:~:text=%22extensions%3D%5B%2Fusr%2Flocal%2Flib%2Flibwiredtiger_zstd.sothe%20additional%20configuration%20argument%20compression_level%20.
[download]: https://hub.docker.com/layers/mongodb/mongodb-enterprise-server/6.0.19-ubuntu2204/images/sha256-2cb24cf730152d9b0710841a9ae6a50732c48fa6c7111bf7b4a84a5a8df5bc01?context=explore
[zstandard]: https://www.mongodb.com/docs/manual/reference/program/mongod/#std-option-mongod.--wiredTigerCollectionBlockCompressor
