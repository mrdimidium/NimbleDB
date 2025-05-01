# Comparative benchmark

CBench is a comparative benchmark that aims to evaluate the performance
of NimbleDB relative to other popular embedded databases.

Unlike microbenchmarks in the database itself, this benchmark serves as a baseline
for design and an attempt to estimate what exactly can be achieved.

You can also use it to select a tool on your own hardware.

The original architecture and operating modes are inspired by [ioarena](https://github.com/pmwkaa/ioarena).
Thanks to them.

## Usage

CBench is a CLI application with the ability to select a driver and database for which to run the test.


To build you need a modern C++ compiler (like gcc-15 or clang-20),
`cmake` and network access:

```bash
# Build with drivers you need only to speed up the build
cmake -S. -GNinja -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_NIMBLEDB=ON -DENABLE_LMDB=ON -DENABLE_ROCKSDB=ON

cmake --build build
```

After build just run cli, for example:
`./build/cbench/cbench -D lmdb -B crud -n 100000 -r 0 -w 0`

It would be better to run the benchmark on an unloaded system to get cleaner results.
Short runs can also give incorrect results.

More details:

```man
./build/cbench/cbench [OPTIONS]

OPTIONS:
  -h,--help                     Print this help message and exit 
  -D,--database  REQUIRED       target database, choices: debug, nimbledb, lmdb, rocksdb
  -B,--benchmark [set, get]     workload type, choices:
                                  set, get, delete, iterate, batch, crud
  -M,--sync-mode [lazy]         database sync mode, choices: sync, nosync, lazy
  -W,--wal-mode [indef]         database wal mode: indef, walon, waloff
  -P,--dirname [./_dbbench.tmp] dirname for temporaries files & reports
  -n UINT  [1000000]            number of operations
  -k UINT  [16]                 key size
  -v UINT  [32]                 value size
  -r UINT  [16]                 number of read threads, `0` to use single thread
  -w UINT  [16]                 number of write threads, `0` to use single thread
  --binary [false]              generate binary (non ASCII) values
  --continuous [false]          continuous completing mode
  --ignore-not-found [false]    ignore key-not-found error
```
