frame
=====

A simple experiment in building a Pandas style dataframe in Haskell.


Data.Frame.HFrame
-----------------

A ``HDataFrame`` is a named collection of columns containing potentially hetereogeneously-typed uniform sized
vectors with row labeling information which can be used to index data.

It is primarily a low-performance data-structure used to for exploratory analysis of data in similar fashion
to a spreadsheet.

Install
-------

```bash
$ cabal configure
$ cabal install --only-dependencies
$ cabal build
```

For the test suite:

```bash
$ cabal configure --enable-tests
$ cabal test
```

For the documentation:

```bash
$ cabal haddock
```

License
-------

MIT
