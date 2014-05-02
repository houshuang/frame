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

Usage
-----

Subject to change but the initial API.

```bash

λ: import import Data.Frame.HFrame
λ: import import Data.Frame.CSV

λ: Right frame <- fromCsvHeaders "examples/titanic.csv"

λ: frame & cols ["sex", "name", "survived", "age", "boat"] . rows [1..20]
    boat  age          survived  name                             sex   
1   11    0.916700006  1         Allison, Master. Hudson Trevor   male  
2         2.0          0         Allison, Miss. Helen Loraine     female
3         30.0         0         Allison, Mr. Hudson Joshua Crei  male  
4         25.0         0         Allison, Mrs. Hudson J C (Bessi  female
5   3     48.0         1         Anderson, Mr. Harry              male  
6   10    63.0         1         Andrews, Miss. Kornelia Theodos  female
7         39.0         0         Andrews, Mr. Thomas Jr           male  
8   D     53.0         1         Appleton, Mrs. Edward Dale (Cha  female
9         71.0         0         Artagaveytia, Mr. Ramon          male  
10        47.0         0         Astor, Col. John Jacob           male  
11  4     18.0         1         Astor, Mrs. John Jacob (Madelei  female
12  9     24.0         1         Aubart, Mme. Leontine Pauline    female
13  6     26.0         1         Barber, Miss. Ellen \"Nellie\"   female
14  B     80.0         1         Barkworth, Mr. Algernon Henry W  male  
15        na           0         Baumann, Mr. John D              male  
16        24.0         0         Baxter, Mr. Quigg Edmond         male  
17  6     50.0         1         Baxter, Mrs. James (Helene DeLa  female
18  8     32.0         1         Bazzani, Miss. Albina            female
19  A     36.0         0         Beattie, Mr. Thomson             male  
20  5     37.0         1         Beckwith, Mr. Richard Leonard    male  

λ: let ages = frame ^. get "age" :: Result [Maybe Double]

λ: Data.List.take 5 <$> ages
Success [Just 29.0,Just 0.916700006,Just 2.0,Just 30.0,Just 25.0]

λ: avg <$> fmap catMaybes ages
Success 29.881134512434034
```

License
-------

Copyright 2013-2014 
Stephen Diehl ( stephen.m.diehl@gmail.com )

Released under the MIT License.
