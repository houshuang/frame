module Test (
  frame1
) where

import Prelude hiding (head, tail, take)

import Data.Text (Text)
import Data.Frame.HFrame
import Data.Frame.Interactive
import Data.Frame.CSV
import Data.Frame.Instances
import Control.Lens ((&), (^.), (^?), (.~), _Just, at)
import Control.Lens.At

import qualified Data.Vector.Generic as VG

frame1 :: HDataFrame Int String
frame1 = Data.Frame.HFrame.fromMap [
    ("a", r1),
    ("b", r2),
    ("c", r3)
  ]
  where
    r1 = iblock ([0..5])
    r2 = bblock [True, True, False, False, True, True]
    r3 = mblock (iblock [0..5]) [True, True, False, False, True, True]

frame2 :: HDataFrame Int String
frame2 = Data.Frame.HFrame.fromMap [
    ("d", r1),
    ("e", r2),
    ("f", r3)
  ]
  where
    r1 = iblock [0..25]
    r2 = bblock [False, False, False, False, True, True]
    r3 = mblock (iblock [100..112]) (replicate 12 True)

frame3 :: HDataFrame Double String
frame3 = Data.Frame.HFrame.fromMap [
    ("a", dblock [0..500]),
    ("b", iblock [0..500])
  ]

test4 :: Result [Int]
test4 = frame1 ^. get "a"

test5 = frame1 & sans "a"
test6 = frame1 & colsWhere (=="a") . head
test7 = frame1 & rowsWhere even . rowsWhere (>=2)

example1 :: Result ([Int], [Bool], [Maybe Int])
example1 = do
  a <- frame1 ^. get "a"
  b <- frame1 ^. get "b"
  c <- frame1 ^. get "c"
  return (a, b, c)

example2 :: IO (Result ([Int], [Text]))
example2 = do
  Right contents <- fromCsvNoHeaders "examples/presidents.csv"
  return $ do
    a <- contents ^. get 1
    b <- contents ^. get 2
    return (a, b)
