{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}

module Data.Frame.Instances (
  get,

  col,
  cols,
  colsWhere,

  (!),
  row,
  rows,
  rowsWhere,

  drop,
  take,
  head,
  tail,
  last,
  vFilter,

  select,
  slice,

  -- XXX: don't export
  _Index,
  _Data,
) where

import Prelude hiding (drop, take, head, last, tail, null)

import Data.List (genericLength)
import Data.Frame.HFrame
import Data.Frame.Internal (Columnable, _Data, _Index, _filterKeys)
import qualified Data.Frame.Internal as I

import qualified Data.Vector as VB
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

import Data.Hashable (Hashable(..))
import qualified Data.HashMap.Strict as M

import Control.Applicative
import Control.Lens hiding (index)
import Control.Lens (Index, IxValue, Ixed(..), At(..))
import Control.Lens.Empty (AsEmpty)

type instance IxValue (HDataFrame k a) = Block
type instance Index (HDataFrame k a) = a

instance (Eq a, Hashable a) => Ixed (HDataFrame k a) where
  ix k f (HDataFrame m ix) = case M.lookup k m of
     Just v  -> fmap (\v' -> HDataFrame (M.insert k v' m) ix) (f v)
     Nothing -> pure (HDataFrame m ix)

instance (Ord a, Hashable a) => At (HDataFrame k a) where
  at k f orig@(HDataFrame m ix) = f mv <&> \r -> case r of
    Nothing -> maybe orig (const $ HDataFrame (M.delete k m) ix) mv
    Just v' -> HDataFrame (M.insert k v' m) ix
    where mv = M.lookup k m

instance AsEmpty (HDataFrame k a) where
  _Empty = nearly (HDataFrame (M.empty) (VB.empty)) null

-------------------------------------------------------------------------------
-- Getters
-------------------------------------------------------------------------------

-- | Extract a column to a Haskell list.
get k = ix k.to(fromBlock)

-- | Select a column.
(!) :: (Columnable a, Ord a, FromBlock b) => HDataFrame k a -> a -> Result [b]
df ! k = df ^. get k

-------------------------------------------------------------------------------
-- Filters
-------------------------------------------------------------------------------

colsWhere :: (k -> Bool) -> HDataFrame i k -> HDataFrame i k
colsWhere k = _Data %~ _filterKeys k

col :: (Eq k) => k -> HDataFrame i k -> HDataFrame i k
col k = colsWhere (==k)

cols :: (Eq k) => [k] -> HDataFrame i k -> HDataFrame i k
cols ks = colsWhere (`elem` ks)

rowsWhere :: (i -> Bool) -> HDataFrame i k -> HDataFrame i k
rowsWhere f df =
  let indexMatcher = vFilter f (df ^. _Index) in
  -- XXX why does inference break under CSE here?
  df & (_Data.traverse %~ btraverse (vFilter f (df ^. _Index)))
     . (_Index %~ indexMatcher)

row :: Eq i => i -> HDataFrame i k -> HDataFrame i k
row i = rowsWhere (==i)

rows :: Eq i => [i] -> HDataFrame i k -> HDataFrame i k
rows i = rowsWhere (`elem` i)

-- Build a hitmap matching a predicate and filter all rows based on the index.
vFilter :: (VG.Vector v1 a, VG.Vector v1 Bool, VG.Vector v a1)
           => (a -> Bool)
           -> v1 a
           -> v a1
           -> v a1
vFilter f xs ys = VG.ifilter (\i _ -> (ms VG.! i) == True) ys
  where ms = VG.map f xs

-------------------------------------------------------------------------------
-- Mutators
-------------------------------------------------------------------------------

-- This doesn't in general maintain the invariant that the index is the same length as all the blocks.

_blocks :: (forall v a. VG.Vector v a => v a -> v a)
        -> HDataFrame a k
        -> HDataFrame a k
_blocks f = _Data.traverse %~ btraverse f

_index :: (VB.Vector a -> VB.Vector a)
       -> HDataFrame a k
       -> HDataFrame a k
_index g = _Index %~ g

_alter :: (forall v a. VG.Vector v a => v a -> v a)
       -> (VB.Vector a -> VB.Vector a)
       -> HDataFrame a k
       -> HDataFrame a k
_alter f g = _blocks f . _index g

-------------------------------------------------------------------------------
-- Functions
-------------------------------------------------------------------------------

drop :: Int -> HDataFrame a k -> HDataFrame a k
drop n = _alter (VG.drop n) (VG.drop n)

take :: Int -> HDataFrame a k -> HDataFrame a k
take n = _alter (VG.take n) (VG.take n)

select :: Int -> HDataFrame a k -> HDataFrame a k
select k = _alter (VG.singleton . (VG.! k)) (VG.singleton . (VG.! k))

slice :: Int -> Int -> HDataFrame a k -> HDataFrame a k
slice a b = _alter (VG.slice a b) (VG.slice a b)

head :: HDataFrame a k -> HDataFrame a k
head = _alter (VG.singleton . VG.head) (VG.singleton . VG.head)

last :: HDataFrame a k -> HDataFrame a k
last = _alter (VG.singleton . VG.last) (VG.singleton . VG.last)

tail :: HDataFrame a k -> HDataFrame a k
tail = _alter (VG.tail) (VG.tail)

avg :: (Real a, Fractional b) => [a] -> b
avg xs = realToFrac (sum xs) / genericLength xs
