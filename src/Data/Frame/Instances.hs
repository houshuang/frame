{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE RankNTypes #-}

module Data.Frame.Instances (
  col,
  (!),

  blocks,
  index,
  alter,
) where

import Data.Frame.HFrame
import Data.Frame.Internal (Columnable, hdfdata, hdfindex)
import qualified Data.Frame.Internal as I
{-import qualified Data.Vector as VB-}
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

import Data.Hashable (Hashable(..))
import qualified Data.HashMap.Strict as M

import Control.Applicative
import Control.Lens ((&), (^.), (^?), (.~), at, (<&>), _Just, to, over, traverse, (%~))
import Control.Lens (Index, IxValue, Ixed(..), At(..))

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

-------------------------------------------------------------------------------
-- Lens Getters
-------------------------------------------------------------------------------

-- | View a column.
col k = at k._Just.to(fromBlock)

-- | Select a column.
(!) :: (Columnable a, Ord a, FromBlock b) => HDataFrame k a -> a -> Result [b]
df ! k = df ^. col k

-- | Alter the block structure
blocks:: (forall v a. VG.Vector v a => v a -> v a) -> HDataFrame a k -> HDataFrame a k
{-blocks:: (Block -> Block) -> HDataFrame a k -> HDataFrame a k-}
blocks f df = df & hdfdata.traverse %~ (btraverse f)

-- | Alter the index
index :: (VU.Vector a -> VU.Vector b) -> HDataFrame a k -> HDataFrame b k
index f df = df & hdfindex %~ f

alter :: (forall v a. VG.Vector v a => v a -> v a)
      -> (VU.Vector a -> VU.Vector a)
      -> HDataFrame a k -> HDataFrame a k
alter f g df = df & (hdfdata.traverse %~ (btraverse f))
                  . (hdfindex %~ g)
