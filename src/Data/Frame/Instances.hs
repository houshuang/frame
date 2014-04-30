{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE NoMonomorphismRestriction #-}

module Data.Frame.Instances (
  col,
  (!),

  blocks,
  index
) where

import Data.Frame.HFrame
import Data.Frame.Internal (Columnable, hdfdata, hdfindex)
import qualified Data.Frame.Internal as I

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
blocks :: (Block -> Block) -> HDataFrame a k -> HDataFrame a k
blocks f df = df & hdfdata.traverse %~ f

-- | Alter the index
index :: (I.Index a -> I.Index b) -> HDataFrame a k -> HDataFrame b k
index f df = df & hdfindex %~ f
