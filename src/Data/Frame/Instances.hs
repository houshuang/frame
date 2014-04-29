{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE NoMonomorphismRestriction #-}


module Data.Frame.Instances where

import Data.Frame.HFrame
import qualified Data.HashMap.Strict as M

import Control.Applicative

import Data.Hashable (Hashable(..))
import Control.Lens ((&), (^.), (^?), (.~), at, (<&>), _Just, to)
import Control.Lens.At

-- f (Seq.index m i) <&> \a -> Seq.update i a m
-- fmap (\a -> Seq update i am) (f (Seq.index m i))

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

col k = at k._Just.to(fromBlock)
