{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}

module Data.Frame.Frame where

import qualified Data.Vector as V
import qualified Data.HashMap.Strict as M
import Data.Hashable (Hashable(..))

import Text.PrettyPrint
import qualified Text.PrettyPrint.Boxes as PB

import Data.Frame.Pretty

-- The homogeneously typed dataframe.
data DataFrame i k v = DataFrame
  { _dfdata :: !(M.HashMap k (V.Vector v))
  , _dfindex  :: [i]
  } deriving (Eq)

-------------------------------------------------------------------------------
-- Constructors
-------------------------------------------------------------------------------

-- | Construct dataframe from a Map.
fromMap :: (Hashable k, Ord k, Ord i) => [(k, [v])] -> [i] -> DataFrame i k v
fromMap xs is | verify xs is = DataFrame (M.fromList $ toVector xs) is
              | otherwise    = error "invalid data"

-- | Construct dataframe from a list of Lists.
fromLists :: (Hashable k, Ord k, Ord i) => [[v]] -> [k] -> [i] -> DataFrame i k v
fromLists xs cols is = fromMap kvs is
  where
    kvs = zip cols xs

toVector :: [(t, [a])] -> [(t, (V.Vector a))]
toVector = map (fmap V.fromList)

unVector :: [(t, (V.Vector a))] -> [(t, [a])]
unVector = map (fmap V.toList)

verify :: (Ord k, Ord i) => [(k, [v])] -> [i] -> Bool
verify xs is = checkLengths && checkIndex
  where
    n = length (snd (head xs))
    checkLengths = all (==n) (map (length . snd) xs)
    checkIndex = length is == n

-------------------------------------------------------------------------------
-- Pretty Printer
-------------------------------------------------------------------------------

showDataFrame :: (Show v, Pretty i, Pretty k) => DataFrame i k v -> String
showDataFrame (DataFrame dt ix) = PB.render $ PB.hsep 2 PB.right $ cols
  where
    p = pix ix -- show index
    cols = p : (map pcols $ unVector (M.toList dt)) -- show cols

    pcols :: (Show b, Pretty a) => (a, [b]) -> PB.Box
    pcols (a, xs) = PB.vcat PB.left $ ([ppb a] ++ (map (PB.text . show) xs))

    pix :: Pretty a => [a] -> PB.Box
    pix xs = PB.vcat PB.left (map ppb xs)
