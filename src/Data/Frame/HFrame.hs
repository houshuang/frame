{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE NoMonomorphismRestriction #-}

module Data.Frame.HFrame (
  HDataFrame,

  fromMap,
  fromLists,

  Val,
  vlist,
  vmap,
  vconvert,

  transform,

  alignVecs,

  (!),
  iloc,
  take,
  drop,
  slice,
  filter,
  ifilter,
  hcat,

  showHDataFrame
) where

import Prelude hiding (take, drop, filter)

import Data.Frame.Pretty

import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Algorithms.Merge as VS

import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import qualified Data.List as L

import Data.Data
import Data.DateTime
import Data.Typeable
import Data.Functor.Identity
import Data.Hashable (Hashable(..))

import Text.PrettyPrint hiding (hcat)
import Data.Text (Text, pack)
import GHC.Generics

import Control.Applicative

import qualified Text.PrettyPrint.Boxes as PB

-- Heterogeneous value
data Val
  = D !Double
  | I !Integer
  | S !Text
  | B !Bool
  | M !(Maybe Val)
  | Dt DateTime
  deriving (Eq, Show, Data, Generic, Typeable)

-- default values for padding
def :: Val -> Val
def (D _) = D 0.0
def (I _) = I 0
def (S _) = S $ pack ""
def (B _) = B False
def (M _) = M Nothing
def (Dt _) = Dt $ startOfTime


like :: Val -> Val -> Bool
like (D _) (D _) = True
like (I _) (I _) = True
like (S _) (S _) = True
like (B _) (B _) = True
like (M (Just a)) (M (Just b)) = like a b
like (M (Just _)) (M Nothing) = True
like (M (Nothing)) (M (Just _)) = True
like (M (Nothing)) (M Nothing) = True
like (Dt _) (Dt _) = True
like _ _ = False

allAlike :: [Val] -> Bool
allAlike xs = all (like t) xs
  where t = head xs

-- The heterogeneously typed dataframe.
data HDataFrame i k = HDataFrame
  { _hdfdata :: !(M.HashMap k (V.Vector Val))
  , _hdfindex  :: [i]
  } deriving (Eq)

instance Hashable Val where
  hashWithSalt s (D v) = (0 :: Int) `hashWithSalt` (hashWithSalt s v)
  hashWithSalt s (I v) = (1 :: Int) `hashWithSalt` (hashWithSalt s v)
  hashWithSalt s (S v) = (2 :: Int) `hashWithSalt` (hashWithSalt s v)
  hashWithSalt s (B v) = (3 :: Int) `hashWithSalt` (hashWithSalt s v)
  hashWithSalt s (M v) = (4 :: Int) `hashWithSalt` (hashWithSalt s v)

instance Pretty Val where
  ppr p (D n) = ppr p n
  ppr p (I n) = ppr p n
  ppr p (S n) = ppr p n
  ppr p (B n) = ppr p n
  ppr p (M Nothing) = text "NA"
  ppr p (M (Just n)) = ppr p n

-- XXX: terrible hack, move to Interactive
instance Num Val where
  fromInteger = I
  (+) = undefined
  (*) = undefined
  abs = undefined
  signum = undefined

instance Fractional Val where
  (/) = undefined
  recip = undefined
  fromRational = D . fromRational

-------------------------------------------------------------------------------
-- Constructors
-------------------------------------------------------------------------------

-- | Construct hdataframe from a Map.
fromMap :: (Hashable k, Ord k, Ord i) => [(k, [Val])] -> [i] -> HDataFrame i k
fromMap xs is = HDataFrame (M.fromList $ aligned) is
  where
    -- XXX: fix later
    aligned = alignCols $ toVector xs

-- | Construct hdataframe from a list of Lists.
fromLists :: (Hashable k, Ord k, Ord i) => [[Val]] -> [k] -> [i] -> HDataFrame i k
fromLists xs cols is = fromMap kvs is
  where kvs = zip cols xs

toVector :: [(t, [Val])] -> [(t, (V.Vector Val))]
toVector = map (fmap V.fromList)

unVector :: [(t, (V.Vector Val))] -> [(t, [Val])]
unVector = map (fmap V.toList)

verify :: (Ord k, Ord i) => [(k, [Val])] -> [i] -> Bool
verify xs is = checkIndex && checkAlike
  where
    n = length (snd (head xs))

    checkLengths = all (==n) (map (length . snd) xs)
    checkIndex = length is == n
    checkAlike = and $ map (snd . fmap allAlike) xs

showHDataFrame :: (Pretty i, Pretty k) => HDataFrame i k -> String
showHDataFrame (HDataFrame dt ix) = PB.render $ PB.hsep 2 PB.right $ cols
  where
    p = pix ix -- show index
    cols = p : (map pcols $ unVector (M.toList dt)) -- show cols

    pcols (a, xs) = PB.vcat PB.left $ col ++ vals
      where
        col = [ppb a]
        vals = map ppb xs

    pix xs = PB.vcat PB.left (map ppb xs)

-------------------------------------------------------------------------------
-- Deconstructors
-------------------------------------------------------------------------------

-- Reconstruct the dataframe by applying two functions over it's structure.
--  f : transforms data keywise
--  g : transforms the index
transform :: (k -> V.Vector Val -> Identity (V.Vector Val))
           -> ([i] -> [j])
           -> HDataFrame i k
           -> HDataFrame j k
transform f g (HDataFrame dt ix) = HDataFrame dt' ix'
  where dt' = runIdentity $ M.traverseWithKey f dt
        ix' = g ix

-------------------------------------------------------------------------------
-- Conversion
-------------------------------------------------------------------------------

class Convertible a where
  vconvert :: a -> Val

instance Convertible Integer where
  vconvert = I

instance Convertible Int where
  vconvert = I . fromIntegral

instance Convertible Double where
  vconvert = D

instance Convertible Text where
  vconvert = S

instance Convertible String where
  vconvert = S . pack

instance Convertible Bool where
  vconvert = B

instance Convertible a => Convertible (Maybe a) where
  vconvert (Just a) = M (Just (vconvert a))
  vconvert (Nothing) = M Nothing

-- convienance conversion routines
vlist :: Convertible a => [a] -> [Val]
vlist xs = map vconvert xs

vmap :: Convertible a => M.HashMap k a -> M.HashMap k Val
vmap xs = M.map vconvert xs

-------------------------------------------------------------------------------
-- Alignment
-------------------------------------------------------------------------------

-- XXX these are really unoptimal, fix later

alignVecs :: [V.Vector Val] -> [V.Vector Val]
alignVecs xs = map pad xs
  where
    mlen = maximum $ map V.length xs
    mkEmpty n v = V.replicate n (def (V.head v))

    -- insert default elements if the
    pad x | V.length x < mlen = (V.++) x (mkEmpty (mlen - (V.length x)) x)
          | otherwise = x

alignCols :: [(k, V.Vector Val)] -> [(k, V.Vector Val)]
alignCols xs = zip a (alignVecs b)
  where (a, b) = unzip xs

alignMaps :: (Eq k, Hashable k) => M.HashMap k (V.Vector Val) -> M.HashMap k (V.Vector Val)
alignMaps dt = M.fromList $ alignCols (M.toList dt)

-------------------------------------------------------------------------------
-- Indexing
-------------------------------------------------------------------------------

-- Select column.
(!) :: (Eq k, Hashable k) => HDataFrame i k -> k -> Maybe (HDataFrame i k)
(HDataFrame dt ix) ! k = case M.lookup k dt of
  Just v -> Just $ HDataFrame (M.singleton k v) ix
  Nothing -> Nothing

-- Select row by label.
iloc :: Eq i => HDataFrame i k -> i -> Maybe (HDataFrame i k)
iloc (HDataFrame dt ix) i =
  case L.elemIndex i ix of
    Just i' -> Just $ HDataFrame (M.map (\x -> V.singleton (x V.! i')) dt) [i]
    Nothing -> Nothing

take :: Int -> HDataFrame i k -> HDataFrame i k
take n (HDataFrame dt ix) = HDataFrame (M.map (V.take n) dt) (L.take n ix)

drop :: Int -> HDataFrame i k -> HDataFrame i k
drop n (HDataFrame dt ix) = HDataFrame (M.map (V.drop n) dt) (L.drop n ix)

slice :: Int -> Int -> HDataFrame i k -> HDataFrame i k
slice i j (HDataFrame dt ix) = HDataFrame (M.map (V.slice i j) dt) (lslice i j ix)
  where
    lslice start end = L.take (end - start + 1) . L.drop start

-- Filter by predicate on label.
ifilter :: (i -> Bool) -> HDataFrame i k -> HDataFrame i k
ifilter p df@(HDataFrame _ ix) = transform f g df
  where
    -- find labels locations matching predicate
    torm = S.fromList $ L.findIndices p ix

    -- Filter the rows on the label locations
    f _ elts = pure $ V.ifilter (\i _ -> (i `S.member` torm)) elts
    g = L.filter p

-- Filter by predicate on column.
filter :: (k -> Bool) -> HDataFrame i k -> HDataFrame i k
filter p (HDataFrame dt ix) = HDataFrame (M.filterWithKey (\k _ -> p k) dt) ix

-- Horizontal concatentation of frames.
hcat :: (Eq i, Eq k, Hashable k) => HDataFrame i k -> HDataFrame i k -> HDataFrame i k
hcat (HDataFrame dt ix) (HDataFrame dt' ix') | ix == ix' = HDataFrame (alignMaps $ M.union dt dt') ix
