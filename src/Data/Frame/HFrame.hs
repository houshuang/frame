{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE NoMonomorphismRestriction #-}

module Data.Frame.HFrame (
  HDataFrame,
  Val(..),
  schema,

  fromMap,
  fromLists,
  fromVectors,
  singleton,

  vlist,
  vmap,
  vconvert,
  sim,

  transform,
  alignVecs,

  (!),
  iloc,
  take,
  drop,
  slice,
  filter,
  ifilter,
  ifilters,
  hcat,
  fold,
  foldAll,
  map,
  sum,
  mean,

  showHDataFrame
) where

import Prelude hiding (take, drop, filter, sum, map)

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
  deriving (Eq, Show, Ord, Data, Generic, Typeable)

-- default values for padding
def :: Val -> Val
def (D _) = D 0.0
def (I _) = I 0
def (S _) = S $ pack ""
def (B _) = B False
def (M _) = M Nothing
def (Dt _) = Dt $ startOfTime

-- XXX less magical
ty :: Val -> String
ty (M (Just v)) = "maybe[" ++ (show (toConstr v)) ++ "]"
ty v = show (toConstr v)

isNa :: Val -> Bool
isNa (M Nothing) = True
isNa _ = False

sim :: Integral a => Val -> a -> Val
sim (D _) = D . fromIntegral
sim (I _) = I . fromIntegral
sim (M (Just x)) = M . Just . sim x
sim _ = const $ M Nothing

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
  , _hdfindex  :: V.Vector i
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

  (I x) + (I y) = I (x+y)
  (D x) + (D y) = D (x+y)
  (M (Just x)) + (M (Just y)) = M (Just $ x + y)
  _ + _ = M Nothing

  (I x) * (I y) = I (x*y)
  (D x) * (D y) = D (x*y)
  (M (Just x)) * (M (Just y)) = M (Just $ x * y)
  _ * _ = M Nothing

  negate (I x) = I (-x)
  negate (D x) = D (-x)

  abs = undefined
  signum = undefined

instance Fractional Val where
  (I x) / (I y) = I (x `div` y)
  (D x) / (D y) = D (x / y)
  (M (Just x)) / (M (Just y)) = M (Just $ x / y)
  {-x / y = error $ (show x) ++ (show y)-}
  x / y = M Nothing

  recip = undefined
  fromRational = D . fromRational

-- Show the type schema for the dataframe.
schema :: HDataFrame i k -> [String]
schema (HDataFrame dt _) = L.map (ty . V.head) $ M.elems dt

transp :: V.Vector (V.Vector a) -> V.Vector (V.Vector a)
transp rows = V.map V.head rows `V.cons` (transp (V.map V.tail rows))

-------------------------------------------------------------------------------
-- Constructors
-------------------------------------------------------------------------------

-- | Construct hdataframe from a Map.
fromMap :: (Hashable k, Ord k, Ord i) => [(k, [Val])] -> [i] -> HDataFrame i k
fromMap xs is = HDataFrame (M.fromList $ aligned) (V.fromList is)
  where
    -- XXX: fix verify check later
    aligned = alignCols $ toVector xs

-- | Construct hdataframe from a list of Lists.
fromLists :: (Hashable k, Ord k, Ord i) => [[Val]] -> [k] -> [i] -> HDataFrame i k
fromLists xs cols is = fromMap kvs is
  where kvs = zip cols xs

fromVectors :: (Hashable k, Ord k) => V.Vector (V.Vector Val) -> [k] -> HDataFrame Int k
fromVectors xs cols = HDataFrame (M.fromList $ zip cols ls) (V.fromList [0..m])
  where
    m = V.length xs
    n = V.length (V.head xs)

    -- XXX: stupidly inefficient
    ls = L.map V.fromList $ L.transpose $ L.map V.toList (V.toList xs)

singleton :: (Ord k, Hashable k) => k -> [Val] -> HDataFrame i k
singleton k v = HDataFrame (M.fromList $ toVector [(k, v)]) (V.empty)

toVector :: [(t, [Val])] -> [(t, (V.Vector Val))]
toVector = L.map (fmap V.fromList)

unVector :: [(t, (V.Vector Val))] -> [(t, [Val])]
unVector = L.map (fmap V.toList)

verify :: (Ord k, Ord i) => [(k, [Val])] -> [i] -> Bool
verify xs is = checkIndex && checkAlike
  where
    n = length (snd (head xs))

    checkLengths = all (==n) (L.map (length . snd) xs)
    checkIndex = length is == n
    checkAlike = and $ L.map (snd . fmap allAlike) xs

showHDataFrame :: (Pretty i, Pretty k) => HDataFrame i k -> String
showHDataFrame (HDataFrame dt ix) = PB.render $ PB.hsep 2 PB.right $ cols
  where
    p = pix (V.toList ix) -- show index
    cols = p : (L.map pcols $ unVector (M.toList dt)) -- show cols

    pcols (a, xs) = PB.vcat PB.left $ col ++ vals
      where
        col = [ppb a]
        vals = L.map ppb xs

    pix xs = PB.vcat PB.left (L.map ppb xs)

-------------------------------------------------------------------------------
-- Deconstructors
-------------------------------------------------------------------------------

-- Reconstruct the dataframe by applying two functions over it's structure.
--  f : transforms data keywise
--  g : transforms the index
transform :: (k -> V.Vector Val -> Identity (V.Vector Val))
           -> (V.Vector i -> V.Vector j)
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
vlist xs = L.map vconvert xs

vmap :: Convertible a => M.HashMap k a -> M.HashMap k Val
vmap xs = M.map vconvert xs

-------------------------------------------------------------------------------
-- Alignment
-------------------------------------------------------------------------------

-- XXX these are really unoptimal, fix later

alignVecs :: [V.Vector Val] -> [V.Vector Val]
alignVecs xs = L.map pad xs
  where
    mlen = maximum $ L.map V.length xs
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
  case V.elemIndex i ix of
    Just i' -> Just $ HDataFrame (M.map (\x -> V.singleton (x V.! i')) dt) (V.singleton i)
    Nothing -> Nothing

take :: Int -> HDataFrame i k -> HDataFrame i k
take n (HDataFrame dt ix) = HDataFrame (M.map (V.take n) dt) (V.take n ix)

drop :: Int -> HDataFrame i k -> HDataFrame i k
drop n (HDataFrame dt ix) = HDataFrame (M.map (V.drop n) dt) (V.drop n ix)

slice :: Int -> Int -> HDataFrame i k -> HDataFrame i k
slice i j (HDataFrame dt ix) = HDataFrame (M.map (V.slice i j) dt) (V.slice i j ix)
  {-where-}
    {-lslice start end = L.take (end - start + 1) . L.drop start-}

-- Filter rows by predicate on label.
ifilter :: (i -> Bool) -> HDataFrame i k -> HDataFrame i k
ifilter p df@(HDataFrame _ ix) = transform f g df
  where
    -- find labels locations matching predicate
    torm = S.fromList $ V.toList $ V.findIndices p ix

    -- Filter the rows on the label locations
    f _ elts = pure $ V.ifilter (\i _ -> (i `S.member` torm)) elts
    g = V.filter p

-- Select rows by a list of labels.
ifilters :: (Eq i, Hashable i) => HDataFrame i k -> [i] -> HDataFrame i k
ifilters df@(HDataFrame _ ix) is = transform f g df
  where
    is' = S.fromList is
    torm = S.fromList $ L.findIndices (\x -> x `S.member` is') (V.toList ix)

    -- Filter the rows on the label locations
    f _ elts = pure $ V.ifilter (\i _ -> (i `S.member` torm)) elts
    g = V.filter (\x -> x `S.member` is')

-- Filter by predicate on column.
filter :: (k -> Bool) -> HDataFrame i k -> HDataFrame i k
filter p (HDataFrame dt ix) = HDataFrame (M.filterWithKey (\k _ -> p k) dt) ix

-- Horizontal concatentation of frames.
hcat :: (Eq i, Eq k, Hashable k) => HDataFrame i k -> HDataFrame i k -> HDataFrame i k
hcat (HDataFrame dt ix) (HDataFrame dt' ix') = HDataFrame (alignMaps $ M.union dt dt') ix

-- Map function across a single column.
map :: Eq k => (Val -> Val) -> k -> HDataFrame i k -> HDataFrame i k
map f k dt = transform fn id dt
  where
    fn k' elts | k' == k   = pure $ V.map f elts
               | otherwise = pure $ elts

-- Left fold function across a single column.
fold :: (Ord i, Ord k, Hashable k)
      => (Val -> Val -> Val)
      -> k
      -> HDataFrame i k
      -> HDataFrame i k
fold f k (HDataFrame dt ix) = fromMap [(k, [V.foldl1' f (dt M.! k)])] []

-- Fold all columns.
foldAll :: (Ord i, Ord k, Hashable k)
      => (Val -> Val -> Val)
      -> HDataFrame i k
      -> HDataFrame i k
foldAll f (HDataFrame dt ix) = HDataFrame (M.map fn dt) V.empty
  where
    fn xs = V.singleton $ V.foldl1' f xs

-- Sum a single column.
sum :: (Ord i, Ord k, Hashable k) => k -> HDataFrame i k -> HDataFrame i k
sum k = fold (+) k

mean :: (Ord i, Ord k, Hashable k) => k -> HDataFrame i k -> HDataFrame i k
mean k (HDataFrame dt ix) = singleton k $ [m / (sim m n)]
  where
    m = V.foldl1' (+) v
    n = V.length v
    v = dt M.! k

dropna :: (Ord i, Ord k, Hashable k) => k -> HDataFrame i k -> HDataFrame i k
dropna = undefined
