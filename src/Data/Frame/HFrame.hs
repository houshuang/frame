{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Data.Frame.HFrame (
  HDataFrame,
  showHDataFrame,

  nrows,
  ncols,
  transformIndex,
  transformKeys,

  Block(..),
  dblock,
  iblock,
  bblock,
  mblock,
  sblock,
  blen,

  schema,

  fromMap,
  fromBlocks,
  column,

  (!),
  iloc,
  take,
  drop,
  slice,

  filter,
  ifilter,
  ifilters,

  mapNum,
  mapOrd,
  mapFrac,

  hcat,

  sum,
  sumCol,

  mean,
  meanCol

) where

import Prelude hiding (take, drop, filter, sum)

import Data.Frame.Pretty

import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Algorithms.Merge as VS

import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import qualified Data.List as L

import Data.Data
{-import Data.DateTime-}
import Data.Typeable
import Data.Hashable (Hashable(..))

import Text.PrettyPrint hiding (hcat)
import Data.Text (Text, pack, unpack)
import qualified GHC.Float as GHC

import qualified Text.PrettyPrint.Boxes as PB

type Index i = V.Vector i

class (Eq k, Show k, Hashable k) => Columnable k where
class (Ord i, Show i, V.Unbox i, Default i) => Indexable i where

instance Columnable Int
instance Columnable String
instance Columnable Text
instance Columnable Bool

instance Indexable Int
instance Indexable Double
instance Indexable Float
instance Indexable Bool
{-instance Indexable DateTime-}

-- The heterogeneously typed dataframe.
data HDataFrame i k = HDataFrame
  { _hdfdata :: !(M.HashMap k Block)
  , _hdfindex :: !(Index i)
  } deriving (Eq)

data Block
  = DBlock !(V.Vector Double)
  | IBlock !(V.Vector Int)
  | BBlock !(V.Vector Bool)
  | MBlock {
     mdata  :: !Block
   , bitmap :: !(V.Vector Bool)
   }
  | SBlock !(VB.Vector Text)
  deriving (Eq, Show, Data, Typeable)

bblock :: [Bool] -> Block
bblock = BBlock . V.fromList

dblock :: [Double] -> Block
dblock = DBlock . V.fromList

iblock :: [Int] -> Block
iblock = IBlock . V.fromList

sblock :: [Text] -> Block
sblock = SBlock . VB.fromList

mblock :: Block -> [Bool] -> Block
mblock dat@(DBlock _) mask = MBlock dat (V.fromList mask)
mblock dat@(IBlock _) mask = MBlock dat (V.fromList mask)
mblock dat@(BBlock _) mask = MBlock dat (V.fromList mask)
mblock _ _ = error "unsupported masked block"

schema :: HDataFrame t k -> [(k, String)]
schema (HDataFrame dt _) = L.map (fmap $ show . toConstr) (M.toList dt)

-------------------------------------------------------------------------------
-- Constructors
-------------------------------------------------------------------------------

-- | Construct hdataframe from a Map.
fromMap :: (Columnable k, V.Unbox i, Num i, Enum i) => [(k, Block)] -> HDataFrame i k
fromMap xs = HDataFrame df_data df_ix
  where
    df_data = M.fromList $ aligned
    df_ix   = V.enumFromTo 0 (fromIntegral nrows)

    aligned = alignCols xs

    nrows :: Int
    nrows = blen $ snd $ head aligned

    ncols :: Int
    ncols = L.length aligned

fromBlocks :: (Eq k, Hashable k) => [Block] -> [k] -> HDataFrame Int k
fromBlocks xs cols = HDataFrame (M.fromList $ zip cols xs) df_ix
  where
    df_ix = V.enumFromTo 0 (fromIntegral nrows)
    ncols = length xs
    nrows = maximum (map blen xs)

-- | Construct a dataframe from a singular index, key and block.
column :: Columnable k => k -> V.Vector i -> Block -> HDataFrame i k
column k i v = HDataFrame (M.fromList $ [(k, v)]) i

-- | Construct a dataframe from a vector of
row :: Indexable i => V.Vector i -> V.Vector k -> HDataFrame i k
row = undefined

-------------------------------------------------------------------------------
-- Pretty Printing
-------------------------------------------------------------------------------

unText :: [Text] -> [String]
unText = L.map unpack

showHDataFrame :: (Pretty i, Pretty k, V.Unbox i) => HDataFrame i k -> String
showHDataFrame (HDataFrame dt ix) = PB.render $ PB.hsep 2 PB.right $ body
  where
    index = pix (V.toList ix) -- show index
    cols = (L.map pcols $ showBlocks (M.toList dt)) -- show cols
    body = index : cols

    pcols (a, xs) = PB.vcat PB.left $ col ++ vals
      where
        col = [ppb a]
        vals = L.map ppb xs

    pix xs = PB.vcat PB.left (L.map ppb xs)

showBlocks ::  [(a, Block)] -> [(a, [String])]
showBlocks = L.map (fmap showBlock)
  where
    showBlock (IBlock xs) = L.map show $ V.toList xs
    showBlock (DBlock xs) = L.map show $ V.toList xs
    showBlock (BBlock xs) = L.map show $ V.toList xs
    showBlock (SBlock xs) = unText (VB.toList xs)
    showBlock (MBlock xs bm) = zipWith unpackMissing (showBlock xs) (V.toList bm)
      where
        unpackMissing a True = a
        unpackMissing _ False = "na"

-------------------------------------------------------------------------------
-- Deconstructors
-------------------------------------------------------------------------------

mapMask :: (V.Unbox a) => (a -> a) -> V.Vector a -> V.Vector Bool -> V.Vector a
mapMask f xs ys = V.zipWith (go f) xs ys
  where
    go :: (a -> a) -> a -> Bool -> a
    go f' x y | y == True = f' x
              | otherwise = x

-- hack to allow us to apply Fractional functions over Integral columsn, unsafe with respect to
-- overflow/underflow
castDoubleTransform :: (Double -> Double) -> Int -> Int
castDoubleTransform f = GHC.double2Int . f . GHC.int2Double

-- Transform the index
transformIndex :: (Indexable a, Indexable b) => (a -> b) -> HDataFrame a k -> HDataFrame b k
transformIndex f (HDataFrame dt ix) = HDataFrame dt (V.map f ix)

-- Transform the columns
transformKeys :: (Columnable a, Columnable b) => (a -> b) -> HDataFrame i a -> HDataFrame i b
transformKeys f (HDataFrame dt ix) = HDataFrame dt' ix
  where dt' = M.fromList [((f k), v) | (k, v) <- M.toList dt]


-- Apply a function over an unparameterized type, we can't implement Functor because of the heteregenous
-- typing of the block structure so this just reaches inside and applies regardless of what the underlying
-- structure of the block is, V.Vector, VB.Vector, List, etc...
class Apply f where
  mapNum  :: (forall t. Num t => t -> t) -> f -> f
  mapFrac :: (forall t. Fractional t => t -> t) -> f -> f
  mapEq   :: (forall t. Eq t => t -> t) -> f -> f
  mapOrd  :: (forall t. Ord t => t -> t) -> f -> f

  foldNum  :: (forall t. Num t => t -> t -> t) -> f -> f
  {-foldFrac :: (forall t. Fractional t => t -> t -> t) -> f -> f-}
  {-foldEq   :: (forall t. Eq t => t -> t -> t) -> f -> f-}
  {-foldOrd  :: (forall t. Ord t => t -> t -> t) -> f -> f-}

  -- Apply directly to the underlying unboxed structure.
  applyVec :: (forall a. V.Unbox a => V.Vector a -> V.Vector a) -> f -> f

instance Apply Block where
  mapNum f (DBlock xs) = DBlock $ V.map f xs
  mapNum f (MBlock (DBlock xs) bm) = MBlock (DBlock $ mapMask f xs bm) bm
  mapNum f (MBlock (IBlock xs) bm) = MBlock (IBlock $ mapMask f xs bm) bm
  mapNum f (IBlock xs) = IBlock $ V.map f xs
  mapNum _ x = x

  mapFrac f (IBlock xs) = IBlock $ V.map (castDoubleTransform f) xs
  mapFrac f (DBlock xs) = DBlock $ V.map f xs
  mapFrac f (MBlock (DBlock xs) bm) = MBlock (DBlock $ mapMask f xs bm) bm
  mapFrac f (MBlock (IBlock xs) bm) = MBlock (IBlock $ mapMask (castDoubleTransform f) xs bm) bm
  mapFrac _ x = x

  mapEq f (DBlock xs) = DBlock $ V.map f xs
  mapEq f (MBlock (DBlock xs) bm) = MBlock (DBlock $ mapMask f xs bm) bm
  mapEq f (MBlock (IBlock xs) bm) = MBlock (IBlock $ mapMask f xs bm) bm
  mapEq f (IBlock xs) = IBlock $ V.map (castDoubleTransform f) xs
  mapEq _ x = x

  mapOrd f (DBlock xs) = DBlock $ V.map f xs
  mapOrd f (MBlock (DBlock xs) bm) = MBlock (DBlock $ mapMask f xs bm) bm
  mapOrd f (MBlock (IBlock xs) bm) = MBlock (IBlock $ mapMask f xs bm) bm
  mapOrd f (IBlock xs) = IBlock $ V.map (castDoubleTransform f) xs
  mapOrd _ x = x

  foldNum f (DBlock xs) = DBlock $ V.singleton $ V.foldl1 f xs
  foldNum f (IBlock xs) = IBlock $ V.singleton $ V.foldl1 f xs
  -- XXX Folds over bitmasked blocks can yield different results depending on if we ignore or use NA values.
  foldNum _ _ = mblock (iblock [0]) [False]

  applyVec f (IBlock xs) = IBlock $ f xs
  applyVec f (DBlock xs) = DBlock $ f xs
  applyVec f (BBlock xs) = BBlock $ f xs
  applyVec f (MBlock xs bm) = MBlock (applyVec f xs) (f bm)
  applyVec _ x = x

instance Indexable i => Apply (HDataFrame i k) where
  mapFrac f (HDataFrame dt ix) = HDataFrame (M.map (mapFrac f) dt) ix
  mapNum f (HDataFrame dt ix)  = HDataFrame (M.map (mapNum f) dt) ix
  mapEq f (HDataFrame dt ix)   = HDataFrame (M.map (mapEq f) dt) ix
  mapOrd f (HDataFrame dt ix)  = HDataFrame (M.map (mapOrd f) dt) ix

  foldNum f (HDataFrame dt _) = HDataFrame (M.map (foldNum f) dt) (V.singleton def)

  applyVec f (HDataFrame dt ix)  = HDataFrame (M.map (applyVec f) dt) (f ix)

-------------------------------------------------------------------------------
-- Block Structure
-------------------------------------------------------------------------------

class Default a where
  def :: a

instance Default Int where
  def = 0

instance Default Bool where
  def = False

instance Default Double where
  def = 0.0

instance Default Float where
  def = 0.0

instance Default Text where
  def = pack ""

blen :: Block -> Int
blen (IBlock xs) = V.length xs
blen (DBlock xs) = V.length xs
blen (BBlock xs) = V.length xs
blen (SBlock xs) = VB.length xs
blen (MBlock _ bm) = V.length bm

-------------------------------------------------------------------------------
-- Automatic Alignment
-------------------------------------------------------------------------------

class Paddable a where
  pad :: Int -> a -> a

instance (Default a, V.Unbox a) => Paddable (V.Vector a) where
  pad 0 xs = xs
  pad n xs | n > m = xs V.++ V.replicate (n - m) def
           | otherwise = xs
    where m = V.length xs

instance (Default a) => Paddable (VB.Vector a) where
  pad 0 xs = xs
  pad n xs = xs VB.++ VB.replicate (n - m) def
    where m = VB.length xs

instance Paddable Block where
  pad n (IBlock x) = IBlock $ pad n x
  pad n (BBlock x) = BBlock $ pad n x
  pad n (DBlock x) = DBlock $ pad n x
  pad n (SBlock x) = SBlock $ pad n x
  pad n (MBlock x bm) = MBlock (pad n x) (pad n bm)

alignVecs :: [Block] -> [Block]
alignVecs xs = L.map (pad mlen) xs
  where mlen = maximum $ L.map blen xs

-- XXX probably a better way to do this
alignMaps :: (Hashable k, Eq k) => M.HashMap k Block -> M.HashMap k Block
alignMaps = M.fromList . alignCols.  M.toList

alignCols :: [(k, Block)] -> [(k, Block)]
alignCols xs = zip a (alignVecs b)
  where (a, b) = unzip xs

-- Pad the index with neutral elements, shouldn't really be used.
alignIndex :: (Default i, Indexable i) => Index i -> Int -> Index i
alignIndex ix n = pad (n - V.length ix) ix

-------------------------------------------------------------------------------
-- Indexing
-------------------------------------------------------------------------------

-- Select column.
(!) :: (Columnable k, Indexable i) => HDataFrame i k -> k -> (HDataFrame i k)
(HDataFrame dt ix) ! k =
  case M.lookup k dt of
    Just v -> HDataFrame (M.singleton k v) ix
    Nothing -> error $ "no such column: " ++ (show k)


-- Select row by label.
iloc :: (Indexable i) => HDataFrame i k -> i -> (HDataFrame i k)
iloc (HDataFrame dt ix) i =
  case V.elemIndex i ix of
    Just i' -> HDataFrame (M.map (ixblock i') dt) (V.singleton i)
    Nothing -> error $ "no such row: " ++ (show i)

  where
    ixblock :: Apply a => Int -> a -> a
    ixblock j = applyVec (V.singleton . (V.! j))

nrows :: HDataFrame i k -> Int
nrows (HDataFrame dt _) = blen $ snd $ head (M.toList dt)

ncols :: HDataFrame i k -> Int
ncols (HDataFrame dt _) = M.size dt

-------------------------------------------------------------------------------
-- Selection
-------------------------------------------------------------------------------

take :: Indexable i => Int -> HDataFrame i k -> HDataFrame i k
take n = applyVec (V.take n)

drop :: Indexable i => Int -> HDataFrame i k -> HDataFrame i k
drop n = applyVec (V.drop n)

slice :: Indexable i => Int -> Int -> HDataFrame i k -> HDataFrame i k
slice i j = applyVec (V.slice i j)

filter :: Indexable i => (forall a. V.Unbox a => a -> Bool) -> HDataFrame i k -> HDataFrame i k
filter p = applyVec (V.filter p)

-- Filter rows by predicate on label.
ifilter :: Indexable i => (i -> Bool) -> HDataFrame i k -> HDataFrame i k
ifilter p df@(HDataFrame dt ix) = applyVec f df
  where
    -- find labels locations matching predicate
    torm :: S.HashSet Int
    torm = S.fromList $ V.toList $ V.findIndices p ix

    -- Filter the rows on the label locations
    f = V.ifilter (\i _ -> (i `S.member` torm))
    g = V.filter p

-- XXX: don't want Hashable constraint
ifilters :: (Indexable i, Hashable i) => [i] -> HDataFrame i k -> HDataFrame i k
ifilters is df@(HDataFrame dt ix) = applyVec f df
  where
    -- find labels locations matching predicate
    is' = S.fromList is
    torm = S.fromList $ L.findIndices (\x -> x `S.member` is') (V.toList ix)

    -- Filter the rows on the label locations
    f = V.ifilter (\i _ -> (i `S.member` torm))

-- Filter by predicate on column.
filterCol :: (k -> Bool) -> HDataFrame i k -> HDataFrame i k
filterCol p (HDataFrame dt ix) = HDataFrame (M.filterWithKey (\k _ -> p k) dt) ix

-- Horizontal concatentation of frames.
hcat :: (Eq i, Eq k, Hashable k) => HDataFrame i k -> HDataFrame i k -> HDataFrame i k
hcat (HDataFrame dt ix) (HDataFrame dt' ix') = HDataFrame (alignMaps $ M.union dt dt') ix

sum :: (Indexable i, Columnable k) => HDataFrame i k -> HDataFrame i k
sum = foldNum (+)

mean :: (Indexable i, Columnable k) => HDataFrame i k -> HDataFrame i k
mean df = mapFrac (/m) $ foldNum (+) df
  where m = fromIntegral $ nrows df

sumCol :: (Indexable i, Columnable k) => HDataFrame i k -> k -> HDataFrame i k
sumCol df k = foldNum (+) (df ! k)

meanCol :: (Indexable i, Columnable k) => HDataFrame i k -> k -> HDataFrame i k
meanCol df k = mapFrac (/m) $ sumCol df k
  where m = fromIntegral $ nrows df
