{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Frame.HFrame (
  HDataFrame(..),
  showHDataFrame,

  nrows,
  ncols,
  transformIndex,
  transformKeys,

  hcat,

  Block(..),
  dblock,
  iblock,
  bblock,
  mblock,
  sblock,

  blen,
  btraverse,

  FromBlock(..),
  Result(..),

  schema,

  null,
  fromMap,
  fromBlocks,
  singleton,

) where

import Prelude hiding (maximum, sum, filter, drop, take, null)

import Control.Monad hiding (
    forM , forM_ , mapM , mapM_ , msum , sequence , sequence_ )

import Data.Frame.Pretty
import Data.Frame.Internal

import Data.Foldable
import Data.Traversable

import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Algorithms.Merge as VS

import qualified Data.List as L
import qualified Data.HashMap.Strict as M

import Data.Data
import Data.Maybe
import Data.Monoid
import Data.DateTime
import Data.Hashable (Hashable(..))

import Data.Text (Text, pack, unpack)
import qualified Text.PrettyPrint.Boxes as PB

import qualified GHC.Float as GHC

-------------------------------------------------------------------------------
-- Block Constructors
-------------------------------------------------------------------------------

bblock :: [Bool] -> Block
bblock = BBlock . VU.fromList

dblock :: [Double] -> Block
dblock = DBlock . VU.fromList

iblock :: [Int] -> Block
iblock = IBlock . VU.fromList

sblock :: [Text] -> Block
sblock = SBlock . VB.fromList

mblock :: Block -> [Bool] -> Block
mblock dat@(DBlock _) mask = MBlock dat (VU.fromList mask)
mblock dat@(IBlock _) mask = MBlock dat (VU.fromList mask)
mblock dat@(BBlock _) mask = MBlock dat (VU.fromList mask)
mblock dat@(SBlock _) mask = MBlock dat (VU.fromList mask)
mblock (MBlock dat _) mask = MBlock dat (VU.fromList mask)
mblock _ _ = error "unsupported masked block"

blen :: Block -> Int
blen (IBlock xs) = VU.length xs
blen (DBlock xs) = VU.length xs
blen (BBlock xs) = VU.length xs
blen (SBlock xs) = VB.length xs
blen (MBlock xs _) = blen xs

-------------------------------------------------------------------------------
-- Block Traversals
-------------------------------------------------------------------------------

btraverse :: (forall v a. VG.Vector v a => v a -> v a) -> Block -> Block
btraverse f (IBlock xs) = IBlock (f xs)
btraverse f (DBlock xs) = DBlock (f xs)
btraverse f (BBlock xs) = BBlock (f xs)
btraverse f (SBlock xs) = SBlock (f xs)
btraverse f (MBlock a b) = MBlock (btraverse f a) (f b)

-------------------------------------------------------------------------------
-- Block Typing
-------------------------------------------------------------------------------

class FromBlock a where
  fromBlock :: Block -> Result [a]
  toBlock :: [a] -> Block

instance FromBlock Int where
  fromBlock (IBlock xs) = Success (VU.toList xs)
  fromBlock x = Error $ "Expected 'Int' but got '" ++ (show $ blockType x) ++ "'"

  toBlock = IBlock . VU.fromList

instance FromBlock Bool where
  fromBlock (BBlock xs) = Success (VU.toList xs)
  fromBlock x = Error $ "Expected 'Bool' but got '" ++ (show $ blockType x) ++ "'"

  toBlock = BBlock . VU.fromList

instance FromBlock Double where
  fromBlock (DBlock xs) = Success (VU.toList xs)
  fromBlock x = Error $ "Expected 'Double' but got '" ++ (show $ blockType x) ++ "'"

  toBlock = DBlock . VU.fromList

instance FromBlock Text where
  fromBlock (SBlock xs) = Success (VB.toList xs)
  fromBlock x = Error $ "Expected 'Text' but got '" ++ (show $ blockType x) ++ "'"

  toBlock = SBlock . VB.fromList

instance (Default a, FromBlock a, Typeable a) => FromBlock (Maybe a) where
  fromBlock x@(MBlock {}) = unMask x
  fromBlock x = Error $ "Expected '" ++ show ty ++ "' but got '" ++ (show $ blockType x) ++ "'"
    where ty = typeOf (undefined :: Maybe a) -- XXX blech

  toBlock = reMask

instance FromBlock () where
  fromBlock NBlock = Success [()]
  fromBlock x = Error $ "Expected '()' but got '" ++ (show $ blockType x) ++ "'"
  toBlock = const NBlock


instance Monoid Block where
  mempty = NBlock
  mappend a NBlock = a
  mappend NBlock a = a
  mappend a b = a -- XXX

schema :: HDataFrame t k -> [(k, String)]
schema (HDataFrame dt _) = fmap (fmap $ show . toConstr) (M.toList dt)

-------------------------------------------------------------------------------
-- Padding
-------------------------------------------------------------------------------

-- Automatic Alignment

class Paddable a where
  pad :: Int -> a -> a

instance (Default a, VU.Unbox a) => Paddable (VU.Vector a) where
  pad n xs | n > m = (VU.++) xs (VU.replicate (n - m) def)
           | otherwise = xs
    where m = VU.length xs

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
alignVecs xs = fmap (pad mlen) xs
  where mlen = maximum $ fmap blen xs

-- XXX probably a better way to do this
alignMaps :: (Hashable k, Eq k) => M.HashMap k Block -> M.HashMap k Block
alignMaps = M.fromList . alignCols.  M.toList

alignCols :: [(k, Block)] -> [(k, Block)]
alignCols xs = zip a (alignVecs b)
  where (a, b) = unzip xs

-- Pad the index with neutral elements, shouldn't really be used.
{-alignIndex :: (Default i, Indexable i) => Index i -> Int -> Index i-}
alignIndex ix n = pad (n - VU.length ix) (ixto ix)

hcat :: (Eq i, Eq k, Hashable k) => HDataFrame i k -> HDataFrame i k -> HDataFrame i k
hcat (HDataFrame dt ix) (HDataFrame dt' ix') = HDataFrame (alignMaps $ M.union dt dt') maxIx
  where
    maxIx | VB.length ix > VB.length ix' = ix
          | otherwise = ix'

-------------------------------------------------------------------------------
-- Mask
-------------------------------------------------------------------------------

-- Convert a MBlock to a list of 'Maybe a'
unMask :: FromBlock a => Block -> Result [Maybe a]
unMask (MBlock dat bm) = do
  dat' <- fromBlock dat
  return $ go dat' (VU.toList bm)
  where
    go [] [] = []
    go (x:xs) (True:ys)  = Nothing : go xs ys
    go (x:xs) (False:ys) = Just x : go xs ys

    -- won't compile if this isn't guaranteed

-- Convert a list of 'Maybe a' to a MBlock
reMask :: (FromBlock a, Default a) => [Maybe a] -> Block
reMask xs = MBlock (toBlock $ map go xs) (VU.fromList $ map isJust xs)
  where
    go (Just a) = a
    go Nothing = def

mapMask :: VU.Unbox a =>
           (a -> a) ->       -- transformation
           VU.Vector a ->     -- data
           VU.Vector Bool ->  -- mask
           VU.Vector a
mapMask f xs ys = VU.zipWith (go f) xs ys
  where
    go :: (a -> a) -> a -> Bool -> a
    go f' x y | y == True = f' x
              | otherwise = x

-------------------------------------------------------------------------------
-- Constructors
-------------------------------------------------------------------------------

-- | Construct hdataframe from a Map.
fromMap :: (Columnable k, Num i, Enum i) => [(k, Block)] -> HDataFrame i k
fromMap xs = HDataFrame df_data df_ix
  where
    df_data = M.fromList $ aligned
    df_ix   = VB.enumFromTo 0 (fromIntegral nrows)

    aligned = alignCols xs

    nrows :: Int
    nrows = blen $ snd $ head aligned

    ncols :: Int
    ncols = L.length aligned

-- | Construct hdataframe from a list of blocks and column keys.
fromBlocks :: (Eq k, Hashable k) => [Block] -> [k] -> HDataFrame Int k
fromBlocks xs cols = HDataFrame (M.fromList $ zip cols xs) df_ix
  where
    df_ix = VB.enumFromTo 0 (fromIntegral nrows)
    ncols = length xs
    nrows = maximum $ fmap blen xs

-- | Construct a dataframe from a singular index, key and block.
singleton :: Columnable k => k -> VB.Vector i -> Block -> HDataFrame i k
singleton k i v = HDataFrame (M.fromList $ [(k, v)]) i

-------------------------------------------------------------------------------
-- Deconstructors
-------------------------------------------------------------------------------

null :: HDataFrame a k -> Bool
null (HDataFrame df ix) = M.null df && VB.null ix

-- hack to allow us to apply Fractional functions over Integral columsn, unsafe with respect to
-- overflow/underflow
castDoubleTransform :: (Double -> Double) -> Int -> Int
castDoubleTransform f = GHC.double2Int . f . GHC.int2Double

-- Transform the index
{-transformIndex :: (Indexable a, Indexable b) => (a -> b) -> HDataFrame a k -> HDataFrame b k-}
transformIndex f (HDataFrame dt ix) = HDataFrame dt (VB.map (ixto . f.  ixfrom) ix)

-- Transform the columns
transformKeys :: (Columnable a, Columnable b) => (a -> b) -> HDataFrame i a -> HDataFrame i b
transformKeys f (HDataFrame dt ix) = HDataFrame dt' ix
  where dt' = M.fromList [((f k), v) | (k, v) <- M.toList dt]

-------------------------------------------------------------------------------
-- Pretty Printing
-------------------------------------------------------------------------------

showHDataFrame :: (Pretty i, Pretty k) => HDataFrame i k -> String
showHDataFrame (HDataFrame dt ix) = PB.render $ PB.hsep 2 PB.right $ body
  where
    index = pix (VB.toList ix) -- show index
    cols  = fmap pcols $ showBlocks (M.toList dt) -- show cols
    body  = index : cols

    pcols (a, xs) = PB.vcat PB.left $ col ++ vals
      where
        col = [ppb a]
        vals = fmap ppb xs

    pix xs = PB.vcat PB.left (fmap ppb xs)

showBlocks ::  [(a, Block)] -> [(a, [String])]
showBlocks = fmap (fmap showBlock)
  where
    showBlock (IBlock xs) = fmap show $ VU.toList xs
    showBlock (DBlock xs) = fmap show $ VU.toList xs
    showBlock (BBlock xs) = fmap show $ VU.toList xs
    showBlock (SBlock xs) = fmap unpack $ VB.toList xs
    showBlock (MBlock xs bm) = zipWith unpackMissing (showBlock xs) (VU.toList bm)
    showBlock NBlock = ["()"]

    unpackMissing a True = a
    unpackMissing _ False = "na"

-------------------------------------------------------------------------------
-- Indexing
-------------------------------------------------------------------------------

nrows :: HDataFrame i k -> Int
nrows (HDataFrame dt _) = blen $ snd $ head (M.toList dt)

ncols :: HDataFrame i k -> Int
ncols (HDataFrame dt _) = M.size dt
