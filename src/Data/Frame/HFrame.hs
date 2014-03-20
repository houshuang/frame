{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}

module Data.Frame.HFrame where

import qualified Data.Vector as V
import qualified Data.HashMap.Strict as M
import Data.Hashable (Hashable(..))
import GHC.Generics

import Data.Data
import Data.Text (Text, pack)

import Text.PrettyPrint
import qualified Text.PrettyPrint.Boxes as PB
import Data.Frame.Pretty

-- Heterogeneous value
data Val
  = D !Double
  | I !Integer
  | S !Text
  | B !Bool
  -- | Dt  datetime
  deriving (Eq, Show, Data, Generic, Typeable)

like :: Val -> Val -> Bool
like (D _) (D _) = True
like (I _) (I _) = True
like (S _) (S _) = True
like (B _) (B _) = True
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

instance Pretty Val where
  ppr p (D n) = ppr p n
  ppr p (I n) = ppr p n
  ppr p (S n) = ppr p n
  ppr p (B n) = ppr p n

-------------------------------------------------------------------------------
-- Constructors
-------------------------------------------------------------------------------

-- | Construct hdataframe from a Map.
fromMap :: (Hashable k, Ord k, Ord i) => [(k, [Val])] -> [i] -> HDataFrame i k
fromMap xs is | verify xs is = HDataFrame (M.fromList $ toVector xs) is
              | otherwise    = error "invalid data"

-- | Construct hdataframe from a list of Lists.
fromLists :: (Hashable k, Ord k, Ord i) => [[Val]] -> [k] -> [i] -> HDataFrame i k
fromLists xs cols is = fromMap kvs is
  where
    kvs = zip cols xs

toVector :: [(t, [Val])] -> [(t, (V.Vector Val))]
toVector = map (fmap V.fromList)

unVector :: [(t, (V.Vector Val))] -> [(t, [Val])]
unVector = map (fmap V.toList)

verify :: (Ord k, Ord i) => [(k, [Val])] -> [i] -> Bool
verify xs is = checkLengths && checkIndex && checkAlike
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

    pcols (a, xs) = PB.vcat PB.left $ (
         [ppb a] -- column
          ++ (map ppb xs) -- values
        )

    pix xs = PB.vcat PB.left (map ppb xs)

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

-- convienance conversion routines
vlist :: Convertible a => [a] -> [Val]
vlist xs = map vconvert xs

vmap :: Convertible a => M.HashMap k a -> M.HashMap k Val
vmap xs = M.map vconvert xs
