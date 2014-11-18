{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE BangPatterns #-}

module Data.Frame.Types (
  Val(..),
  Type(..),

  subsumes,
  subsume,
  like,
  typeVal,
  lub
) where

import Data.Data
-- import Data.DateTime
import Data.Text (Text, pack)
import Data.Frame.Internal (Default(..))
import Control.DeepSeq (NFData(..))

-------------------------------------------------------------------------------
-- Types
-------------------------------------------------------------------------------

-- Columns types have a subsumption rule which dictates when we upcast the type of the values in column. If we
-- have a column of Int values with a single String element in the middle of the data then then we upcast to
-- String. If the user specifes (Maybe a) type for the column then the column treats mismatched values as
-- missing values.
--
-- a <: a
-- a <: b |- Maybe a <: Maybe b

-- Double   <: String
-- Bool     <: String
-- Datetime <: String
-- Int      <: Double

subsumes :: Type -> Type -> Bool
subsumes (MT a) b = subsumes a b
subsumes ST DT  = True
subsumes ST BT  = True
subsumes ST IT  = True
subsumes ST TT  = True
subsumes DT IT = True
subsumes _ Any = True
subsumes a b = a == b

subsume :: Type -> Val -> Val
subsume ST v = case v of
  D x -> S (pack $ show x)
  I x -> S (pack $ show x)
  S x -> S x
  B True  -> S (pack "true")
  B False -> S (pack "false")
  -- T x -> S (pack $ show x)
  {-M x -> error "maybe case"-}
subsume DT v = case v of
  D x -> D x
  I x -> D (fromIntegral x)
  {-M x -> error "maybe case"-}
subsume IT v = case v of
  I x -> I x
  M x -> error "maybe case"
subsume BT v = case v of
  B x -> B x

subsume (MT IT) v = case v of
  I x -> I x
  _   -> NA
subsume (MT DT) v = case v of
  D x -> D x
  I x -> D (fromIntegral x)
  _   -> NA
subsume (MT ST) v = case v of
  S x -> S x
  _   -> NA
subsume (MT BT) v = case v of
  B x -> B x
  _   -> NA
subsume (MT Any) v = NA

like :: Val -> Val -> Bool
like (D _) (D _) = True
like (I _) (I _) = True
like (S _) (S _) = True
like (B _) (B _) = True
-- like (T _) (T _) = True

like (M (Just a)) (M (Just b))  = like a b
like (M (Just _)) (M Nothing)   = True
like (M (Nothing)) (M (Just _)) = True
like (M (Nothing)) (M Nothing)  = True
like _ _ = False

data Type = DT | IT | ST | BT | MT Type | TT | Any
  deriving (Eq, Show, Ord)

-- Heterogeneous value
data Val
  = D {-# UNPACK #-} !Double
  | I {-# UNPACK #-} !Int
  | S {-# UNPACK #-} !Text
  | B !Bool
  | M !(Maybe Val)
  -- | T !DateTime
  | NA
  deriving (Eq, Show, Ord, Data, Typeable)

instance NFData Val where
  rnf (D _) = ()
  rnf (I _) = ()
  rnf (S _) = ()
  rnf (B a) = rnf a
  rnf (M a) = rnf a
  -- rnf (T a) = rnf a
  rnf NA = ()

typeVal :: Val -> Type
typeVal (D _) = DT
typeVal (I _) = IT
typeVal (S _) = ST
typeVal (B _) = BT
-- typeVal (T _) = TT
typeVal (M (Just t)) = MT (typeVal t)
typeVal (M Nothing) = Any
typeVal NA = Any

-- lub [I 3, D 2.3] -> DT
-- lub [I 3, D 2.3, S "a"] -> ST

lub :: [Val] -> Either String Type
lub vals = go Nothing vals
  where
    go (Just lub) [] = Right lub
    go Nothing (NA:xs) = goNa Nothing xs -- first value is a NA
    go Nothing (x:xs) = go (Just (typeVal x)) xs
    go (Just lub) (x:xs)
      | typeVal x == Any         = goNa (Just (maybeT lub)) xs -- we hit a NA midstream
      | lub == typeVal x         = go (Just lub) xs
      | lub `subsumes` typeVal x = go (Just lub) xs
      | typeVal x `subsumes` lub = go (Just (typeVal x)) xs
      | otherwise                = Left $ "No subsumption: " ++ (show lub) ++ " ~ " ++ (show $ typeVal x)

    goNa Nothing (x:xs) = goNa (Just (typeVal x)) xs
    goNa (Just lub) [] = Right lub
    goNa (Just lub) (x:xs)
      | lub == typeVal x         = goNa (Just lub) xs
      | lub `subsumes` typeVal x = goNa (Just lub) xs
      | maybeT (typeVal x) `subsumes` lub = goNa (Just (maybeT (typeVal x))) xs
      | otherwise                = goNa (Just lub) xs -- missing case

maybeT :: Type -> Type
maybeT (MT a) = MT a
maybeT a = (MT a)
