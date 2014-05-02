{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE BangPatterns #-}

module Data.Frame.Types (
  Val(..),
  Type(..),

  subsumes,
  subsume,
  like,
  typeVal,
  lub,
  lubNa
) where

import Data.Data
import Data.DateTime
import Data.Text (Text, pack)

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
subsumes ST _  = True
subsumes DT IT = True
subsumes (MT a) b = subsumes a b
subsumes a b = a == b

subsume :: Type -> Val -> Val
subsume ST v = case v of
  D x -> S (pack $ show x)
  I x -> S (pack $ show x)
  S x -> S x
  B True  -> S (pack "true")
  B False -> S (pack "false")
  T x -> S (pack $ show x)
  M x -> error "maybe case"
subsume DT v = case v of
  D x -> D x
  I x -> D (fromIntegral x)
  M x -> error "maybe case"
subsume IT v = case v of
  I x -> I x
  M x -> error "maybe case"
subsume BT v = case v of
  B x -> B x

like :: Val -> Val -> Bool
like (D _) (D _) = True
like (I _) (I _) = True
like (S _) (S _) = True
like (B _) (B _) = True
like (T _) (T _) = True

like (M (Just a)) (M (Just b))  = like a b
like (M (Just _)) (M Nothing)   = True
like (M (Nothing)) (M (Just _)) = True
like (M (Nothing)) (M Nothing)  = True
like _ _ = False

data Type = DT | IT | ST | BT | MT Type | TT
  deriving (Eq, Show, Ord)

-- Heterogeneous value
data Val
  = D {-# UNPACK #-} !Double
  | I {-# UNPACK #-} !Int
  | S {-# UNPACK #-} !Text
  | B !Bool
  | M !(Maybe Val)
  | T !DateTime
  deriving (Eq, Show, Ord, Data, Typeable)

typeVal :: Val -> Type
typeVal (D _) = DT
typeVal (I _) = IT
typeVal (S _) = ST
typeVal (B _) = BT
typeVal (T _) = TT
typeVal (M _) = error "maybe type"

-- lub [I 3, D 2.3] -> DT
-- lub [I 3, D 2.3, S "a"] -> ST

lub :: [Val] -> Either String Type
lub vals = go Nothing vals
  where
    go (Just lub) [] = Right lub
    go Nothing (x:xs) = go (Just (typeVal x)) xs
    go (Just lub) (x:xs)
      | lub == typeVal x         = go (Just lub) xs
      | lub `subsumes` typeVal x = go (Just lub) xs
      | typeVal x `subsumes` lub = go (Just (typeVal x)) xs
      | otherwise                = Left "No subsumption"

-- lubNa [I 3, D 2.3] -> DT
-- lubNa [I 3, D 2.3, S "a"] -> ST
-- lubNa [I 3, D 2.3, B True] -> (M DT, [2])

lubNa :: [Val] -> (Type, [Int])
lubNa vals = go Nothing (zip vals [0..]) []
  where
    go (Just lub) [] !miss = (lub, miss)
    go Nothing ((x,_):xs) !miss = go (Just (typeVal x)) xs miss
    go (Just lub) ((x,i):xs) !miss
      | lub == typeVal x         = go (Just lub) xs miss
      | lub `subsumes` typeVal x = go (Just lub) xs miss
      | typeVal x `subsumes` lub = go (Just (typeVal x)) xs miss
      | otherwise                = go (Just (maybe lub)) xs (i:miss)

    maybe :: Type -> Type
    maybe (MT a) = MT a
    maybe a = (MT a)
