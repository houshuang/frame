{-# LANGUAGE DeriveDataTypeable #-}

module Data.Frame.Types (
  Val(..),
  Type(..),

  subsumes,
  subsume,
  like,
  typeVal,

) where

import Data.Data
import Data.DateTime
import Data.Text (Text, pack)

-------------------------------------------------------------------------------
-- Types
-------------------------------------------------------------------------------

-- Columns types have a subsumption rule which dictates when we upcast the type of the
-- values in column. If we have a column of Bool values with a single String element
-- in the middle of the data then then we upcast to String. If the user specifes (Maybe a)
-- type for the column then the column treats mismatched values as missing values.
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
subsumes (MT a) (MT b) = subsumes a b
subsumes _ _ = False

subsume :: Type -> Val -> Val
subsume ST v = case v of
  D x -> S (pack $ show x)
  I x -> S (pack $ show x)
  S x -> S x
  B x -> S (pack $ show x)
  T x -> S (pack $ show x)
subsume DT v = case v of
  D x -> D x
  I x -> D (fromIntegral x)
subsume IT v = case v of
  I x -> I x

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
