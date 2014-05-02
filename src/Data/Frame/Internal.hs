{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoMonomorphismRestriction #-}

{-# OPTIONS_GHC -fno-warn-incomplete-patterns #-}

module Data.Frame.Internal (
  HDataFrame(..),
  Block(..),
  IxRep,
  --Index,

  _filterKeys,
  _alterKeys,

  Columnable,
  Indexable(..),
  Default(..),
  Result(..),

  resultEither,
  blockType,

  -- ** Lenses
  _Data,
  _Index,

  -- ** Prisms
  _DBlock,
  _IBlock,
  _BBlock,
  _MBlock,
  _SBlock,
  _NBlock

) where

import Data.Foldable
import Data.Traversable
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import qualified Data.HashMap.Strict as M

import Data.Data
import Data.Monoid
import Control.DeepSeq (NFData(..))
import Data.DateTime
import Data.Hashable (Hashable(..))

import Data.Text (Text, pack, empty)

import Control.Monad
import Control.Applicative
import Control.Lens (lens, prism, Lens', Prism', makePrisms)
import Control.DeepSeq (NFData(..))

-------------------------------------------------------------------------------
-- Indexes and Columns
-------------------------------------------------------------------------------

-- type Index i = V.Vector i

type family IxRep (s :: *) :: *
type instance IxRep Int      = Int
type instance IxRep String   = String
type instance IxRep Double   = Double
type instance IxRep Float    = Float
type instance IxRep Bool     = Bool
type instance IxRep DateTime = Int

class (Eq k, Show k, Hashable k) => Columnable k where
class (Ord i, Show i, VU.Unbox (IxRep i), Default i) => Indexable i where
  ixto   :: i -> IxRep i
  ixfrom :: IxRep i -> i

instance Columnable Int where
instance Columnable String
instance Columnable Text
instance Columnable Bool

instance Indexable Int where
  ixto = id
  ixfrom = id

instance Indexable Double where
  ixto = id
  ixfrom = id

instance Indexable Float where
  ixto = id
  ixfrom = id

instance Indexable Bool where
  ixto = id
  ixfrom = id

instance Indexable DateTime where
  -- maxint(64) is 21 times the age of the unvierse, so we should be good for a while
  ixto = fromIntegral . toSeconds
  ixfrom = fromSeconds . fromIntegral

-------------------------------------------------------------------------------
-- Frame
-------------------------------------------------------------------------------

-- The heterogeneously typed dataframe.
data HDataFrame i k = HDataFrame
  { _hdfdata  :: !(M.HashMap k Block)
  , _hdfindex :: !(VB.Vector i)
  } deriving (Eq)

instance NFData k => NFData (HDataFrame i k) where
  rnf (HDataFrame dt _) = rnf dt

_Index :: Lens' (HDataFrame i k) (VB.Vector i)
_Index = lens _hdfindex (\f new -> f { _hdfindex = new })

_Data :: Lens' (HDataFrame i k) (M.HashMap k Block)
_Data = lens _hdfdata (\f new -> f { _hdfdata = new })

-- does a deep copy
_alterKeys ::(Eq b, Hashable b) => (a -> b) -> M.HashMap a v -> M.HashMap b v
_alterKeys f xs = M.fromList $ [(f a,b) | (a,b) <- M.toList xs]

-- O(n)
_filterKeys :: (k -> Bool) -> M.HashMap k v -> M.HashMap k v
_filterKeys f xs = M.filterWithKey (\k _ -> f k) xs

-------------------------------------------------------------------------------
-- Default Values
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

instance Default DateTime where
  def = startOfTime

instance Default (Maybe a) where
  def = Nothing

-------------------------------------------------------------------------------
-- Blocks
-------------------------------------------------------------------------------

data Block
  = DBlock !(VU.Vector Double)
  | IBlock !(VU.Vector Int)
  | BBlock !(VU.Vector Bool)
  | MBlock {
     _mdata  :: !Block
   , _bitmap :: !(VU.Vector Bool)
   }
  | SBlock !(VB.Vector Text)
  | NBlock
  deriving (Eq, Show, Data, Typeable)

instance NFData Block where
  rnf (MBlock dat _) = rnf dat
  rnf _ = ()

_DBlock :: Prism' Block (VU.Vector Double)
_DBlock = prism remit review
  where
    remit a = DBlock a

    review (DBlock a) = Right a
    review a = Left a

_IBlock :: Prism' Block (VU.Vector Int)
_IBlock = prism remit review
  where
    remit a = IBlock a

    review (IBlock a) = Right a
    review a          = Left a

_BBlock :: Prism' Block (VU.Vector Bool)
_BBlock = prism remit review
  where
    remit a = BBlock a

    review (BBlock a) = Right a
    review a          = Left a

_MBlock :: Prism' Block (Block, VU.Vector Bool)
_MBlock
  = prism remit review
  where
      remit (a, b) = MBlock a b
      review (MBlock a b) = Right (a, b)
      review a = Left a

_SBlock :: Prism' Block (VB.Vector Text)
_SBlock = prism remit review
  where
    remit a = SBlock a

    review (SBlock a) = Right a
    review a = Left a

_NBlock :: Prism' Block ()
_NBlock = prism remit review
  where
      remit () = NBlock
      review NBlock = Right ()
      review a = Left a

-------------------------------------------------------------------------------
-- Block Type
-------------------------------------------------------------------------------

blockType :: Block -> TypeRep
blockType (DBlock _) = typeOf (0.0 :: Double)
blockType (IBlock _) = typeOf (1 :: Int)
blockType (BBlock _) = typeOf False
blockType (SBlock _) = typeOf (Data.Text.empty)
blockType (MBlock a _) = mkTyConApp (mkTyCon "Maybe") [blockType a]
blockType NBlock = typeOf ()

-------------------------------------------------------------------------------
-- Result
-------------------------------------------------------------------------------

data Result a
  = Error String
  | Success a
  deriving (Eq, Show, Typeable)

instance (NFData a) => NFData (Result a) where
  rnf (Success a) = rnf a
  rnf (Error err) = rnf err

instance Functor Result where
  fmap f (Success a) = Success (f a)
  fmap _ (Error err) = Error err
  {-# INLINE fmap #-}

instance Monad Result where
  return = Success
  {-# INLINE return #-}
  Success a >>= k = k a
  Error err >>= _ = Error err
  {-# INLINE (>>=) #-}

instance Applicative Result where
  pure  = return
  {-# INLINE pure #-}
  (<*>) = ap
  {-# INLINE (<*>) #-}

instance MonadPlus Result where
  mzero = fail "mzero"
  {-# INLINE mzero #-}
  mplus a@(Success _) _ = a
  mplus _ b             = b
  {-# INLINE mplus #-}

instance Monoid (Result a) where
  mempty  = fail "mempty"
  {-# INLINE mempty #-}
  mappend = mplus
  {-# INLINE mappend #-}

instance Alternative Result where
  empty = mzero
  {-# INLINE empty #-}
  (<|>) = mplus
  {-# INLINE (<|>) #-}

resultEither :: Result b -> Either String b
resultEither (Error x)   = Left x
resultEither (Success x) = Right x
