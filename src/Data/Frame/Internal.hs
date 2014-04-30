{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE KindSignatures #-}

module Data.Frame.Internal (
  HDataFrame(..),
  Block(..),
  Index,

  Columnable(..),
  Indexable(..),
  Default(..),
  Result(..),

  blockType,

  -- ** Lenses
  hdfdata,
  hdfindex,

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
import qualified Data.Vector.Unboxed as V
import qualified Data.HashMap.Strict as M

import Data.Data
import Data.Maybe
import Data.Monoid
import Data.Typeable
import Control.DeepSeq (NFData(..))
import Data.DateTime
import Data.Hashable (Hashable(..))

import Data.Text (Text, pack, unpack, empty)

import Control.Monad
import Control.Applicative
import Control.Lens (makeLenses, makePrisms)

-------------------------------------------------------------------------------
-- Indexes and Columns
-------------------------------------------------------------------------------

type Index i = V.Vector i

type family IxRep (s :: *) :: *
type instance IxRep Int      = Int
type instance IxRep String   = String
type instance IxRep Double   = Double
type instance IxRep Float    = Float
type instance IxRep Bool     = Bool
type instance IxRep DateTime = Int

class (Eq k, Show k, Hashable k) => Columnable k where
class (Ord i, Show i, V.Unbox (IxRep i), Default i) => Indexable i where
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
  { _hdfdata :: !(M.HashMap k Block)
  , _hdfindex :: !(Index i)
  } deriving (Eq)

-------------------------------------------------------------------------------
-- Blocks
-------------------------------------------------------------------------------

data Block
  = DBlock !(V.Vector Double)
  | IBlock !(V.Vector Int)
  | BBlock !(V.Vector Bool)
  | MBlock {
     _mdata  :: !Block
   , _bitmap :: !(V.Vector Bool)
   }
  | SBlock !(VB.Vector Text)
  | NBlock
  deriving (Eq, Show, Data, Typeable)

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
-- Block Type
-------------------------------------------------------------------------------

blockType :: Block -> TypeRep
blockType (DBlock a) = typeOf (0.0 :: Double)
blockType (IBlock a) = typeOf (1 :: Int)
blockType (BBlock a) = typeOf False
blockType (SBlock a) = typeOf (Data.Text.empty)
blockType (MBlock a _) = typeOf a -- XXX
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

makeLenses ''HDataFrame
makeLenses ''Block
makePrisms ''Block
