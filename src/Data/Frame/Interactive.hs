{-# OPTIONS_GHC -fno-warn-orphans #-}

module Data.Frame.Interactive where

import Data.Frame.Frame
import Data.Frame.HFrame
import Data.Frame.Pretty

instance (Show i, Show k, Show v, Pretty k, Pretty i)
  => Show (DataFrame i k v) where
  show = showDataFrame

instance (Pretty k, Pretty i)
  => Show (HDataFrame i k) where
  show = showHDataFrame

-- terrible hack
instance Num Val where
  fromInteger = I
  (+) = undefined
  (*) = undefined
  abs = undefined
  signum = undefined

-- terrible hack
instance Fractional Val where
  (/) = undefined
  recip = undefined
  fromRational = D . fromRational
