{-# OPTIONS_GHC -fno-warn-orphans #-}

module Data.Frame.Interactive where

import Data.Frame.HFrame
import Data.Frame.Pretty
import qualified Data.Vector.Unboxed as V

instance (Pretty k, Pretty i, V.Unbox i)
  => Show (HDataFrame i k) where
  show = showHDataFrame
