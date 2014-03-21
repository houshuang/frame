{-# OPTIONS_GHC -fno-warn-orphans #-}

module Data.Frame.Interactive where

import Data.Frame.HFrame
import Data.Frame.Pretty

instance (Pretty k, Pretty i)
  => Show (HDataFrame i k) where
  show = showHDataFrame
