{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}

module Data.Frame.Pretty where

import Data.Text

import Text.PrettyPrint
import qualified Text.PrettyPrint.Boxes as PB

class Pretty a where
  ppr :: Int -> a -> Doc

instance Pretty String where
  ppr _ x = text x

instance Pretty Bool where
  ppr _ True = text "True"
  ppr _ False = text "False"

instance Pretty Integer where
  ppr _ x = integer x

instance Pretty Double where
  ppr _ x = double x

instance Pretty Float where
  ppr _ x = float x

instance Pretty Text where
  ppr _ x = text (unpack x)

-- | Lift a pretty printable object into a box.
ppb :: Pretty a => a -> PB.Box
ppb a = PB.text (render (ppr 0 a))
