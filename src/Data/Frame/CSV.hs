{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}

module Data.Frame.CSV (
  {-fromCsv,-}
  fromCsvHeaders
) where

import Data.Frame.HFrame

import Control.Applicative

import Data.Data
import Data.List (transpose)
import Data.Text (Text)
import qualified Data.Vector as V

import Data.Csv
import qualified Data.Attoparsec.Char8 as P8

import Data.Text.Encoding (decodeUtf8)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BLS

-------------------------------------------------------------------------------
-- Parsing
-------------------------------------------------------------------------------

type CsvData a = V.Vector (V.Vector a)

unVector = V.toList . (V.map V.toList)

parseCsv :: BLS.ByteString -> Either String (CsvData BS.ByteString)
parseCsv x = decode NoHeader x

parseCsvHeader :: BLS.ByteString -> Either String (CsvData BS.ByteString)
parseCsvHeader x = decode HasHeader x

decodeVal :: BS.ByteString -> Val
decodeVal x = case P8.parseOnly P8.number $ x of
  Left _ -> S (decodeUtf8 x)
  Right (P8.D n) -> D n
  Right (P8.I n) -> I (fromIntegral n)

-------------------------------------------------------------------------------
-- CSV Type Conversion
-------------------------------------------------------------------------------

refineTypes :: CsvData BS.ByteString -> [[Val]]
refineTypes xs = transpose $ unVector $ V.map (V.map decodeVal) xs

like :: Val -> Val -> Bool
like (D _) (D _) = True
like (I _) (I _) = True
like (S _) (S _) = True
like (B _) (B _) = True
like (M (Just a)) (M (Just b)) = like a b
like (M (Just _)) (M Nothing) = True
like (M (Nothing)) (M (Just _)) = True
like (M (Nothing)) (M Nothing) = True
{-like (Dt _) (Dt _) = True-}
like _ _ = False

lower :: [Val] -> Block
lower vs@((D x):_) = buildD vs []
  where
    buildD [] acc = dblock acc
    buildD ((D x):xs) acc = buildD xs (x:acc)
lower vs@((I x):_) = buildI vs []
  where
    buildI [] acc = iblock acc
    buildI ((I x):xs) acc = buildI xs (x:acc)
lower vs@((S x):_) = buildS vs []
  where
    buildS [] acc = sblock acc
    buildS ((S x):xs) acc = buildS xs (x:acc)

allAlike :: [Val] -> Bool
allAlike xs = all (like t) xs
  where t = head xs

lowerTypes :: [Val] -> Block
lowerTypes xs | allAlike xs = lower xs
              | otherwise   = error ("unlike" ++ (show xs))

-- Heterogeneous value
data Val
  = D !Double
  | I !Int
  | S !Text
  | B !Bool
  | M !(Maybe Val)
  {-| Dt DateTime-}
  deriving (Eq, Show, Ord, Data, Typeable)

-------------------------------------------------------------------------------
-- Toplevel
-------------------------------------------------------------------------------

fromCsv :: FilePath -> IO (Either String (HDataFrame Int Int))
fromCsv fname = do
  contents <- BLS.readFile fname
  let result = parseCsv contents
  case result of
    Left err -> return $ Left err
    Right bs -> do
      let xs = refineTypes (V.tail bs)
      let n = length xs
      return $ Right $ fromBlocks (map lowerTypes xs) [1..n]

fromCsvHeaders fname = do
  contents <- BLS.readFile fname
  let result = parseCsv contents
  case result of
    Left err -> return $ Left err
    Right bs -> do
      let hdr = Prelude.map decodeUtf8 $ V.toList $ V.head bs
      let xs = refineTypes (V.init bs)
      let n = length xs
      return $ Right $ fromBlocks (map lowerTypes xs) hdr
