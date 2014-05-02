{-# LANGUAGE OverloadedStrings #-}

{-# OPTIONS_GHC -fno-warn-incomplete-patterns #-}

module Data.Frame.CSV (
  fromCsvNoHeaders,
  fromCsvHeaders,
  fromCsvWith,

  refineColumn,
  subsumeColumn,

  testSubsume,
) where

import Data.Frame.Types
import Data.Frame.HFrame
import Data.Frame.Internal (def, Default)

import Control.Applicative
import Control.Monad.State

import Data.Csv

import Data.Data
import Data.Maybe
import Data.List (transpose)
import Data.Text (Text, pack)
import Data.DateTime
import qualified Data.Vector as V

import Data.Text.Encoding (decodeUtf8)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BLS

import qualified Data.Attoparsec.Text as A
import qualified Data.Attoparsec.Char8 as P8

-------------------------------------------------------------------------------
-- Parsing
-------------------------------------------------------------------------------

type CsvData a = V.Vector (V.Vector a)

parseCsv :: BLS.ByteString -> Either String (CsvData BS.ByteString)
parseCsv = decode NoHeader

parseCsvHeader :: BLS.ByteString -> Either String (CsvData BS.ByteString)
parseCsvHeader = decode HasHeader

parseVal :: P8.Parser Val
parseVal =  P8.string "true"  *> pure (B True)
        <|> P8.string "false" *> pure (B False)
        <|> P8.string "NA"    *> pure NA
        <|> parseNumber
        <|> parseString

parseString :: P8.Parser Val
parseString = S . decodeUtf8 <$> P8.takeByteString

parseNumber :: P8.Parser Val
parseNumber = do
  v <- P8.number
  case v of
    (P8.D n) -> return $ D n
    (P8.I n) -> return $ I (fromIntegral n)


decodeVal :: BS.ByteString -> Val
decodeVal x = case P8.parseOnly parseVal x of
  Left _         -> S (decodeUtf8 x)
  Right val      -> val

-------------------------------------------------------------------------------
-- CSV Type Conversion
-------------------------------------------------------------------------------

-- Parse the data from the CSV file into the least general type possible for the column.

refineColumn :: [Val] -> Either String Type
refineColumn xs = lub xs

refineFrame :: [[Val]] -> Either String [Type]
refineFrame = sequence . fmap refineColumn

refine :: [[Val]] -> Either String [Block]
refine cols = do
  tys <- refineFrame cols
  let svals = zipWith subsumeColumn tys cols
  return $ zipWith refineBlock tys svals

subsumeColumn :: Type -> [Val] -> [Val]
subsumeColumn ty xs = fmap (subsume ty) xs

testSubsume :: [Val] -> Either String (Type, [Val])
testSubsume xs = do
  ty <- refineColumn xs
  return $ (ty, subsumeColumn ty xs)

extractI :: Val -> Int
extractI (I x) = x
extractI _ = def

extractD :: Val -> Double
extractD (D x) = x
extractD _ = def

extractS :: Val -> Text
extractS (S x) = x
extractS _ = def

extractB :: Val -> Bool
extractB (B x) = x
extractB _ = def

makeMask NA = False
makeMask x = True

-- Unbox the types after we've guaranteed that all the types in the column are the most general type.
refineBlock :: Type -> [Val] -> Block
refineBlock IT vs = iblock (fmap extractI vs)
refineBlock DT vs = dblock (fmap extractD vs)
refineBlock ST vs = sblock (fmap extractS vs)
refineBlock BT vs = bblock (fmap extractB vs)
refineBlock (MT t) vs = mblock values mask
  where
    mask   = fmap makeMask vs
    values = refineBlock t vs
refineBlock Any vs = error "not possible" -- XXX

-- Cassava gives us data row-wise, but we need it column wise. So we do a ridiculously expensive transpose
-- transpose.
parseVals :: CsvData BS.ByteString -> [[Val]]
parseVals xs = transpose $ unVector $ V.map (V.map decodeVal) xs
  where
    unVector :: CsvData a -> [[a]]
    unVector = V.toList . (V.map V.toList)

-------------------------------------------------------------------------------
-- Toplevel
-------------------------------------------------------------------------------

data CsvOptions = CsvOptions
  { indexCol   :: Int
  , header     :: Bool
  , delimeter  :: Text
  } deriving (Eq, Show)

-- | Parse a CSV file without headers into DataFrame
fromCsvNoHeaders :: FilePath -> IO (Either String (HDataFrame Int Int))
fromCsvNoHeaders fname = do
  contents <- BLS.readFile fname

  case parseCsv contents of
    Left err -> return $ Left err
    Right bs -> do
      let vals = parseVals bs
      let n = length vals

      case refine vals of
        Left err -> error err
        Right blocks -> return $ Right $ fromBlocks blocks [1..n]

-- | Parse a CSV file with headers into DataFrame
fromCsvHeaders :: FilePath -> IO (Either String (HDataFrame Int Text))
fromCsvHeaders fname = do
  contents <- BLS.readFile fname

  case parseCsv contents of
    Left err -> return $ Left err
    Right bs -> do
      let (bs0, bs1) = (V.head bs, V.tail bs)
      let headers = fmap decodeUtf8 $ V.toList $ bs0
      let vals = parseVals bs1

      case refine vals of
        Left err -> error err
        Right blocks -> return $ Right $ fromBlocks blocks headers

fromCsvWith :: CsvOptions -> FilePath -> IO (Either String (HDataFrame Int Text))
fromCsvWith opts file = case header opts of
  True  -> fromCsvHeaders file
  False -> undefined -- fromCsvNoHeaders file
