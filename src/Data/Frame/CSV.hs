{-# LANGUAGE OverloadedStrings #-}

module Data.Frame.CSV (
  fromCsv,
  fromCsvHeaders,
) where

import Data.Frame.HFrame (HDataFrame, Val(..), fromVectors)

import Debug.Trace
import Control.Applicative

import Data.Vector
import qualified Data.Vector as V

import Data.Csv
import qualified Data.Attoparsec.Char8 as P8

import Data.Text.Encoding (decodeUtf8)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BLS

type CsvData a = Vector (Vector a)

parseCsv :: BLS.ByteString -> Either String (CsvData BS.ByteString)
parseCsv x = decode NoHeader x

parseCsvHeader :: BLS.ByteString -> Either String (CsvData BS.ByteString)
parseCsvHeader x = decode HasHeader x

refineTypes :: CsvData BS.ByteString -> CsvData Val
refineTypes = V.map (V.map decodeVal)

decodeVal :: BS.ByteString -> Val
decodeVal x = case P8.parseOnly P8.number $ x of
  Left _ -> S (decodeUtf8 x)
  Right (P8.D n) -> D n
  Right (P8.I n) -> I n

fromCsv :: FilePath -> IO (Either String (HDataFrame Int Int))
fromCsv fname = do
  contents <- BLS.readFile fname
  let result = parseCsv contents
  case result of
    Left err -> return $ Left err
    Right bs -> do
      let xs = refineTypes bs
      let n = V.length (V.head xs)
      return $ Right $ fromVectors xs [1..n]

fromCsvHeaders fname = do
  contents <- BLS.readFile fname
  let result = parseCsv contents
  case result of
    Left err -> return $ Left err
    Right bs -> do
      let hdr = Prelude.map decodeUtf8 $ V.toList $ V.head bs
      let xs = refineTypes (V.tail bs)
      let n = V.length (V.head xs)
      return $ Right $ fromVectors xs hdr
