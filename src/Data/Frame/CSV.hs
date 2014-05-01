{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

{-# OPTIONS_GHC -fno-warn-incomplete-patterns #-}

module Data.Frame.CSV (
  fromCsvNoHeaders,
  fromCsvHeaders
) where

import Data.Frame.Types
import Data.Frame.HFrame

import Control.Applicative
import Control.Monad.State

import Data.Csv

import Data.Data
import Data.List (transpose)
import Data.Text (Text, pack)
import Data.DateTime
import qualified Data.Vector as V

import Data.Text.Encoding (decodeUtf8)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BLS
import qualified Data.Attoparsec.Char8 as P8

-------------------------------------------------------------------------------
-- Parsing
-------------------------------------------------------------------------------

type CsvData a = V.Vector (V.Vector a)

parseCsv :: BLS.ByteString -> Either String (CsvData BS.ByteString)
parseCsv x = decode NoHeader x

parseCsvHeader :: BLS.ByteString -> Either String (CsvData BS.ByteString)
parseCsvHeader x = decode HasHeader x

decodeVal :: BS.ByteString -> Val
decodeVal x = case P8.parseOnly P8.number $ x of
  Left _         -> S (decodeUtf8 x)
  Right (P8.D n) -> D n
  Right (P8.I n) -> I (fromIntegral n)

-------------------------------------------------------------------------------
-- CSV Type Conversion
-------------------------------------------------------------------------------

refineColumn :: [Val] -> Either String Type
refineColumn xs = go Nothing xs
  where
    go (Just lub) [] = Right lub
    go Nothing (x:xs) = go (Just (typeVal x)) xs
    go (Just lub) (x:xs)
      | lub == typeVal x         = go (Just lub) xs
      | lub `subsumes` typeVal x = go (Just lub) xs
      | typeVal x `subsumes` lub = go (Just (typeVal x)) xs
      | otherwise                = Left "No subsumption"

refineFrame :: [[Val]] -> Either String [Type]
refineFrame = sequence . map refineColumn

refine :: [[Val]] -> Either String [Block]
refine cols = do
  tys <- refineFrame cols
  let svals = zipWith subsumeColumn tys cols
  return $ zipWith refineBlock tys svals

subsumeColumn :: Type -> [Val] -> [Val]
subsumeColumn ty xs = map (subsume ty) xs

refineBlock :: Type -> [Val] -> Block
refineBlock IT vs = iblock (go vs)
  where
    go [] = []
    go ((I x):xs) = x : go xs
refineBlock DT vs = dblock (go vs)
  where
    go [] = []
    go ((D x):xs) = x : go xs
refineBlock ST vs = sblock (go vs)
  where
    go [] = []
    go ((S x):xs) = x : go xs

parseVals :: CsvData BS.ByteString -> [[Val]]
parseVals xs = transpose $ unVector $ V.map (V.map decodeVal) xs
  where unVector = V.toList . (V.map V.toList) -- XXX

-------------------------------------------------------------------------------
-- Toplevel
-------------------------------------------------------------------------------

fromCsvNoHeaders :: FilePath -> IO (Either String (HDataFrame Int Int))
fromCsvNoHeaders fname = do
  contents <- BLS.readFile fname
  let result = parseCsv contents
  case result of
    Left err -> return $ Left err
    Right bs -> do
      let xs = parseVals bs
      let n = length xs
      case refine xs of
        Left err -> error err
        Right blocks -> return $ Right $ fromBlocks blocks [1..n]

fromCsvHeaders :: FilePath -> IO (Either String (HDataFrame Int Text))
fromCsvHeaders fname = do
  contents <- BLS.readFile fname
  let result = parseCsv contents
  case result of
    Left err -> return $ Left err
    Right bs -> do
      let hdr = Prelude.map decodeUtf8 $ V.toList $ V.head bs
      let xs = parseVals (V.init bs)
      let n = length xs
      case refine xs of
        Left err -> error err
        Right blocks -> return $ Right $ fromBlocks blocks hdr
