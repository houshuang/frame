{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE BangPatterns #-}

{-# OPTIONS_GHC -funbox-strict-fields #-}

module Data.Frame.CSV (
  fromCsv,
  fromCsvHeaders
) where

import Data.Frame.HFrame

import Control.Applicative
import Control.Monad.State

import Data.Data
import Data.List (transpose)
import Data.Text (Text, pack)
import Data.DateTime
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
              | otherwise = error ("unlike" ++ (show xs))

-------------------------------------------------------------------------------
-- Types
-------------------------------------------------------------------------------

subsumes :: Type -> Type -> Bool
subsumes ST _  = True
subsumes DT IT = True
subsumes _ _ = False

subsume :: Type -> Val -> Val
subsume ST v = case v of
  D x -> S (pack $ show x)
  I x -> S (pack $ show x)
  S x -> S x
  B x -> S (pack $ show x)
  T x -> S (pack $ show x)
subsume DT v = case v of
  D x -> D x
  I x -> D (fromIntegral x)
subsume IT v = case v of
  I x -> I x

like :: Val -> Val -> Bool
like (D _) (D _) = True
like (I _) (I _) = True
like (S _) (S _) = True
like (B _) (B _) = True
like (T _) (T _) = True

like (M (Just a)) (M (Just b))  = like a b
like (M (Just _)) (M Nothing)   = True
like (M (Nothing)) (M (Just _)) = True
like (M (Nothing)) (M Nothing)  = True
like _ _ = False

data Type = DT | IT | ST | BT | MT Type | TT
  deriving (Eq, Show, Ord)

-- Heterogeneous value
data Val
  = D {-# UNPACK #-} !Double
  | I {-# UNPACK #-} !Int
  | S {-# UNPACK #-} !Text
  | B !Bool
  | M !(Maybe Val)
  | T !DateTime
  deriving (Eq, Show, Ord, Data, Typeable)

typeVal (D x) = DT
typeVal (I x) = IT
typeVal (S x) = ST
typeVal (B x) = BT
typeVal (T x) = TT
typeVal (M x) = error "maybe type"

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
      let xs = parseVals (V.tail bs)
      let n = length xs
      case refine xs of
        Left err -> error err
        Right blocks -> return $ Right $ fromBlocks blocks [1..n]

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
