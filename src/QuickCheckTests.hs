-- QUESTION FOR JOE: how do we import the module defined in the Types folder?
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ImportQualifiedPost #-}

module QuickCheckTests where

import Data.Data (Data, gMapQ)
import Data.Generics.Aliases (ext1Q)
import Data.Maybe (isNothing)
import Parser (Parser)
import Parser qualified as P
-- import SQLParser
import Test.QuickCheck (Arbitrary (..), Gen)
import Test.QuickCheck qualified as QC
import Types.PandasTypes
import Types.SQLTypes
import Types.TableTypes
import Types.Types

-- Arbitrary SQL queries
instance Arbitrary Query where
  arbitrary = undefined
  shrink = undefined

instance Arbitrary Table where
  arbitrary = undefined
  shrink = undefined

quickCheckN :: QC.Testable prop => Int -> prop -> IO ()
quickCheckN n = QC.quickCheckWith $ QC.stdArgs {QC.maxSuccess = n, QC.maxSize = 100}

-- | Generate a small set of names for generated tests. These names are guaranteed to not include
-- reserved words
genTableName :: Gen TableName
genTableName = QC.elements ["_G", "x", "X", "y", "x0", "X0", "xy", "XY", "_x"]

-- prop_roundtrip_val :: Value -> Bool
-- prop_roundtrip_val v = P.parse valueP (pretty v) == Right v

prop_roundtrip_query :: Query -> Bool
prop_roundtrip_query q = P.parse parseQuery (pretty q) == Right q

-- prop_roundtrip_stat :: Statement -> Bool
-- prop_roundtrip_stat s = P.parse statementP (pretty s) == Right s

prop_table_equality :: Query -> Bool
prop_table_equality q = pretty (getSQLTable q) == pretty (getPandasTable (translateSQL q))

prop_table_len :: Query -> Bool
prop_table_len q = len $ pretty (getSQLTable q) == len $ pretty (getPandasTable (translateSQL q))

getSQLTable :: Query -> Table
getSQLTable = undefined

getPandasTable :: Query -> Table
getPandasTable = undefined

-- Check if fields corresponding to irrelevant components of a Query
-- are set to Nothing after parsing
-- QUESTION FOR JOE: clarify how generics works???
-- Reference:
-- https://stackoverflow.com/questions/62580560/how-to-check-if-all-of-the-maybe-fields-in-a-haskell-record-are-nothing
fieldsAreNothing :: (Data d) => d -> Bool
fieldsAreNothing = and . gmapQ (const True `ext1Q` fieldsAreNothing)

irrelevantFieldsAreNothing :: (Data d) => d -> Maybe d
irrelevantFieldsAreNothing x = if fieldsAreNothing x then Nothing else Just x

initialQuery :: Query
initialQuery =
  Query
    { select = EmptySelect,
      from = EmptyFrom,
      wher = Nothing,
      groupBy = Nothing,
      limit = Nothing,
      orderBy = Nothing
    }
