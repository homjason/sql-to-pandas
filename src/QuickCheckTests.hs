module QuickCheckTests where

-- QUESTION FOR JOE: how do we import the module defined in the Types folder?

import Parser (Parser)
import Parser qualified as P
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

-- TODO: unit tests for parsing