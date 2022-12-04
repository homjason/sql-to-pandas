-- QUESTION FOR JOE: how do we import the module defined in the Types folder?
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ImportQualifiedPost #-}

module QuickCheckTests where

import Data.Data (Data, gMapQ)
import Data.Generics.Aliases (ext1Q)
import Data.Maybe (isNothing)
import Parser (Parser)
import Parser qualified as P
import Print
import SQLParser
import Test.QuickCheck (Arbitrary (..), Gen)
import Test.QuickCheck qualified as QC
import Translator
import Types.PandasTypes
import Types.SQLTypes
import Types.TableTypes
import Types.TableTypes (Row)
import Types.Types

-- Generator for aggregate functions
genAggFunc :: Gen AggFunc
genAggFunc = QC.elements [Count, Avg, Sum, Min, Max]

-- Generator for the style in which two tables should be joined
genJoinStyle :: Gen JoinStyle
genJoinStyle = QC.elements [LeftJoin, RightJoin, InnerJoin]

-- Generator for the choice of sort order
genOrder :: Gen Order
genOrder = QC.elements [Asc, Desc]

genSelectExp :: Gen SelectExp
genSelectExp =
  QC.oneof
    [ Cols <$> genColName,
      DistinctCols <$> genColName,
      genAgg,
      return EmptySelect
    ]

-- List of permitted colnames
colNames :: [ColName]
colNames = ["total_bill", "tip", "day", "party_size"]

-- Generator for column names
genColName :: Gen ColName
genColName = QC.elements colNames

-- Generator for Aggregate Function Expressions in SELECT
genAgg :: Gen SelectExp
genAgg = do
  fn <- genAggFunc
  col <- genColName
  let newCol = col ++ "_agg"
  return $ Agg fn col newCol

-- Generator for FROM expressions
genFromExp :: Gen FromExp
genFromExp = undefined

-- Generator for WHERE expressions
genBoolExp :: Gen BoolExp
genBoolExp = undefined

-- Arbitrary SQL queries
instance Arbitrary Query where
  arbitrary :: Gen Query
  arbitrary = undefined

  shrink :: Query -> [Query]
  shrink = undefined

-- QUESTION FOR JOE: given an input schema, how do we arbitrarily generate a Map in QuickCheck? (Each Row is a Map)
-- See https://stackoverflow.com/questions/59472608/quickcheck-sequential-map-key-generation
genRow :: Gen Row
genRow = undefined

-- | Generator for small strings (from HW4)
smallString :: Gen String
smallString = resize 15 arbitrary

-- | Generator that produces small lists of non-empty strings
smallNonEmptyStringLists :: Gen String
smallNonEmptyStringLists = resize 15 $ listOf1 arbitrary

-- Generator for small Ints (between 0 & 5)
smallInt :: Gen Int
smallInt = chooseInt (0, 5)

instance Arbitrary Table where
  arbitrary :: Gen Table
  arbitrary = undefined

  shrink :: Table -> [Table]
  shrink = undefined

quickCheckN :: QC.Testable prop => Int -> prop -> IO ()
quickCheckN n = QC.quickCheckWith $ QC.stdArgs {QC.maxSuccess = n, QC.maxSize = 100}

-- | Generate a small set of names for generated tests. These names are guaranteed to not include
-- reserved words
genTableName :: Gen TableName
genTableName = QC.elements ["_G", "x", "X", "y", "x0", "X0", "xy", "XY", "_x"]

prop_roundtrip_val :: Value -> Bool
prop_roundtrip_val v = P.parse valueP (pretty v) == Right v

prop_roundtrip_query :: Query -> Bool
prop_roundtrip_query q = parseQuery (pretty q) == Right q

prop_table_equality :: Query -> Bool
prop_table_equality q = pretty (getSQLTable q) == pretty (getPandasTable (translateSQL q))

prop_table_len :: Query -> Bool
prop_table_len q = length (pretty (getSQLTable q)) == length (pretty (getPandasTable (translateSQL q)))

getSQLTable :: Query -> Table
getSQLTable = undefined

getPandasTable :: Block -> Table
getPandasTable = undefined

-- Check if fields corresponding to irrelevant components of a Query
-- are set to Nothing after parsing
-- QUESTION FOR JOE: clarify how generics works???
-- Reference:
-- https://stackoverflow.com/questions/62580560/how-to-check-if-all-of-the-maybe-fields-in-a-haskell-record-are-nothing
fieldsAreNothing :: (Data d) => d -> Bool
fieldsAreNothing = and . gmapQ (const True `ext1Q` isNothing)

irrelevantFieldsAreNothing :: (Data d) => d -> Maybe d
irrelevantFieldsAreNothing x =
  if fieldsAreNothing x then Nothing else Just x

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

-- checkTableEquality :: Table --> Table
-- checkTableEquality t1 t2 = Data.List.sort t1 == Data.List.sort t2

-- TODO: unit tests for parsing
