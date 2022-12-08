-- QUESTION FOR JOE: how do we import the module defined in the Types folder?
{-# LANGUAGE ImportQualifiedPost #-}

module QuickCheckTests where

import Data.Data (Data)
-- import Data.Generics.Aliases (ext1Q)
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
import Types.Types

-- TODO: fix!
genSelectExp :: Gen SelectExp
genSelectExp =
  QC.oneof
    [ Cols <$> genColName,
      DistinctCols <$> genColName,
      return EmptySelect
    ]

genColExp :: Gen ColExp
genColExp =
  QC.oneof
    [ Col <$> genColName,
      genAgg
    ]

-- List of permitted colnames
colNames :: [ColName]
colNames = ["total_bill", "tip", "day", "party_size"]

-- Generator for column names
genColName :: Gen ColName
genColName = QC.elements colNames

-- List of permitted TableNames
tableNames :: [TableName]
tableNames = ["df", "df1", "df2", "_G", "x", "X", "y", "x0", "X0", "xy", "XY", "_x"]

-- Generator for table names
genTableName :: Gen TableName
genTableName = QC.elements tableNames

-- Generator for Aggregate Function Expressions in SELECT
genAgg :: Gen ColExp
genAgg = do
  fn <- arbitrary
  Agg fn <$> genColName

-- Generator for FROM expressions
genFromExp :: Gen FromExp
genFromExp = undefined

-- Generator for WHERE expressions
genBoolExp :: Gen BoolExp
genBoolExp =
  QC.oneof
    [ OpC <$> genComparable <*> arbitrary <*> genComparable,
      OpA <$> genComparable <*> arbitrary <*> genComparable,
      OpL <$> arbitrary <*> arbitrary <*> arbitrary,
      OpN <$> arbitrary <*> genColName
    ]

-- Generator for Comparable values
genComparable :: Gen Comparable
genComparable =
  QC.oneof
    [ ColName <$> genColName,
      LitInt <$> genSmallInt,
      LitString <$> genSmallString,
      LitDouble <$> genSmallDouble
    ]

-- TODO: how to ensure that genColName returns
-- an actual col from the table???

-- TODO: how to avoid leftTable & rightTable being the same?
-- (write a generator for pairs of distinct TableNames??)

-- Generator for Join Expressions
instance Arbitrary JoinExp where
  arbitrary = do
    leftTable <- genTableName
    leftCol <- genColName
    rightTable <- genTableName
    rightCol <- genColName
    style <- arbitrary
    return $
      Join
        { leftTable = leftTable,
          leftCol = leftCol,
          rightTable = rightTable,
          rightCol = rightCol,
          style = style
        }

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

-- | Generator for strings of length <= 10 that only contain letters a-d (from HW4)
genSmallString :: Gen String
genSmallString = QC.resize 10 (QC.listOf (QC.elements "abcd"))

-- | Generator that produces small lists of non-empty strings
genSmallNonEmptyStringLists :: Gen String
genSmallNonEmptyStringLists = QC.resize 15 $ QC.listOf1 arbitrary

-- Generator for small Ints (between 0 & 5)
genSmallInt :: Gen Int
genSmallInt = QC.chooseInt (0, 5)

-- Generator for small Doubles (between 0.00 & 50.00)
genSmallDouble :: Gen Double
genSmallDouble = QC.choose (0.00 :: Double, 50.00 :: Double)

----------------------------------------------------
-- Arbitrary instances for Enum types

-- Generators for unary & binary operators
instance Arbitrary CompOp where
  arbitrary = QC.arbitraryBoundedEnum

instance Arbitrary ArithOp where
  arbitrary = QC.arbitraryBoundedEnum

instance Arbitrary LogicOp where
  arbitrary = QC.arbitraryBoundedEnum

instance Arbitrary NullOp where
  arbitrary = QC.arbitraryBoundedEnum

-- Generator for aggregate functions
instance Arbitrary AggFunc where
  arbitrary = QC.arbitraryBoundedEnum

-- Generator for the style in which two tables should be joined
instance Arbitrary JoinStyle where
  arbitrary = QC.arbitraryBoundedEnum

-- Generator for the choice of sort order
instance Arbitrary Order where
  arbitrary = QC.arbitraryBoundedEnum

-----------------------------------------------------
-- Generators for Tables
instance Arbitrary Table where
  arbitrary :: Gen Table
  arbitrary = undefined

  shrink :: Table -> [Table]
  shrink = undefined

quickCheckN :: QC.Testable prop => Int -> prop -> IO ()
quickCheckN n = QC.quickCheckWith $ QC.stdArgs {QC.maxSuccess = n, QC.maxSize = 100}

-- | Generate a small set of names for generated tests. These names are guaranteed to not include
-- reserved words
-- genOtherTableName :: Gen TableName
-- genOtherTableName = do
--   table <-
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
-- fieldsAreNothing :: (Data d) => d -> Bool
-- fieldsAreNothing = and . gmapQ (const True `ext1Q` isNothing)

-- irrelevantFieldsAreNothing :: (Data d) => d -> Maybe d
-- irrelevantFieldsAreNothing x =
--   if fieldsAreNothing x then Nothing else Just x

-- checkTableEquality :: Table --> Table
-- checkTableEquality t1 t2 = Data.List.sort t1 == Data.List.sort t2

-- TODO: unit tests for parsing
