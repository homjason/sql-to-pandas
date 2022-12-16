-- QUESTION: how do we import the module defined in the Types folder?
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE ScopedTypeVariables #-}

module QuickCheckTests where

import Control.Monad
import Data.Array
import Data.Data (Data)
import Data.Map (Map)
import Data.Map qualified as Map
import Data.Maybe (catMaybes, isNothing)
import Data.Set (Set)
import Data.Set qualified as Set
import Parser (Parser)
import Parser qualified as P
import Print
import SQLParser
import Test.QuickCheck (Arbitrary (..), Gen)
import Test.QuickCheck qualified as QC
import Test.QuickCheck qualified as Qc
import Translator
import Types.PandasTypes
import Types.SQLTypes
import Types.TableTypes
import Types.Types

genSelectExp :: Gen SelectExp
genSelectExp =
  QC.oneof
    [ Cols <$> QC.resize 3 (QC.listOf1 genColExp),
      DistinctCols <$> QC.resize 3 (QC.listOf1 genColExp),
      return Star
    ]

genColExp :: Gen ColExp
genColExp =
  QC.oneof
    [ Col <$> genColName,
      genAgg
    ]

-- Generator for column names
genColName :: Gen ColName
genColName = QC.elements colNames

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
genFromExp =
  QC.oneof
    [ Table <$> genTableName,
      TableJoin <$> arbitrary
    ]

-- Generator for WHERE expressions
genWhereExp :: Gen WhereExp
genWhereExp =
  QC.oneof
    [ liftM2 Op1 genWhereExp arbitrary,
      liftM3 Op2 genWhereExp genBop genWhereExp,
      CompVal <$> genComparable
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
  arbitrary = do
    select <- genSelectExp
    from <- genFromExp
    wher <- QC.frequency [(1, return Nothing), (7, Just <$> genWhereExp)]
    groupBy <- QC.frequency [(1, return Nothing), (7, Just <$> genGroupBy)]
    orderBy <-
      QC.frequency
        [ (1, return Nothing),
          (7, Just <$> liftM2 (,) genColName (arbitrary :: Gen Order))
        ]
    limit <-
      QC.frequency
        [ (1, return Nothing),
          (7, Just <$> genSmallInt)
        ]
    return $ Query select from wher groupBy orderBy limit

genGroupBy :: Gen [ColName]
genGroupBy = QC.resize 2 (QC.listOf1 genColName)

-- | Generator for non-empty strings of length <= 5 that
-- only contain letters a-d
genSmallString :: Gen String
genSmallString = QC.resize 5 (QC.listOf1 (QC.elements "abcd"))

-- Generator for small Ints (between 0 & 5)
genSmallInt :: Gen Int
genSmallInt = QC.chooseInt (0, 5)

-- Generator for small Doubles (between 0.00 & 50.00)
genSmallDouble :: Gen Double
genSmallDouble = QC.choose (0.00 :: Double, 50.00 :: Double)

----------------------------------------------------
-- Arbitrary instances for Enum types

-- Generator for Bops uses the Arbitrary instance for various binary operations
genBop :: Gen Bop
genBop =
  QC.oneof
    [ Comp <$> arbitrary,
      Arith <$> arbitrary,
      Logic <$> arbitrary
    ]

-- Generators for Enum types (unary & binary operators)
-- just use QC.arbitraryBoundedEnum
instance Arbitrary CompOp where
  arbitrary = QC.arbitraryBoundedEnum

instance Arbitrary ArithOp where
  arbitrary = QC.arbitraryBoundedEnum

instance Arbitrary LogicOp where
  arbitrary = QC.arbitraryBoundedEnum

instance Arbitrary Uop where
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

-------------------------------------------------------------------------------

schema :: Schema
schema = mkSchema [("bill", IntC), ("day", StringC)]

-- >>> colNameToIdx schema
-- fromList [("bill",0),("day",1)]

getColIdxs :: Schema -> Map ColName Int
getColIdxs schema =
  Map.mapWithKey (\colName _ -> colName `getColIndex` schema) schema

-- Generator for Tables
-- Schema :: Map ColName ColType
genTable :: Schema -> Gen Table
genTable schema = do
  -- Arbitrarily generate the no. of rows
  numRows <- QC.chooseInt (1, 10)

  let numCols = Map.size schema

  -- ScopedTypeVariables GHC extension gives us
  -- type annotations for let bindings (for ease of readability)
  let colToIdx = getColIdxs schema :: Map ColName Int

  let idxToCol = invertMap colToIdx :: Map Int ColName

  -- colToGenerator :: Map ColName (Gen Column)
  let colToGenerator = Map.mapWithKey (\colName colType -> genCol numRows colType) schema

  -- idxToGenerator :: Map Int (Gen Column)
  let idxToGenerator = colToGenerator `Map.compose` idxToCol

  -- TODO: "concatenate" the columns together to form a table

  -- TODO: delete the dummy return statement below
  return $ listArray ((0, 0), (2, 2)) []

-- Generates a column of a fixed length containing
-- Maybe values of a particular type (for 1/8th of the time, generate Nothing)
genCol :: Int -> ColType -> Gen Column
genCol colLen colType =
  case colType of
    IntC -> Column <$> genIntCol colLen
    StringC -> Column <$> genStringCol colLen
    DoubleC -> Column <$> genDoubleCol colLen
  where
    genIntCol :: Int -> Gen [Maybe Value]
    genIntCol colLen =
      QC.vectorOf
        colLen
        ( QC.frequency
            [ (1, return Nothing),
              (7, Just . IntVal <$> genSmallInt)
            ]
        )

    genStringCol :: Int -> Gen [Maybe Value]
    genStringCol colLen =
      QC.vectorOf
        colLen
        ( QC.frequency
            [ (1, return Nothing),
              (7, Just . StringVal <$> genSmallString)
            ]
        )

    genDoubleCol :: Int -> Gen [Maybe Value]
    genDoubleCol colLen =
      QC.vectorOf
        colLen
        ( QC.frequency
            [ (1, return Nothing),
              (7, Just . DoubleVal <$> genSmallDouble)
            ]
        )

-- >>> QC.sample' (genCol 3 IntC)
-- [Column [Just (IntVal 1),Just (IntVal 4),Just (IntVal 5)],Column [Nothing,Just (IntVal 3),Just (IntVal 4)],Column [Just (IntVal 2),Just (IntVal 5),Just (IntVal 4)],Column [Just (IntVal 3),Just (IntVal 1),Just (IntVal 3)],Column [Just (IntVal 2),Just (IntVal 5),Just (IntVal 0)],Column [Just (IntVal 4),Just (IntVal 0),Just (IntVal 5)],Column [Just (IntVal 5),Just (IntVal 0),Just (IntVal 1)],Column [Just (IntVal 4),Nothing,Just (IntVal 4)],Column [Just (IntVal 1),Just (IntVal 3),Just (IntVal 1)],Column [Nothing,Just (IntVal 2),Just (IntVal 2)],Column [Just (IntVal 2),Just (IntVal 0),Nothing]]

--------------------------------------------------------------------------------

-- | Generate a small set of names for generated tests. These names are guaranteed to not include
-- reserved words

-- TODO: fix!
-- prop_roundtrip_val :: Value -> Bool
-- prop_roundtrip_val v = P.parse valueP (pretty v) == Right v

-- prop_roundtrip_query :: Query -> Bool
-- prop_roundtrip_query q = parseQuery (pretty q) == Right q

-- prop_table_equality :: Query -> Bool
-- prop_table_equality q = pretty (getSQLTable q) == pretty (getPandasTable (translateSQL q))

-- prop_table_len :: Query -> Bool
-- prop_table_len q = length (pretty (getSQLTable q)) == length (pretty (getPandasTable (translateSQL q)))

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
--------------------------------------------------------------------------------
-- Permitted table & column names

-- List of permitted colnames
colNames :: [ColName]
colNames = ["total_bill", "tip", "day", "party_size"]

-- List of permitted TableNames
tableNames :: [TableName]
tableNames = ["df", "df1", "df2", "_G", "x", "X", "y", "x0", "X0", "xy", "XY", "_x"]

--------------------------------------------------------------------------------
-- Convenience functions for QuickCheck (independent of generators)
quickCheckN :: QC.Testable prop => Int -> prop -> IO ()
quickCheckN n = QC.quickCheckWith $ QC.stdArgs {QC.maxSuccess = n, QC.maxSize = 100}
