-- QUESTION: how do we import the module defined in the Types folder?
-- {-# LANGUAGE ImportQualifiedPost #-}
-- {-# LANGUAGE ScopedTypeVariables #-}

module QuickCheckTests where

-- TOOD: look at error instance for QC
-- no need to do install importance

import Control.Monad
import Data.Array
import Data.Data (Data)
import Data.List
import Data.Map (Map)
import Data.Map qualified as Map
import Data.Maybe (catMaybes, isNothing)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Vector qualified as Vector
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
-- import Types.TableTypes
import Types.TableTypes
import Types.Types

-- Generator for SELECT expressions
genSelectExp :: Gen SelectExp
genSelectExp =
  QC.oneof
    [ Cols <$> QC.resize 3 (QC.listOf1 genColExp),
      DistinctCols <$> QC.resize 3 (QC.listOf1 genColExp),
      return Star
    ]
  where
    -- Generator for columns / aggregate functions in SELECT expressions
    genColExp :: Gen ColExp
    genColExp = QC.oneof [Col <$> genColName, genAgg]

    -- Generator for Aggregate Function Expressions in SELECT
    genAgg :: Gen ColExp
    genAgg = do
      fn <- arbitrary
      Agg fn <$> genColName

-- Generator for an individual column name (taken from a colname pool)
genColName :: Gen ColName
genColName = do
  colNames <- genColNamePool
  QC.elements colNames

-- Generator for the pool of column names ("col0", "col1", etc.)
genColNamePool :: Gen [ColName]
genColNamePool = do
  numCols <- QC.chooseInt (1, 4)
  return ["col" ++ show i | i <- [0 .. numCols - 1]]

-- Generator for table names
genTableName :: Gen TableName
genTableName = QC.elements tableNames

-- Generator for FROM expressions
genFromExp :: Gen FromExp
genFromExp =
  QC.oneof
    [ Table <$> genTableName,
      TableJoin <$> arbitrary
    ]

-- Generator for WHERE expressions
genWhereExp :: Gen WhereExp
genWhereExp = QC.sized aux
  where
    aux 0 = CompVal <$> genComparable
    aux n =
      QC.oneof
        [ liftM2 Op1 (aux (n - 1)) arbitrary,
          liftM3 Op2 (aux (n - 1)) genBop (aux (n - 1)),
          CompVal <$> genComparable
        ]

-- Implementation of QC.listOf (from meeting with Joe)
-- TODO: delete
genList :: Gen a -> Gen [a]
genList g = QC.sized aux
  where
    aux 0 = pure []
    aux n = (:) <$> g <*> aux (n `div` 2)

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
    rightTable <- genTableName `QC.suchThat` (/= leftTable)
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
-- Generator for Schemas & Tables

-- Generator for the type of a column
genColType :: Gen ColType
genColType = QC.oneof [return IntC, return StringC, return DoubleC]

-- Generator for schemas
genSchema :: Gen Schema
genSchema = do
  -- Randomly generate a "pool" of column names to choose from
  colNamePool <- Vector.fromList <$> genColNamePool

  -- Randomly choose an int representing the no. of columns in the schema
  numCols <- QC.chooseInt (1, length colNamePool)

  -- Choose a prefix of the (ordered) pool of colnames of length numCols
  let colNames = Vector.toList $ Vector.slice 0 numCols colNamePool

  -- Randomly choose a type for each column
  colTypes <- QC.vectorOf numCols genColType

  -- Create schema
  return $ Map.fromList (zip colNames colTypes)

-- Maps each column name to the index for that column
getColIdxs :: Schema -> Map ColName Int
getColIdxs schema =
  Map.mapWithKey (\colName _ -> colName `getColIndex` schema) schema

-- Generator that produces a Table adhering to an input schema
-- If an empty schema is provided, this generator returns the special
-- QuickCheck discard value (QC.discard)
genTable :: Schema -> Gen Table
genTable schema
  | schema == Map.empty = return QC.discard
  | otherwise = do
    let numCols = Map.size schema
    -- Arbitrarily generate the no. of rows
    numRows <- QC.chooseInt (1, 5)

    -- colToIdx :: Map ColName Int
    -- Map from colnames to indexes
    let colToIdx = getColIdxs schema

    -- colToGenerator :: Map ColName (Gen Column)
    -- Map each colname to the generator for that column
    let colToGen = Map.mapWithKey (\_ cType -> genCol numRows cType) schema

    -- idxToGenerator :: Map Int (Gen Column)
    -- Map each column index to the generator for that column
    let idxToGen = colToGen `Map.compose` invertMap colToIdx

    -- colMap :: Map Int Column
    -- Use sequence to pull the Gen out of the Map, then bind to obtain the
    -- resultant map from indexes to the (randomly generated) columns
    colMap <- sequence idxToGen

    -- cols :: [[Maybe Value]]
    -- Extract the randomly generated columns from their constructor
    let cols = [c | col@(Column c) <- map snd (Map.toList colMap)]

    -- All elements in the table, laid out in row-major format
    -- elts :: [Maybe Value]
    let elts = concat (transpose cols)

    -- Create the Table & use return to create a Generator of Tables
    return $ listArray ((0, 0), (numRows - 1, numCols - 1)) elts

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

-- "Wrapper" generators that randomly generates a table schema
-- & a table that abides by that schema
genSchemaAndTable :: Gen Table
genSchemaAndTable = genSchema >>= genTable

-- TODO: implement function that figures out if a table accepts a query

-- TODO: generate queries that are accepted by a table

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
colNames = ["col0", "col1", "col2", "col3", "col4"]

-- List of permitted TableNames
tableNames :: [TableName]
tableNames = ["df", "df1", "df2", "_G", "x", "X", "y", "x0", "X0", "xy", "XY", "_x"]

--------------------------------------------------------------------------------
-- Convenience functions for QuickCheck (independent of generators)
quickCheckN :: QC.Testable prop => Int -> prop -> IO ()
quickCheckN n = QC.quickCheckWith $ QC.stdArgs {QC.maxSuccess = n, QC.maxSize = 100}
