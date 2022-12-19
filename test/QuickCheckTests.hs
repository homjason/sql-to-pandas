module QuickCheckTests where

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
import Test.QuickCheck (Arbitrary (..), Gen, Property, (==>))
import Test.QuickCheck qualified as QC
import Translator
import Types.PandasTypes qualified as Pandas
import Types.SQLTypes
import Types.TableTypes
import Types.Types

-- | Takes a generator w/ type Gen a, and returns a generator of Maybe a
-- that generates Nothing 1/8th of the time
sometimesGenNothing :: Gen a -> Gen (Maybe a)
sometimesGenNothing g =
  QC.frequency
    [ (1, return Nothing),
      (7, Just <$> g)
    ]

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

-------------------------------------------------------------------------------
-- Generators for schema-independent sub-components of SQL queries

instance Arbitrary ColExp where
  arbitrary :: Gen ColExp
  arbitrary = genColExp

  shrink :: ColExp -> [ColExp]
  shrink colExp =
    case colExp of
      Col colName -> []
      Agg _ colName -> [Col colName]

-- Generator for columns / aggregate functions in SELECT expressions
genColExp :: Gen ColExp
genColExp = QC.oneof [Col <$> genColName, genAgg]
  where
    -- Generator for Aggregate Function Expressions in SELECT
    genAgg :: Gen ColExp
    genAgg = do
      fn <- arbitrary
      Agg fn <$> genColName

instance Arbitrary SelectExp where
  arbitrary :: Gen SelectExp
  arbitrary = genSelectExp

  shrink :: SelectExp -> [SelectExp]
  shrink selectExp =
    case selectExp of
      Cols colExps -> map Cols (shrink colExps)
      DistinctCols colExps -> map Cols (shrink colExps)
      Star -> []

-- Generator for SELECT expressions
genSelectExp :: Gen SelectExp
genSelectExp =
  QC.oneof
    [ Cols <$> QC.resize 3 (QC.listOf1 genColExp),
      DistinctCols <$> QC.resize 3 (QC.listOf1 genColExp),
      return Star
    ]

instance Arbitrary WhereExp where
  arbitrary = genWhereExp
  shrink = shrinkWhereExp

-- Generator for WHERE expressions
genWhereExp :: Gen WhereExp
genWhereExp =
  QC.oneof
    [ liftM2 Op1 (CompVal <$> genColNameComparable) arbitrary,
      genOp2,
      CompVal <$> genComparable
    ]
  where
    -- Generator for Comparable values
    genComparable :: Gen Comparable
    genComparable =
      QC.oneof
        [ genColNameComparable,
          genCompInt,
          LitString <$> genSmallString
        ]

    genOp2 :: Gen WhereExp
    genOp2 = do
      bop <- genBop
      case bop of
        Arith _ ->
          liftM3
            Op2
            (CompVal <$> genCompInt)
            (return bop)
            (CompVal <$> genCompInt)
        Comp bop' ->
          liftM3
            Op2
            (CompVal <$> genComparable)
            (return bop)
            (CompVal <$> genComparable)
        Logic bop' -> liftM3 Op2 genCompOpExp (return bop) genCompOpExp

    -- Generator for Bops uses Arbitrary instance for binary operations
    genBop :: Gen Bop
    genBop =
      QC.oneof
        [ Comp <$> arbitrary,
          Arith <$> arbitrary,
          Logic <$> arbitrary
        ]

    genCompOpExp :: Gen WhereExp
    genCompOpExp = do
      compOp <- (arbitrary :: Gen CompOp)
      liftM3
        Op2
        (CompVal <$> genComparable)
        (return (Comp compOp))
        (CompVal <$> genComparable)

    genColNameComparable :: Gen Comparable
    genColNameComparable = ColName <$> genColName

    genCompInt :: Gen Comparable
    genCompInt = LitInt <$> genSmallInt

-- Shrinker for WHERE expressions
shrinkWhereExp :: WhereExp -> [WhereExp]
shrinkWhereExp w =
  case w of
    Op1 w' uop -> shrink w'
    Op2 w1 bop w2 -> shrink w1 ++ shrink w2
    CompVal (ColName _) -> []
    CompVal (LitInt n) -> map (CompVal . LitInt) (shrink n)
    CompVal (LitString str) -> map (CompVal . LitString) (shrink str)

genComparable :: Gen Comparable
genComparable =
  QC.oneof
    [ ColName <$> genColName,
      LitInt <$> genSmallInt,
      LitString <$> genSmallString
    ]

instance Arbitrary FromExp where
  arbitrary = genFromExp
  shrink = shrinkFromExp

-- Generator for FROM expressions
genFromExp :: Gen FromExp
genFromExp =
  QC.oneof
    [ Table <$> genTableName,
      TableJoin <$> arbitrary
    ]

-- Shrinker for FROM expressions
shrinkFromExp :: FromExp -> [FromExp]
shrinkFromExp fromExp =
  case fromExp of
    Table _ -> []
    TableJoin joinExp ->
      [Table (leftTable joinExp), Table (rightTable joinExp)]

-- Generator for Join Expressions
-- (We mandate that the leftTable & rightTable in a join must be different)
instance Arbitrary JoinExp where
  arbitrary :: Gen JoinExp
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
    wher <- sometimesGenNothing genWhereExp
    groupBy <- sometimesGenNothing genGroupBy
    orderBy <-
      sometimesGenNothing $
        liftM2 (,) genColName (arbitrary :: Gen Order)
    limit <- sometimesGenNothing genSmallInt
    return $ Query select from wher groupBy orderBy limit

  shrink :: Query -> [Query]
  shrink (Query s f w gb ob l) =
    let shrunken =
          [(s', f', w') | s' <- shrink s, f' <- shrink f, w' <- shrink w]
     in map (\(s', f', w') -> Query s' f' w' Nothing Nothing Nothing) shrunken

-- | Generator for non-empty strings of length <= 5 that
-- only contain letters a-d
genSmallString :: Gen String
genSmallString = QC.resize 5 (QC.listOf1 (QC.elements "abcd"))

-- Generator for small Ints (between 1 & 5)
genSmallInt :: Gen Int
genSmallInt = QC.chooseInt (1, 5)

-- Generator for small Doubles (between 0.00 & 50.00)
genSmallDouble :: Gen Double
genSmallDouble = QC.choose (0.00 :: Double, 50.00 :: Double)

genGroupBy :: Gen [ColName]
genGroupBy = QC.resize 2 (QC.listOf1 genColName)

----------------------------------------------------
-- Arbitrary instances for Enum types

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
genColType = QC.oneof [return IntC, return StringC]

instance Arbitrary ColType where
  arbitrary = genColType
  shrink c = [IntC]

-- | Generator for schemas
genSchema :: Gen Schema
genSchema = do
  -- Randomly generate a "pool" of column names to choose from
  colNamePool <- Vector.fromList <$> genColNamePool

  -- Randomly choose an int representing the no. of columns in the schema
  numCols <- QC.chooseInt (1, length colNamePool)

  -- Choose a prefix of the (ordered) pool of colnames of length numCols
  -- (this guarantees that column names in the schema are distinct)
  let colNames = Vector.toList $ Vector.slice 0 numCols colNamePool

  -- Randomly choose a type for each column
  colTypes <- QC.vectorOf numCols genColType

  -- Create schema
  return $ Map.fromList (zip colNames colTypes)

-- Generator that produces a non-empty Table adhering to an input Schema
-- If an empty Schema is provided, this generator returns the special
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

    -- TODO: figure out how to associate table name with table
    -- (add tablename to schema???)
    -- TODO: look at allocateTable in LuStepper.hs

    let table = listArray ((0, 0), (numRows - 1, numCols - 1)) elts

    -- Create the Table & use return to create a Generator of Tables
    return $ listArray ((0, 0), (numRows - 1, numCols - 1)) elts

-- Generates a column of a fixed length containing
-- Maybe values of a particular type (for 1/8th of the time, generate Nothing)
genCol :: Int -> ColType -> Gen Column
genCol colLen colType =
  case colType of
    IntC -> Column <$> genIntCol colLen
    StringC -> Column <$> genStringCol colLen
  where
    genIntCol :: Int -> Gen [Maybe Value]
    genIntCol colLen =
      QC.vectorOf colLen $ sometimesGenNothing (IntVal <$> genSmallInt)

    genStringCol :: Int -> Gen [Maybe Value]
    genStringCol colLen =
      QC.vectorOf colLen $ sometimesGenNothing (StringVal <$> genSmallString)

-- | "Wrapper" generator that randomly generates a table schema
-- & a table that abides by that schema
genSchemaAndTable :: Gen Table
genSchemaAndTable = genSchema >>= genTable

-- | Given a (table, schema) pair,
-- decide if a particular query is accepted by the table
accept :: (Table, Schema) -> Query -> Bool
accept (table, schema) (Query s f w gb ob l) =
  checkSelect schema s
    && checkWhere schema w
    && checkGroupBy schema gb
    && checkOrderBy schema ob
    && checkLimit table l
  where
    -- Checks if the columns in a SQL query are present in the table schema
    checkColsInQuery :: [ColName] -> Schema -> Bool
    checkColsInQuery colNames schema =
      let colsInQuery = Set.fromList colNames
          colsInTable = (Set.fromList . Map.keys) schema
       in colsInQuery `Set.isSubsetOf` colsInTable

    -- Checks that the table schema accepts the SELECT expression
    checkSelect :: Schema -> SelectExp -> Bool
    checkSelect schema selectExp =
      case selectExp of
        Star -> True
        Cols colExps -> colExpHandler colExps
        DistinctCols colExps -> colExpHandler colExps
      where
        colExpHandler colExps =
          let cs = [c | cExp@(Col c) <- colExps]
           in checkColsInQuery cs schema

    -- Checks that the table schema accepts the WHERE expression
    checkWhere :: Schema -> Maybe WhereExp -> Bool
    checkWhere schema Nothing = True
    checkWhere schema (Just whereExp) =
      case whereExp of
        Op1 w uop -> checkWhere schema (Just w)
        Op2 w1 bop w2 ->
          checkWhere schema (Just w1) && checkWhere schema (Just w2)
        CompVal (ColName c) -> checkColsInQuery [c] schema
        -- Literal string/int/double values are accepted
        CompVal _ -> True

    -- Checks that the table schema accepts the GROUP BY expression
    checkGroupBy :: Schema -> Maybe [ColName] -> Bool
    checkGroupBy schema gb =
      case gb of
        Just cols -> checkColsInQuery cols schema
        Nothing -> True

    -- Checks that the table schema accepts the ORDER BY expression
    checkOrderBy :: Schema -> Maybe (ColName, Order) -> Bool
    checkOrderBy schema ob =
      case ob of
        Just (col, order) -> checkColsInQuery [col] schema
        Nothing -> True

    -- Check that no. of rows in LIMIT expression <= no. of rows in the table
    checkLimit :: Table -> Maybe Int -> Bool
    checkLimit _table Nothing = True
    checkLimit table (Just n) =
      let (numRows, _) = dimensions table
       in n <= numRows

------------------------------------------------------------------------------

-- List of permitted TableNames (single letters from A - Z)
tableNames :: [TableName]
tableNames = map (: []) ['A' .. 'Z']

--------------------------------------------------------------------------------
-- Convenience functions for QuickCheck (independent of generators)
quickCheckN :: QC.Testable prop => Int -> prop -> IO ()
quickCheckN n = QC.quickCheckWith $ QC.stdArgs {QC.maxSuccess = n, QC.maxSize = 100}

--------------------------------------------------------------------------------
-- Round trip properties
prop_roundtrip_query :: Query -> Property
prop_roundtrip_query q@(Query s f w gb ob l) =
  case s of
    Cols colExps ->
      not (null colExps) ==> P.parse queryP (pretty q) == Right q
    DistinctCols colExps ->
      not (null colExps) ==> P.parse queryP (pretty q) == Right q
    _ -> QC.property (P.parse queryP (pretty q) == Right q)

prop_roundtrip_select :: SelectExp -> Property
prop_roundtrip_select s =
  case s of
    Star -> QC.property (P.parse selectExpP (pretty s) == Right s)
    Cols colExps ->
      not (null colExps) ==> P.parse selectExpP (pretty s) == Right s
    DistinctCols colExps ->
      not (null colExps) ==> P.parse selectExpP (pretty s) == Right s

prop_roundtrip_from :: FromExp -> Bool
prop_roundtrip_from f = P.parse fromExpP (pretty f) == Right f

prop_roundtrip_where :: WhereExp -> Bool
prop_roundtrip_where w = P.parse whereExpP ("where " <> pretty w) == Right w

--------------------------------------------------------------------------------
-- Other properties

-- | Check that Schemas only contain unique columns (no duplicate columns)
prop_schema_uniqueCols :: Schema -> Bool
prop_schema_uniqueCols schema =
  let colNames = (Set.fromList . Map.keys) schema
   in Set.size colNames == Map.size schema

--------------------------------------------------------------------------------
qc :: IO ()
qc = do
  putStrLn "roundtrip_select"
  QC.quickCheck prop_roundtrip_select
  putStrLn "roundtrip_from"
  QC.quickCheck prop_roundtrip_from
  putStrLn "roundtrip_where"
  QC.quickCheck prop_roundtrip_where
  putStrLn "schema_uniqueCols"
  QC.quickCheck prop_schema_uniqueCols
