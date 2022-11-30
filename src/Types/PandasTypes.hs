module Types.PandasTypes where

import Control.Applicative ((<|>))
import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.Map
import Data.String (IsString (..))
import Test.QuickCheck

{-
Things we (might) need types for:
- Dataframe names (eg. `df`)
- Column names (regular strings)
- Projecting one or more columns (eg. `df[['col1', 'col2']]`)
- Pandas function names (eg. drop_duplicates(), sort_values(), )
- Applying a Boolean filter (eg. countries[countries['area'] > 1000])
-}

type TableName = String

type ColName = String

-- An atomic Pandas command
data Command = Command
  { df :: TableName,
    cols :: Maybe [ColName],
    fn :: Maybe Func
  }
  deriving (Eq, Show)

-- Sequence of Pandas commands
newtype Block = Block [Command]
  deriving (Eq, Show)

-- Instances for joining Blocks together
instance Semigroup Block where
  Block s1 <> Block s2 = Block (s1 <> s2)

instance Monoid Block where
  mempty = Block []

data Order = Asc | Desc
  deriving (Show, Eq)

-- Pandas functions
-- For AggFunc, the 1st ColName arg is the column we're aggregating over
-- and the 2nd ColName arg is the new (alias) name for the aggregated column
data Func
  = DropDuplicates (Maybe [ColName])
  | SortValues ColName Order
  | Rename (Map ColName ColName)
  | GroupBy [ColName]
  | Agg AggFunc ColName ColName
  | Loc [BoolExp] -- filtering on rows (akin to WHERE in SQL)
  | Merge MergeExp
  | Unique [ColName]
  | Head Int -- limit no. of rows in output
  | ResetIndex
  deriving (Show, Eq)

-- Unary operations for checking if a column is null / not-null
-- akin to isna() & notna() in Pandas
data NullOp
  = IsNull ColName -- Check whether a column contains null values
  | IsNotNull ColName -- Check whether a column contains non-null values
  deriving (Eq, Show)

-- Merge expressions for joining tables in Pandas
data MergeExp = MkMerge
  { rightDf :: TableName,
    leftOn :: ColName,
    rightOn :: ColName,
    how :: MergeStyle
  }
  deriving (Show, Eq)

-- The manner in which the join ("merge") should be performed in Pandas
data MergeStyle = LeftJoin | RightJoin | InnerJoin
  deriving (Show, Eq)

-- Aggregate Functions
data AggFunc
  = Count
  | Avg
  | Sum
  | Min
  | Max
  deriving (Eq, Show, Enum, Bounded)

-- Values that can be compared in SQL queries
data Comparable
  = ColName ColName -- column name
  | LitInt Int -- literal ints
  | LitString String -- literal strings
  deriving (Eq, Show)

-- Datatype for filtering on rows in Pandas (akin to WHERE clauses in SQL)
data BoolExp
  = OpC CompOp Comparable Comparable -- Comparison operations
  | OpA ArithOp Comparable Comparable -- Arithmetic Operations
  | OpL LogicOp Bool Bool -- Logical operations
  | OpN NullOp ColName -- isna() / notna() in Pandas
  deriving (Eq, Show)

-- Comparison (binary) operations that return a Boolean
data CompOp
  = Eq -- `=` :: Comparable -> Comparable -> Bool
  | Gt -- `>`  :: Comparable -> Comparable -> Bool
  | Ge -- `>=` :: Comparable -> Comparable -> Bool
  | Lt -- `<`  :: Comparable -> Comparable -> Bool
  | Le -- `<=` :: Comparable -> Comparable -> Bool
  deriving (Eq, Show, Enum, Bounded)

-- Arithmetic (binary) operations
data ArithOp
  = Plus -- `+`  :: Comparable -> Comparable -> Comparable
  | Minus -- `-`  :: Comparable -> Comparable -> Comparable
  | Times -- `*`  :: Comparable -> Comparable -> Comparable
  | Divide -- `/` :: Comparable -> Comparable -> Comparable   -- floor division
  | Modulo -- `%`  :: Comparable -> Comparable -> Comparable   -- modulo
  deriving (Eq, Show, Enum, Bounded)

-- QUESTION: Why don't Enum or Bounded work?
-- Logical (binary) operations
data LogicOp
  = And Bool Bool
  | Or Bool Bool
  deriving (Eq, Show)
