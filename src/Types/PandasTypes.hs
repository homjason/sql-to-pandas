module Types.PandasTypes where

import Control.Applicative ((<|>))
import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.Map
import Data.String (IsString (..))
import Test.QuickCheck
import Types.TableTypes
import Types.Types

{-
Things we (might) need types for:
- Dataframe names (eg. `df`)
- Column names (regular strings)
- Projecting one or more columns (eg. `df[['col1', 'col2']]`)
- Pandas function names (eg. drop_duplicates(), sort_values(), )
- Applying a Boolean filter (eg. countries[countries['area'] > 1000])
-}

-- An atomic Pandas command
data Command = Command
  { df :: TableName,
    cols :: Maybe [ColName],
    fn :: Maybe [Func]
  }
  deriving (Eq, Show)

-- A block is a sequence of Pandas commands
newtype Block = Block [Command]
  deriving (Eq, Show)

-- Instances for joining Blocks together
instance Semigroup Block where
  Block s1 <> Block s2 = Block (s1 <> s2)

instance Monoid Block where
  mempty = Block []

-- Pandas functions
-- For AggFunc, the 1st ColName arg is the column we're aggregating over
data Func
  = SortValues ColName Order
  | Rename (Map ColName ColName)
  | Group [ColName]
  | Aggregate AggFunc ColName
  | Loc BoolExp -- filtering on rows (akin to WHERE in SQL)
  | Merge MergeExp
  | Unique [ColName]
  | Head Int -- limit no. of rows in output
  | ResetIndex
  deriving (Show, Eq)

-- Merge expressions for joining tables in Pandas
data MergeExp = MkMerge
  { rightDf :: TableName,
    leftOn :: ColName,
    rightOn :: ColName,
    how :: JoinStyle
  }
  deriving (Show, Eq)

-- Datatype for filtering rows (LOC function in Pandas)
-- The only unary operators supported are "IS NULL" / "IS NOT NULL",
data BoolExp
  = Op1 BoolExp Uop
  | Op2 BoolExp Bop BoolExp
  | CompVal Comparable
  deriving (Eq, Show)

-- Values that can be compared in SQL queries (either columns or literal values)
data Comparable
  = ColName ColName TableName -- column name
  | LitInt Int -- literal ints (positive / negative)
  | LitString String -- literal strings
  | LitDouble Double -- literal doubles
  deriving (Eq, Show)

-- Postfix unary operators for checking if a column is null / not-null
data Uop = IsNull | IsNotNull
  deriving (Eq, Show, Bounded, Enum)

-- Datatype representing different families of (infix) binary operators
data Bop = Comp CompOp | Arith ArithOp | Logic LogicOp
  deriving (Eq, Show)

-- (Infix) binary operators used for comparisons
data CompOp
  = Eq -- `=`
  | Neq -- `!=`
  | Gt -- `>`
  | Ge -- `>=`
  | Lt -- `<`
  | Le -- `<=`
  deriving (Eq, Show, Enum, Bounded)

-- Logical (binary) operations
data LogicOp
  = And
  | Or
  deriving (Eq, Show, Enum, Bounded)