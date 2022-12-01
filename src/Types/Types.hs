module Types.Types where

import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.Map (Map)
import Data.String (IsString (..))
import Test.QuickCheck

type TableName = String

type ColName = String

-- Order in which to sort query results (ascending or descending)
data Order = Asc | Desc
  deriving (Show, Eq)

-- Datatype for filtering on rows in Pandas (akin to WHERE clauses in SQL)
data BoolExp
  = OpC CompOp Comparable Comparable -- Comparison operations
  | OpA ArithOp Comparable Comparable -- Arithmetic Operations
  | OpL LogicOp Bool Bool -- Logical operations
  | OpN NullOp ColName -- isna() / notna() in Pandas
  deriving (Eq, Show)

-- Values that can be compared in SQL queries
data Comparable
  = ColName ColName -- column name
  | LitInt Int -- literal ints
  | LitString String -- literal strings
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

-- Logical (binary) operations
data LogicOp
  = And Bool Bool
  | Or Bool Bool
  deriving (Eq, Show)

-- Unary operations for checking if a column is null / not-null
-- akin to isna() & notna() in Pandas
data NullOp
  = IsNull ColName -- Check whether a column contains null values
  | IsNotNull ColName -- Check whether a column contains non-null values
  deriving (Eq, Show)

-- Aggregate Functions to be used with a GroupBy
data AggFunc
  = Count
  | Avg
  | Sum
  | Min
  | Max
  deriving (Eq, Show, Enum, Bounded)

-- The manner in which the join in SQL (or "merge" in Pandas) should be performed
data JoinStyle = LeftJoin | RightJoin | InnerJoin
  deriving (Show, Eq, Enum)