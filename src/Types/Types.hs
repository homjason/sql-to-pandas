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
  deriving (Show, Eq, Enum, Bounded)

-- Datatype for filtering on rows in Pandas (akin to WHERE clauses in SQL)
data BoolExp
  = OpC Comparable CompOp Comparable -- Comparison operations
  | OpA Comparable ArithOp Comparable -- Arithmetic Operations
  -- TODO: change the line below so that it takes in Expressions (Comparables?)
  -- and not Bools
  | OpL BoolExp LogicOp BoolExp
  | OpN NullOp ColName -- isna() / notna() in Pandas
  deriving (Eq, Show)

-- Values that can be compared in SQL queries
data Comparable
  = ColName ColName -- column name
  | LitInt Int -- literal ints
  | LitString String -- literal strings
  | LitDouble Double -- literal doubles
  deriving (Eq, Show)

-- Datatype representing unary/binary operators
data Bop = CompOp CompOp | ArithOp ArithOp | LogicOp LogicOp
  deriving (Eq, Show)

-- Binary operator precedence
-- (similar to Haskell's operator precedence)
level :: Bop -> Int
level (LogicOp Or) = 2
level (LogicOp And) = 3
level (CompOp _) = 4
level (ArithOp Plus) = 6
level (ArithOp Minus) = 6
level (ArithOp Times) = 7
level (ArithOp Divide) = 7
level (ArithOp Modulo) = 7

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
  = And
  | Or
  deriving (Eq, Show, Enum, Bounded)

-- Unary operations for checking if a column is null / not-null
-- akin to isna() & notna() in Pandas
data NullOp
  = IsNull -- Check whether a column contains null values
  | IsNotNull -- Check whether a column contains non-null values
  deriving (Eq, Show, Enum, Bounded)

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
  deriving (Show, Eq, Enum, Bounded)