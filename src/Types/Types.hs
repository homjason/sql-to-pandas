module Types.Types where

import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.Map (Map)
import Data.String (IsString (..))
import Test.QuickCheck

type TableName = String

type ColName = String

-- Order in which to sort rows in the query result (ascending or descending)
data Order = Asc | Desc
  deriving (Show, Eq, Enum, Bounded)

-- Datatype for filtering rows (WHERE clauses in SQL)
-- The only unary operators supported are "IS NULL" / "IS NOT NULL",
-- which are applied postfix in SQL
data WhereExp
  = Op1 WhereExp Uop
  | Op2 WhereExp Bop WhereExp
  | CompVal Comparable
  deriving (Eq, Show)

-- Values that can be compared in SQL queries (either columns or literal values)
data Comparable
  = ColName ColName -- column name
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

-- Binary operator precedence (similar to Haskell's operator precedence)
-- https://rosettacode.org/wiki/Operator_precedence
-- Higher number means that the operator binds more tightly to the operands
level :: Bop -> Int
level (Logic Or) = 3
level (Logic And) = 3
level (Comp _) = 4
level (Arith Plus) = 6
level (Arith Minus) = 6
level (Arith Times) = 7
level (Arith Divide) = 7
level (Arith Modulo) = 7

-- (Infix) binary operators used for comparisons
data CompOp
  = Eq -- `=`
  | Neq -- `!=`
  | Gt -- `>`
  | Ge -- `>=`
  | Lt -- `<`
  | Le -- `<=`
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
