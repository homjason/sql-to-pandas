-- File to declare types

module Types.SQLTypes where

import Control.Applicative ((<|>))
import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.String (IsString (..))
import Test.QuickCheck ()
import Types.TableTypes ()
import Types.Types

-- Datatype representing a SQL query
data Query = Query
  { select :: SelectExp,
    from :: FromExp,
    wher :: Maybe WhereExp,
    groupBy :: Maybe [ColName],
    orderBy :: Maybe (ColName, Order),
    limit :: Maybe Int
  }
  deriving (Eq, Show)

-- Datatype that encapsulates various SQL query conditions
-- (WHERE / GROUP BY / ORDER BY / LIMIT)
data Condition
  = Wher WhereExp
  | GroupBy [ColName]
  | OrderBy (ColName, Order)
  | Limit Int
  deriving (Eq, Show)

-- Datatype for SELECT clauses in SQL
data SelectExp
  = Cols [ColExp] -- colnames are a list of string names
  | DistinctCols [ColExp] -- SELECT DISTINCT in SQL
  | Star -- SELECT * in SQL (select all columns from the tablename)
  deriving
    (Eq, Show)

-- Column expressions are allowed to be colname string literals
-- or aggregate functions for SELECT expressions
data ColExp
  = Col ColName
  | Agg AggFunc ColName -- Aggregate functions (used with GROUP BY clauses)
  deriving
    (Eq, Show)

-- Datatype for FROM clauses in SQL
-- We can either select from a named table or from a subquery
data FromExp
  = Table TableName
  | TableJoin JoinExp
  deriving (Eq, Show)

-- Datatype for JOIN clauses in SQL (only equality joins supported)
-- Note: updated JoinExp to be a record (12/4)
data JoinExp = Join
  { leftTable :: TableName,
    leftCol :: ColName,
    rightTable :: TableName,
    rightCol :: ColName,
    style :: JoinStyle
  }
  deriving (Eq, Show)

-- Datatype representing any table/colname
data Name = T TableName | C ColName
  deriving (Eq, Show)

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

-- Logical (binary) operations
data LogicOp
  = And
  | Or
  deriving (Eq, Show, Enum, Bounded)
