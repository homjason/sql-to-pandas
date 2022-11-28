-- File to declare types

module SQLTypes where

import Control.Applicative ((<|>))
import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.String (IsString (..))
import Test.QuickCheck

-- A SQL Query is a collection of statements
newtype Query = Query [Statement]
  deriving (Eq, Show)

-- QUESTION: Do we need semigroup/monoid?
-- instance Semigroup SQLQuery where
--   SQLQuery s1 <> SQLQuery s2 = Block (s1 <> s2)

-- instance Monoid SQLQuery where
--   mempty = SQLQuery []

-- TODO: JOIN, HAVING, Set Operations

data Statement
  = Select SelectExp -- x = e
  | From FromExp
  | Where WhereExp
  | GroupBy [Name] -- Group by one or more columns
  | Limit Int -- Limit no. of rows in query output
  | OrderBy Name Order -- Can only order by one column
  | Empty -- ; (semicolon signalling the end of a SQL query)
  deriving (Eq, Show)

-- Order in which to sort query results (ascending or descending)
data Order = Asc | Desc
  deriving (Eq, Show)

-- Datatype for SELECT clauses in SQL
data SelectExp
  = Cols [Name] -- colnames are a list of string names
  | DistinctCols [Name] -- SELECT DISTINCT in SQL
  | Agg AggFunc Name -- Aggregate functions (used with GROUP BY clauses)
  | Val Value -- literal values
  | As Name -- "AS" keyword in SQL (renaming columns)
  deriving (Eq, Show)

-- Datatype for FROM clauses in SQL
-- We can either select from a named table or from a subquery
data FromExp
  = TableName Name
  | SubQuery Query
  deriving (Eq, Show)

-- Datatype for WHERE clauses in SQL
data WhereExp
  = OpC Comparable Comparable -- Comparison operations
  | OpA Comparable Comparable -- Arithmetic Operations
  | OpL Bool Bool -- Logical operations
  | OpN Name -- NULL/IS NULL
  deriving (Eq, Show)

data Value
  = NilVal -- nil
  | IntVal Int -- 1
  | BoolVal Bool -- false, true
  | StringVal String -- "abd"
  | TableVal Name -- <not used in source programs>
  deriving (Eq, Show, Ord)

-- Type representing values that can be compared in SQL queries
data Comparable
  = ColName Name -- column name
  | Int
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
data LogOp
  = And Bool Bool
  | Or Bool Bool
  deriving (Eq, Show)

-- Unary operations for checking if a column is null / not-null
data NullOp
  = IsNull Name -- Check whether a column contains null values
  | IsNotNull Name -- Check whether a column contains non-null values
  deriving (Eq, Show)

-- Aggregate Functions
data AggFunc
  = Count
  | Avg
  | Sum
  | Min
  | Max
  deriving (Eq, Show, Enum, Bounded)

type Name = String -- column names
