-- File to declare types

module SQLTypes where

import Control.Applicative ((<|>))
import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.String (IsString (..))
import Test.QuickCheck

-- A SQL Query is a collection of statements
-- newtype Query = Query [Statement]
--   deriving (Eq, Show)

-- QUESTION: Do we need semigroup/monoid?
-- instance Semigroup SQLQuery where
--   SQLQuery s1 <> SQLQuery s2 = Block (s1 <> s2)

-- instance Monoid SQLQuery where
--   mempty = SQLQuery []

-- TODO: JOIN, HAVING, Set Operations

data Query = Query
  { select :: SelectExp,
    from :: FromExp,
    wher :: Maybe [WhereExp],
    groupBy :: Maybe [Name],
    limit :: Maybe Int,
    orderBy :: Maybe (Name, Order),
    empty :: ()
  }
  deriving (Eq, Show)

-- TODO: Think about how to represent this with semigroups and queries
-- instance Semigroup Query where
--   Query s1 <> Query s2 = Block (s1 <> s2)

-- instance Monoid Query where
--   mempty = Query {}

-- Order in which to sort query results (ascending or descending)
data Order = Asc | Desc
  deriving (Eq, Show)

-- Datatype for SELECT clauses in SQL
data SelectExp
  = Cols [Name] -- colnames are a list of string names
  | DistinctCols [Name] -- SELECT DISTINCT in SQL
  | Agg AggFunc Name Name -- Aggregate functions (used with GROUP BY clauses)
  deriving
    ( -- | TODO: RenameOp
      Eq,
      Show
    )

-- Datatype for FROM clauses in SQL
-- We can either select from a named table or from a subquery
data FromExp
  = TableName Name (Maybe JoinExp)
  | SubQuery Query (Maybe JoinExp)
  deriving (Eq, Show)

-- Datatype for WHERE clauses in SQL
data WhereExp
  = OpC CompOp Comparable Comparable -- Comparison operations
  | OpA ArithOp Comparable Comparable -- Arithmetic Operations
  | OpL LogicOp Bool Bool -- Logical operations
  | OpN NullOp Name -- NULL/IS NULL
  deriving (Eq, Show)

data Value
  = NilVal -- nil
  | IntVal Int -- 1
  | BoolVal Bool -- false, true
  | StringVal String -- "abd"
  | TableVal Name -- <not used in source programs>
  deriving (Eq, Show, Ord)

-- Datatype for JOIN clauses in SQL (only equality joins supported)
-- table = the other table you're joining on
-- condition, where each tuple represents (table name, column name)
-- ((A, name), (B, id)) == A.name = B.id
data JoinExp = Join
  { table :: Name,
    condition :: ((Name, Name), (Name, Name)),
    style :: JoinStyle
  }
  deriving (Eq, Show)

-- The manner in which the join should be performed in SQL
data JoinStyle = LeftJoin | RightJoin | InnerJoin
  deriving (Eq, Show)

-- Type representing values that can be compared in SQL queries
data Comparable
  = ColName Name -- column name
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

-- QUESTION: Why don't Enum or Bounded work?
-- Logical (binary) operations
data LogicOp
  = And Bool Bool
  | Or Bool Bool
  deriving (Eq, Show)

type TableName = String

type ColName = String

-- TODO: come back to this
-- `As` operator in SQL
data RenameOp
  = AsCol ColName
  | AsTable TableName
  | AsAgg ColName ColName

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
