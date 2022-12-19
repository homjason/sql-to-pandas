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

-- Arithmetic (binary) operations
data ArithOp
  = Plus -- `+`  :: Comparable -> Comparable -> Comparable
  | Minus -- `-`  :: Comparable -> Comparable -> Comparable
  | Times -- `*`  :: Comparable -> Comparable -> Comparable
  | Divide -- `/` :: Comparable -> Comparable -> Comparable   -- floor division
  | Modulo -- `%`  :: Comparable -> Comparable -> Comparable   -- modulo
  deriving (Eq, Show, Enum, Bounded)

-- -- Logical (binary) operations
-- data LogicOp
--   = And
--   | Or
--   deriving (Eq, Show, Enum, Bounded)

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