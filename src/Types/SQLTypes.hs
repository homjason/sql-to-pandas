-- File to declare types

module Types.SQLTypes where

import Control.Applicative ((<|>))
import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.String (IsString (..))
import Test.QuickCheck
import Types.TableTypes
import Types.Types

-- Datatype representing a SQL query
data Query = Query
  { select :: SelectExp,
    from :: FromExp,
    wher :: Maybe [BoolExp],
    groupBy :: Maybe [ColName],
    limit :: Maybe Int,
    orderBy :: Maybe (ColName, Order)
  }
  deriving (Eq, Show)

-- Datatype for SELECT clauses in SQL
data SelectExp
  = Cols [ColName] -- colnames are a list of string names
  | DistinctCols [ColName] -- SELECT DISTINCT in SQL
  | Agg AggFunc ColName ColName -- Aggregate functions (used with GROUP BY clauses)
  | EmptySelect -- initial state of SelectExp prior to parsing
  deriving
    (Eq, Show)

-- Datatype for FROM clauses in SQL
-- We can either select from a named table or from a subquery
data FromExp
  = TableName TableName (Maybe JoinExp)
  | SubQuery Query (Maybe JoinExp)
  | EmptyFrom -- initial state of FromExp prior to parsing
  deriving (Eq, Show)

-- Datatype for JOIN clauses in SQL (only equality joins supported)
-- table = the other table you're joining on
-- condition, where each tuple represents (table name, column name)
-- ((A, name), (B, id)) == A.name = B.id
data JoinExp = Join
  { table :: TableName,
    condition :: ((TableName, ColName), (TableName, ColName)),
    style :: JoinStyle
  }
  deriving (Eq, Show)

-- Limit the no. of rows in output
newtype LimitExp = Limit Int

-- TODO: come back to this
-- QUESTION FOR JOE: Do we need to use the state monad to map table aliases to the acutal table datatype?
-- `As` operator in SQL
data RenameOp
  = AsCol ColName
  | AsTable TableName
  | AsAgg ColName ColName
