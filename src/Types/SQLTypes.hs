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
  | SubQuery Query (Maybe JoinExp)
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

-- TODO: come back to this
-- QUESTION FOR JOE: Do we need to use the state monad to map table aliases to the acutal table datatype?
-- `As` operator in SQL
data RenameOp
  = AsCol ColName
  | AsTable TableName
  | AsAgg ColName ColName
