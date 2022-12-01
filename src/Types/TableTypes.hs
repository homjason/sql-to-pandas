module Types.TableTypes where

import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.Map (Map)
import Data.String (IsString (..))
import Test.QuickCheck
import Types.Types

-- QUESTION FOR JOE: define tables by row or by column?
-- (both are implemented below)

-- Maps each Table's name (alias) to the actual table
type Store = Map TableName Table

-- A table is a list of Rows
newtype Table = Table [Row]

-- Each row is a map from ColName to Maybe Values
type Row = Map ColName (Maybe Value)

-- Each schema is a map from ColName to a ColType
type Schema = Map ColName ColType

-- Permitted types for columns
data ColType = Int | String | Bool | Double
  deriving (Show, Eq, Enum)

{-
-- Alternate: a Table is a Map from column name to column values
type Table' = Map ColName Col

-- A column in the table is a list of Maybes (allow for null values)
data Col
  = IntCol [Maybe Int]
  | StringCol [Maybe String]
  | BoolCol [Maybe Bool]
  | DoubleCol [Maybe Double]
  deriving (Show, Eq)
-}

-- Allows a single column in a named table to be referenced (eg. "t.col")
type Reference = (TableName, ColName)

data Value
  = NullVal -- null
  | IntVal Int -- 1
  | BoolVal Bool -- false, true
  | StringVal String -- "abd"
  | DoubleVal Double -- 64-bit
  deriving (Eq, Show, Ord)