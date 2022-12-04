module Types.TableTypes where

import Data.Array (Array, (!), (//))
import Data.Array qualified as A
import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.Map (Map)
import Data.String (IsString (..))
import Test.QuickCheck
import Types.Types

-- QUESTION FOR JOE: define tables by row or by column?
-- (both are implemented below)
-- QUESTION FOR JOE: How to define table quality? (can't sort Maps)

-- Maps each Table's name (alias) to the actual table
type Store = Map TableName Table

-- Represents a singular value stored in a Table
data Value
  = NullVal -- null
  | IntVal Int -- 1
  | BoolVal Bool -- false, true
  | StringVal String -- "abd"
  | DoubleVal Double -- 64-bit
  deriving (Eq, Show, Ord)

-- Tables are represented as Arrays, indexed by their (row, col)
-- (Recommended by Joe)
type Table = Array (Int, Int) Value

-- Joe: represent Rows as Maybe Values, enforce row invariants at runtime
type Row = [Maybe Value]

-- Each row is a map from each ColName to a Maybe Value (allowing for null entries)
-- type Row = Map ColName (Maybe Value)

-- Each schema is a map from ColName to a ColType
type Schema = Map ColName ColType

-- Permitted types for columns
data ColType = Int | String | Bool | Double
  deriving (Show, Eq, Enum)

-- Allows a single column in a named table to be referenced (eg. "t.col")
type Reference = (TableName, ColName)
