module Types.TableTypes where

import Control.Monad
import Data.Array
import Data.Char
import Data.Csv
import Data.Foldable (toList)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
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
  = IntVal Int -- 1
  | StringVal String -- "abd"
  | DoubleVal Double -- 64-bit
  deriving (Eq, Show, Ord)

-- Tables are represented as Arrays, indexed by their (row, col)
-- Each cell contains a Maybe Value (we allow for null entries)
-- (Recommended by Joe)
type Table = Array (Int, Int) (Maybe Value)

-- Each schema is a map from ColName to a ColType
-- (Schemas are internally ordered by lexicographic order of the colname
-- since ColName is the type of the key in this Map)
type Schema = Map ColName ColType

-- Permitted types for columns
data ColType = IntC | StringC | DoubleC
  deriving (Show, Eq, Ord)

-- Data type representing a column
newtype Column = Column [Maybe Value]
  deriving (Show, Eq)

type Row = [Maybe Value]

-- Allows a single column in a named table to be referenced (eg. "t.col")
type Reference = (TableName, ColName)

-- | Maps a column name to its column index (given a specified schema)
-- Returns -1 if the colname is not in the schema
getColIndex :: ColName -> Schema -> Int
getColIndex colName schema =
  case Map.lookupIndex colName schema of
    Just i -> i
    Nothing -> -1

--------------------------------------------------------------------------------
-- CONVENIENCE FUNCTIONS FOR TABLE CONSTRUCTION

-- | Helper function for creating a table
mkTable :: (Int, Int) -> [Maybe Value] -> Table
mkTable = curry listArray (0, 0)

-- | Equivalent of "map" for pairs consisting of the same type
-- mapPair :: (a -> b) -> (a, a) -> (b, b)
-- mapPair f (a1, a2) = (f a1, f a2)

-- | Retrieves a (non-empty) table's dimensions in the form (numRows, numCols)
-- Need to add one since tables are zero-indexed
dimensions :: Table -> (Int, Int)
dimensions table =
  let (numRows, numCols) = (snd . bounds) table
   in (numRows + 1, numCols + 1)

-- | Converts a (non-empty) Table to a human-readable 2D list
-- (for exporting to a CSV later)
tableToList :: Table -> [[Maybe Value]]
tableToList table =
  let (numRows, numCols) = dimensions table
   in [[table ! (i, j) | j <- [0 .. numCols - 1]] | i <- [0 .. numRows - 1]]

--------------------------------------------------------------------------------
-- CONVENIENCE FUNCTIONS FOR SCHEMA CONSTRUCTION

-- | Makes a table schema given a list of colnames and coltypes
-- POTENTIAL QC PROPERTY: check that colnames in schema are unique
mkSchema :: [(ColName, ColType)] -> Map ColName ColType
mkSchema = Map.fromList

-- | Inverts the keys & values of a (one-to-one) Map
invertMap :: Ord v => Map k v -> Map v k
invertMap = Map.fromList . map (\(k, v) -> (v, k)) . Map.toList

---------------------------------------------------------------------------------
-- Take a schema and returns a Map from each column name to its column index
-- POTENTIAL QC PROPERTY: check that this function is injective (?)
-- i.e. unique colnames map to unique (non-negative) indexes
-- getColNameIndex :: Schema -> Map ColName ColIndex
-- getColNameIndex schema =
--   let colnames = map fst schema
--    in undefined
