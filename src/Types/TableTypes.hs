module Types.TableTypes where

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

-- | Helper function for creating a table
mkTable :: (Int, Int) -> [Maybe Value] -> Table
mkTable = curry listArray (0, 0)

-- | Retrieves a table's dimensions in the form (numRows, numCols)
dimensions :: Table -> (Int, Int)
dimensions = snd . bounds

-- TODO: delete (sample table)
table :: Table
table = mkTable (2, 3) [Just $ IntVal i | i <- [1 .. 9] :: [Int]]

tableList :: [[Maybe Value]]
tableList = tableToList table

tableToList :: Table -> [[Maybe Value]]
tableToList table =
  let (numRows, numCols) = dimensions table
   in [[table ! (i, j) | j <- [0 .. numCols - 1]] | i <- [0 .. numRows - 1]]


-- Joe: represent Rows as Maybe Values, enforce row invariants at runtime
type Row = [Maybe Value]

-- Each row is a map from each ColName to a Maybe Value (allowing for null entries)
-- type Row = Map ColName (Maybe Value)

-- Each schema is a map from ColName to a ColType
-- (Note: schemas are internally ordered by lexicographic order of the colname
-- since ColName is the type of the key in this Map)
type Schema = Map ColName ColType

-- Permitted types for columns
data ColType = IntC | StringC | DoubleC
  deriving (Show, Eq)

-- Data type representing a column
data Column
  = IntCol [Maybe Int]
  | StringCol [Maybe String]
  | DoubleCol [Maybe Double]
  deriving (Show, Eq)

-- Allows a single column in a named table to be referenced (eg. "t.col")
type Reference = (TableName, ColName)

type ColIndex = Int

type RowIndex = Int

-- Take a schema and returns a Map from each column name to its column index
-- POTENTIAL QC PROPERTY: check that this function is bijective (?)
-- i.e. unique colnames map to unique (non-negative) indexes
-- getColNameIndex :: Schema -> Map ColName ColIndex
-- getColNameIndex schema =
--   let colnames = map fst schema
--    in undefined

-- | Maps a column name to its column index (given a specified schema)
-- Returns Nothing if the colname is not in the schema
getColIndex :: ColName -> Schema -> Maybe Int
getColIndex = Map.lookupIndex

----------------------------------------------------------------------------------------------

-- TODO: figure out how to arbitrary generate the type [(ColName, ColType)]

-- | Makes a table schema given a list of colnames and coltypes
-- POTENTIAL QC PROPERTY: check that colnames in schema are unique
mkSchema :: [(ColName, ColType)] -> Map ColName ColType
mkSchema = Map.fromList

-- | Given a schema, creates a table with that schema
schemaToTable :: Schema -> Table
schemaToTable schema = undefined
