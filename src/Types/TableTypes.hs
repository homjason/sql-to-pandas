{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Types.TableTypes where

import Control.Monad
import Data.Array
import Data.Char
import Data.Csv
  ( DefaultOrdered (headerOrder),
    FromField (parseField),
    FromNamedRecord (parseNamedRecord),
    Header,
    ToField (toField),
    ToNamedRecord (toNamedRecord),
    (.:),
    (.=),
  )
import Data.Csv qualified as Cassava
import Data.Foldable (toList)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.String (IsString (..))
import Data.Text (Text)
import Data.Text.Encoding qualified as Text
import Data.Vector (Vector)
import Data.Vector qualified as Vector
import Types.Types

-- Maps each Table's name to the actual table
-- (akin to the Store for LuStepper in HW5)
type Store = Map TableName Table

-- Tables are represented as Arrays, indexed by their (row, col)
-- Each cell contains a Maybe Value (we allow for null entries)
-- (Recommended by Joe)
type Table = Array (Int, Int) (Maybe Value)

-- Represents a singular value stored in a Table
data Value
  = IntVal Int -- 1
  | StringVal String -- "abd"
  deriving (Eq, Show, Ord)
  
-- | DoubleVal Double -- 64-bit
  

-- Initial table name
initTableName :: TableName
initTableName = "_G"

-- Initial table schema
initSchema :: Schema
initSchema = Map.fromList [("col0", StringC), ("col1", StringC)]

-- Initial table
initTable :: Table
initTable =
  array
    ((0, 0), (1, 1))
    [ ((0, 0), Just (StringVal "(0, 0)")),
      ((0, 1), Just (StringVal "(0, 1)")),
      ((1, 0), Just (StringVal "(1, 0)")),
      ((1, 1), Just (StringVal "(1, 1)"))
    ]

-- Initial store maps the initTable's name to the initial table
initialStore :: Store
initialStore = Map.singleton initTableName initTable

-- Each schema is a map from ColName to a ColType
-- (Schemas are internally ordered by lexicographic order of the colname
-- since ColName is the type of the keys in this Map)
type Schema = Map ColName ColType

-- Permitted types for columns
data ColType = IntC | StringC
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

-- Maps each column name to the index for that column
getColIdxs :: Schema -> Map ColName Int
getColIdxs schema =
  Map.mapWithKey (\colName _ -> colName `getColIndex` schema) schema

-- | Given a schema & a table, maps each colname to the particular column
-- in the table (this function presumes that the table adheres to the schema)
getColOfTable :: Schema -> Table -> Map ColName Column
getColOfTable schema table =
  let (colnames, colToIdx) = (Map.keys schema, getColIdxs schema)
      idxEltPairs = assocs table
      (numRows, numCols) = dimensions table
   in Map.mapWithKey
        ( \colName colIdx ->
            Column [elt | ((row, col), elt) <- idxEltPairs, col == colIdx]
        )
        colToIdx

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
