module Translator where

import Control.Applicative
import Data.Char qualified as Char
import Parser (Parser)
import Parser qualified as P
import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Types.PandasTypes
import Types.SQLTypes
import Types.TableTypes
import Types.Types

-- Wrapper function that takes in a Pandas Query and outputs a Pandas Block
translateSQL :: Query -> Block
translateSQL = undefined

{-
let gb = groupBy q in
  case gb of
    Nothing -> ()
    Just [ColName] ->
-}

-- Initial Pandas command prior to parsing
initialCommand :: Command
initialCommand =
  Command
    { df = "",
      cols = Nothing,
      fn = Nothing
    }

-- Converts "FROM" expressions in SQL to the table name
fromExpToTable :: FromExp -> TableName
fromExpToTable = undefined

-- Converts "JOIN ON" expressions in SQL to "merge" function in Pandas
joinExpToMerge :: JoinExp -> MergeExp
joinExpToMerge = undefined

-- Converts "SELECT" expressions in SQL to a list of colnames in Pandas
selectExpToCols :: SelectExp -> [ColName]
selectExpToCols = undefined

-- Converts "WHERE" expressions in SQL to "loc" function in Pandas
whereExpToLoc :: BoolExp -> Func
whereExpToLoc = undefined

-- Converts "LIMIT" clauses in SQL to "head" function in Pandas
limitExpToHead :: LimitExp -> Func
limitExpToHead = undefined

-- Converts "ORDER BY" clauses in SQL to "sort" function in Pandas
orderByToSortValues :: (ColName, Order) -> Func
orderByToSortValues = undefined

-- Converts an aggregation function in SQL to an aggregation function in Pandas
aggToPandasAgg :: SelectExp -> Func
aggToPandasAgg = undefined

-- Converts a list of columns to be grouped on (in SQL) into
-- a Pandas groupby() function invocation
groupByToPandasGroupBy :: [ColName] -> Func
groupByToPandasGroupBy = undefined

-- Converts the "DISTINCT" keyword in SQL to the "unique" function in Pandas
distinctToUnique :: SelectExp -> Func
distinctToUnique = undefined
