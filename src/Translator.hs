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
-- initialCommand :: Command
-- initialCommand =
--   Command
--     { df = "",
--       cols = Nothing,
--       fn = Nothing
--     }

-- Extracts ColName from [ColExp]
getColNames :: [ColExp] -> [ColName]
getColNames cExps = [name | x@(Col name) <- cExps]

-- >>> getColNames [Col "col1", Col "col2", Agg Count "col1"]
-- ["col1","col2"]

-- Converts "SELECT" expressions in SQL to a list of colnames in Pandas
selectExpToCols :: SelectExp -> ([ColName], Maybe Func)
selectExpToCols Star = ([], Nothing)
selectExpToCols (Cols cs) = (getColNames cs, Nothing)
selectExpToCols (DistinctCols cs) =
  let cNames = getColNames cs
   in (cNames, Just $ Unique cNames)

-- Converts "FROM" expressions in SQL to the table name and translates the
-- JOIN expression too if present

translateFromExp :: FromExp -> (TableName, Maybe Func)
translateFromExp fromExp = case fromExp of
  Table name mJoin -> case mJoin of
    Nothing -> (name, Nothing)
    Just je -> (name, Just $ translateJoinExp je)
  SubQuery query mJoin -> undefined

-- Converts "JOIN ON" expressions in SQL to Pandas' Merge function
translateJoinExp :: JoinExp -> Func
translateJoinExp je@(Join leftTable leftCol rightTable rightCol style) =
  Merge $
    MkMerge
      { rightDf = rightTable,
        leftOn = leftCol,
        rightOn = rightCol,
        how = style
      }

-- fromExpToTable' :: FromExp -> Command
-- fromExpToTable' fromExp = case fromExp of
--   Table name mJoin -> case mJoin of
--     Nothing ->
--       Command
--         { df = name,
--           cols = Nothing,
--           fn = Nothing
--         }
--     Just je@(Join leftTable leftCol rightTable rightCol style) ->
--       Command
--         { df = name,
--           cols = Nothing,
--           fn =
--             Just
--               [ Merge $
--                   MkMerge
--                     { rightDf = rightTable,
--                       leftOn = leftCol,
--                       rightOn = rightCol,
--                       how = style
--                     }
--               ]
--         }
--   SubQuery query mJoin -> undefined

-- Converts "JOIN ON" expressions in SQL to "merge" function in Pandas
-- joinExpToMerge :: JoinExp -> MergeExp
-- joinExpToMerge = undefined

-- Converts "WHERE" expressions in SQL to "loc" function in Pandas
whereExpToLoc :: WhereExp -> Func
whereExpToLoc = undefined

-- Converts "LIMIT" clauses in SQL to "head" function in Pandas
limitExpToHead :: Int -> Func
limitExpToHead = Head

-- Converts "ORDER BY" clauses in SQL to "sort" function in Pandas
orderByToSortValues :: (ColName, Order) -> Func
orderByToSortValues (col, o) = SortValues col o

-- Converts an aggregation function in SQL to an aggregation function in Pandas
aggToPandasAgg :: SelectExp -> Func
aggToPandasAgg = undefined

-- Converts a list of columns to be grouped on (in SQL) into
-- a Pandas groupby() function invocation
groupByToPandasGroupBy :: [ColName] -> Func
groupByToPandasGroupBy = GroupBy
