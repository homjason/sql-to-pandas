module Translator where

import Control.Applicative
import Data.Char qualified as Char
import Data.Maybe (fromMaybe, mapMaybe)
import Parser (Parser)
import Parser qualified as P
import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Types.PandasTypes
import Types.SQLTypes
import Types.TableTypes
import Types.Types

-- Wrapper function that takes in a Pandas Query and outputs a Pandas Block
-- TODO: handle SelectExp & add reset_index
translateSQL :: Query -> Command
translateSQL q@(Query s f w gb ob l) =
  let cols = getColsFromSelectTranslation (selectExpToCols s)
   in let dfName = getTableName $ translateFromExp f
       in let fns = getFuncs q
           in Command
                { df = dfName,
                  cols = Just cols,
                  fn = Just fns
                }

-- Given a SQL query, extracts a list of (equivalent) Pandas functions
-- TODO: Add reset_index
getFuncs :: Query -> [Func]
getFuncs q@(Query s f w gb ob l) =
  getJoinFunc (translateFromExp f) ++ whereExpToLoc w
    ++ groupByToPandasGroupBy gb
    ++ getFnsFromSelectTranslation (selectExpToCols s)
    ++ orderByToSortValues ob
    ++ limitExpToHead l

-- Converts a list of ColExps into list of pairs consisting of colnames & aggregate functions
getColExps :: [ColExp] -> [(ColName, Maybe Func)]
getColExps = map deconstructColExp
  where
    -- Deconstructs a ColExp into its constituent colname & aggregate function
    -- (if it exists)
    deconstructColExp :: ColExp -> (ColName, Maybe Func)
    deconstructColExp (Col col) = (col, Nothing)
    deconstructColExp (Agg f col) = (col, Just $ Aggregate f col)

-- | Extracts a list of colnames from a list of deconstructed ColExps
getColNames :: [(ColName, Maybe Func)] -> [ColName]
getColNames = map fst

-- | Extracts all the aggregate functions from a list of deconstructed ColExps
getAggFuncs :: [(ColName, Maybe Func)] -> [Func]
getAggFuncs = mapMaybe snd

-- Old implementation
-- getColNames cExps = [name | x@(Col name) <- cExps]

-- >>> getColNames [Col "col1", Col "col2", Agg Count "col1"]
-- ["col1","col2"]

-- Converts "SELECT" expressions in SQL to a list of colnames in Pandas
-- NB: we either have AggFuncs or Distinct
selectExpToCols :: SelectExp -> ([ColName], Maybe [Func])
selectExpToCols Star = ([], Nothing)
selectExpToCols (Cols cs) =
  let cExps = getColExps cs
   in case getAggFuncs cExps of
        [] -> (getColNames cExps, Nothing)
        funcs@(f : fs) -> (getColNames cExps, Just funcs)
selectExpToCols (DistinctCols cols) =
  let cNames = (getColNames . getColExps) cols
   in (cNames, Just [Unique cNames])

getColsFromSelectTranslation :: ([ColName], Maybe [Func]) -> [ColName]
getColsFromSelectTranslation (cNames, mFn) = cNames

getFnsFromSelectTranslation :: ([ColName], Maybe [Func]) -> [Func]
getFnsFromSelectTranslation (cNames, mFn) = Data.Maybe.fromMaybe [] mFn

-- Converts "FROM" expressions in SQL to the table name and translates the
-- JOIN expression too if present

translateFromExp :: FromExp -> (TableName, Maybe Func)
translateFromExp fromExp = case fromExp of
  Table name mJoin -> case mJoin of
    Nothing -> (name, Nothing)
    Just je -> (name, Just $ translateJoinExp je)
  SubQuery query mJoin -> undefined

getJoinFunc :: (TableName, Maybe Func) -> [Func]
getJoinFunc (tName, f) = case f of
  Nothing -> []
  Just fn -> [fn]

getTableName :: (TableName, Maybe Func) -> TableName
getTableName (tName, f) = tName

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

-- Converts "WHERE" expressions in SQL to "loc" function in Pandas
whereExpToLoc :: Maybe WhereExp -> [Func]
whereExpToLoc wExp = case wExp of
  Nothing -> []
  Just we -> [Loc we]

-- Converts "LIMIT" clauses in SQL to "head" function in Pandas
limitExpToHead :: Maybe Int -> [Func]
limitExpToHead x = case x of
  Nothing -> []
  Just n -> [Head n]

-- Converts "ORDER BY" clauses in SQL to "sort" function in Pandas
orderByToSortValues :: Maybe (ColName, Order) -> [Func]
orderByToSortValues colOrd = case colOrd of
  Nothing -> []
  Just (col, o) -> [SortValues col o]

-- Extracts Agg from [ColExp]
getAggs :: [ColExp] -> [ColExp]
getAggs cExps = [x | x@(Agg f cols) <- cExps]

-- Converts a list of columns to be grouped on (in SQL) into
-- a Pandas groupby() function invocation
groupByToPandasGroupBy :: Maybe [ColName] -> [Func]
groupByToPandasGroupBy cols = case cols of
  Nothing -> []
  Just cs -> [GroupBy cs]
