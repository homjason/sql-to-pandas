module Translator where

import Control.Applicative
import Data.Char qualified as Char
import Data.List
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Set (Set)
import Data.Set qualified as Set
import Parser (Parser)
import Parser qualified as P
import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Types.PandasTypes as Pandas
import Types.SQLTypes as SQL
import Types.TableTypes
import Types.Types

-- Wrapper function that takes in a Pandas Query and outputs a Pandas Block
-- TODO: handle SelectExp & add reset_index
translateSQL :: Query -> Command
translateSQL q@(Query s f w gb ob l) =
  let cols = getColsFromSelectTranslation (selectExpToCols s)
   in let dfName = getTableName $ translateFromExp f
       in let fns = getFuncs q dfName
           in Command
                { df = dfName,
                  cols = Just cols,
                  fn = fns
                }

moveResetIndex :: [Func] -> [Func]
moveResetIndex fns =
  if ResetIndex `elem` fns
    then
      let newFns = delete ResetIndex fns
       in newFns ++ [ResetIndex]
    else fns

-- >>> moveResetIndex [Pandas.GroupBy ["col1"], ResetIndex, Aggregate Count "col2"]
-- [GroupBy ["col1"],Aggregate Count "col2",ResetIndex]

-- | Given a SQL query, extracts a list of (equivalent) Pandas functions
getFuncs :: Query -> TableName -> Maybe [Func]
getFuncs q@(Query s f w gb ob l) df =
  let funcList = getJoinFunc (translateFromExp f) ++ whereExpToLoc w df ++ groupByToPandasGroupBy gb ++ getFnsFromSelectTranslation (selectExpToCols s) ++ orderByToSortValues ob ++ limitExpToHead l
   in case funcList of
        [] -> Nothing
        hd : tl -> Just $ moveResetIndex funcList

-- | Converts a list of ColExps into list of pairs consisting of colnames
-- & aggregate functions
decompColExps :: [ColExp] -> [(ColName, Maybe Func)]
decompColExps = map decompose
  where
    -- Decomposes a ColExp into its constituent colname & aggregate function
    -- (if it exists)
    decompose :: ColExp -> (ColName, Maybe Func)
    decompose (Col col) = (col, Nothing)
    decompose (Agg f col) = (col, Just $ Aggregate f col)

-- | Extracts a list of colnames from a list of deconstructed ColExps
getColNames :: [(ColName, Maybe Func)] -> [ColName]
getColNames = map fst

-- | Extracts a set of non-aggregated columns from a list of deconstructed ColExps
getNonAggCols :: [(ColName, Maybe Func)] -> Set ColName
getNonAggCols cExps = Set.fromList [col | (col, Nothing) <- cExps]

-- Extracts a set of _aggregated_ columns from a list of deconstructed ColExps
getAggCols :: [(ColName, Maybe Func)] -> Set ColName
getAggCols cExps = Set.fromList [col | (col, Just _) <- cExps]

-- | Extracts all the aggregate functions from a list of deconstructed ColExps
getAggFuncs :: [(ColName, Maybe Func)] -> [Func]
getAggFuncs = mapMaybe snd

-- Converts "SELECT" expressions in SQL to a list of colnames in Pandas
-- NB: we either have AggFuncs or Distinct
selectExpToCols :: SelectExp -> ([ColName], Maybe [Func])
selectExpToCols Star = ([], Nothing)
selectExpToCols (Cols cs) =
  let cExps = decompColExps cs
   in case getAggFuncs cExps of
        [] -> (getColNames cExps, Nothing)
        funcs@(f : fs) -> (getColNames cExps, Just funcs)
selectExpToCols (DistinctCols cols) =
  let cNames = (getColNames . decompColExps) cols
   in (cNames, Just [Unique cNames])

getColsFromSelectTranslation :: ([ColName], Maybe [Func]) -> [ColName]
getColsFromSelectTranslation (cNames, mFn) = cNames

getFnsFromSelectTranslation :: ([ColName], Maybe [Func]) -> [Func]
getFnsFromSelectTranslation (cNames, mFn) = Data.Maybe.fromMaybe [] mFn

-- | Converts "FROM" expressions in SQL to the table name and translates the
-- JOIN expression too if present
translateFromExp :: FromExp -> (TableName, Maybe Func)
translateFromExp fromExp =
  case fromExp of
    Table name -> (name, Nothing)
    TableJoin joinExp -> (leftTable joinExp, Just $ translateJoinExp joinExp)

getJoinFunc :: (TableName, Maybe Func) -> [Func]
getJoinFunc (tName, f) = case f of
  Nothing -> []
  Just fn -> [fn]

getTableName :: (TableName, Maybe Func) -> TableName
getTableName (tName, f) = tName

-- | Converts "JOIN ON" expressions in SQL to Pandas' Merge function
translateJoinExp :: JoinExp -> Func
translateJoinExp je@(Join leftTable leftCol rightTable rightCol style) =
  Merge $
    MkMerge
      { rightDf = rightTable,
        leftOn = leftCol,
        rightOn = rightCol,
        how = style
      }

sqlToPandasUop :: SQL.Uop -> Pandas.Uop
sqlToPandasUop uop = case uop of
  SQL.IsNull -> Pandas.IsNull
  SQL.IsNotNull -> Pandas.IsNotNull

sqlToPandasBop :: SQL.Bop -> Pandas.Bop
sqlToPandasBop bop = case bop of
  SQL.Comp co -> case co of
    SQL.Eq -> Pandas.Comp Pandas.Eq
    SQL.Neq -> Pandas.Comp Pandas.Neq
    SQL.Gt -> Pandas.Comp Pandas.Gt
    SQL.Ge -> Pandas.Comp Pandas.Ge
    SQL.Lt -> Pandas.Comp Pandas.Lt
    SQL.Le -> Pandas.Comp Pandas.Le
  SQL.Arith ao -> Pandas.Arith ao
  SQL.Logic lo -> case lo of
    SQL.And -> Pandas.Logic Pandas.And
    SQL.Or -> Pandas.Logic Pandas.Or

sqlToPandasCompVal :: SQL.Comparable -> TableName -> Pandas.Comparable
sqlToPandasCompVal comp df = case comp of
  SQL.ColName s -> Pandas.ColName s df
  SQL.LitInt n -> Pandas.LitInt n
  SQL.LitString s -> Pandas.LitString s
  SQL.LitDouble x -> Pandas.LitDouble x

whereExpToBoolExp :: WhereExp -> TableName -> BoolExp
whereExpToBoolExp we df = case we of
  SQL.Op1 we' uop -> Pandas.Op1 (whereExpToBoolExp we' df) (sqlToPandasUop uop)
  SQL.Op2 we1 bop we2 -> Pandas.Op2 (whereExpToBoolExp we1 df) (sqlToPandasBop bop) (whereExpToBoolExp we2 df)
  SQL.CompVal com -> Pandas.CompVal (sqlToPandasCompVal com df)

-- | Converts "WHERE" expressions in SQL to "loc" function in Pandas
whereExpToLoc :: Maybe WhereExp -> TableName -> [Func]
whereExpToLoc wExp df = case wExp of
  Nothing -> []
  Just we -> [Loc (whereExpToBoolExp we df)]

-- | Converts "LIMIT" clauses in SQL to "head" function in Pandas
limitExpToHead :: Maybe Int -> [Func]
limitExpToHead x = case x of
  Nothing -> []
  Just n -> [Head n]

-- | Converts "ORDER BY" clauses in SQL to "sort" function in Pandas
orderByToSortValues :: Maybe (ColName, Order) -> [Func]
orderByToSortValues colOrd = case colOrd of
  Nothing -> []
  Just (col, o) -> [SortValues col o]

-- | Extracts Agg from [ColExp]
getAggs :: [ColExp] -> [ColExp]
getAggs cExps = [x | x@(Agg f cols) <- cExps]

-- | Converts a list of columns to be grouped on (in SQL) into
-- a Pandas groupby() function invocation
groupByToPandasGroupBy :: Maybe [ColName] -> [Func]
groupByToPandasGroupBy cols = case cols of
  Nothing -> []
  Just cs -> [Group cs, ResetIndex]
