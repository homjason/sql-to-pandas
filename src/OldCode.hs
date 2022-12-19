module OldCode where

import Control.Applicative
import Control.Monad
import Control.Monad.Error.Class (throwError)
import Data.Char (isSpace, toLower)
import Data.Either (rights)
import Data.Functor (($>), (<&>))
import Data.List (dropWhileEnd, foldl')
import Data.List.Split (splitOn)
import Data.Set (Set, disjoint, isSubsetOf)
import Data.Set qualified as Set
import Parser (Parser)
import Parser qualified as P
import Print
import Translator (decompColExps, getAggCols, getAggFuncs, getColNames, getNonAggCols, translateSQL)
import Types.PandasTypes as Pandas
import Types.SQLTypes as SQL

parseQuery :: String -> Either P.ParseError Query
parseQuery str =
  case splitQueryString str of
    [] -> Left "No parses"
    [select, from] -> do
      s <- parseSelectExp select
      f <- parseFromExp from
      Right $ Query s f Nothing Nothing Nothing Nothing
    [select, from, c] -> do
      s <- parseSelectExp select
      f <- parseFromExp from
      case inferCondition c of
        Left errorMsg -> Left errorMsg
        Right c' -> Right $ mkQuery s f c'
    [select, from, c1, c2] -> do
      s <- parseSelectExp select
      f <- parseFromExp from
      case (inferCondition c1, inferCondition c2) of
        (Right w@(Wher _), Right gb@(SQL.GroupBy _)) -> mkQuery2 s f w gb
        (Right w@(Wher _), Right ob@(OrderBy _)) -> mkQuery2 s f w ob
        (Right w@(Wher _), Right l@(Limit _)) -> mkQuery2 s f w l
        (Right gb@(SQL.GroupBy _), Right ob@(OrderBy _)) -> mkQuery2 s f gb ob
        (Right gb@(SQL.GroupBy _), Right l@(Limit _)) -> mkQuery2 s f gb l
        (Right ob@(OrderBy _), Right l@(Limit _)) -> mkQuery2 s f ob l
        (_, _) -> Left "Malformed SQL query"
    [select, from, c1, c2, c3] -> do
      s <- parseSelectExp select
      f <- parseFromExp from
      case (inferCondition c1, inferCondition c2, inferCondition c3) of
        (Right w@(Wher _), Right gb@(SQL.GroupBy _), Right ob@(OrderBy _)) ->
          mkQuery3 s f w gb ob
        (Right w@(Wher _), Right ob@(OrderBy _), Right l@(Limit _)) ->
          mkQuery3 s f w ob l
        (Right gb@(SQL.GroupBy _), Right ob@(OrderBy _), Right l@(Limit _)) ->
          mkQuery3 s f gb ob l
        (_, _, _) -> Left "Malformed SQL query"
    [select, from, wher, groupBy, orderBy, limit] -> do
      s <- parseSelectExp select
      f <- parseFromExp from
      w <- parseWhereExp wher
      gb <- parseGroupByExp groupBy
      ob <- parseOrderByExp orderBy
      l <- parseLimitExp limit
      Right $ Query s f (Just w) (Just gb) (Just ob) (Just l)
    _ -> Left "Invalid SQL Query"

-- | Infers whether a query condition is a WHERE, a GROUP BY,
-- an ORDER BY or a LIMIT
inferCondition :: String -> Either P.ParseError Condition
inferCondition str =
  case parseWhereExp str of
    Right whereExp -> Right $ Wher whereExp
    Left e1 ->
      case parseGroupByExp str of
        Right groupByCols -> Right $ SQL.GroupBy groupByCols
        Left e2 ->
          case parseOrderByExp str of
            Right orderByExp -> Right $ OrderBy orderByExp
            Left e3 ->
              case parseLimitExp str of
                Right numRows -> Right $ Limit numRows
                Left e4 -> Left "Malformed SQL query"

-- Generator that produces a non-empty Table adhering to an input Schema
-- If an empty Schema is provided, this generator returns the special
-- QuickCheck discard value (QC.discard)
genTable :: Schema -> Gen Table
genTable schema
  | schema == Map.empty = return QC.discard
  | otherwise = do 
    -- Arbitrarily generate the no. of rows
    numRows <- QC.chooseInt (1, 5)

    -- Map each column indexes to the generator for that Column
    let numCols = Map.size schema
        colToIdx = getColIdxs schema
        colToGen = Map.mapWithKey (\_ cType -> genCol numRows cType) schema
        idxToGen = colToGen `Map.compose` invertMap colToIdx

    -- Use sequence to pull the Gen out of the Map, then bind to obtain the
    -- resultant map from indexes to the (randomly generated) columns
    colMap <- sequence idxToGen

    -- Extract the randomly generated columns from their constructor
    let cols = [c | col@(Column c) <- map snd (Map.toList colMap)]
        elts = concat (transpose cols)
        table = listArray ((0, 0), (numRows - 1, numCols - 1)) elts

    -- Create the Table & use return to create a Generator of Tables
    return $ listArray ((0, 0), (numRows - 1, numCols - 1)) elts                


genOp2 :: Gen WhereExp
genOp2 = do
  bop <- genBop
  case bop of
    Arith _ ->
      liftM3 Op2 (CompVal <$> genCompInt) 
        (return bop) (CompVal <$> genCompInt)
    Comp bop' -> 
      liftM3 Op2 (CompVal <$> genComparable) 
        (return bop) (CompVal <$> genComparable)
    Logic bop' -> liftM3 Op2 genCompOpExp (return bop) genCompOpExp

