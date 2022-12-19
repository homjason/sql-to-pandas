{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use lambda-case" #-}

module SQLParser where

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
  ( Bop (..),
    ColExp (..),
    CompOp (Eq, Ge, Gt, Le, Lt, Neq),
    Comparable (ColName, LitInt, LitString),
    Condition (..),
    FromExp (..),
    JoinExp (..),
    LogicOp (And, Or),
    Query (Query),
    SelectExp (..),
    Uop (..),
    WhereExp (..),
    level,
  )
import Types.TableTypes
import Types.Types

-- Use a parser for a particular string
-- (Modification of P.parser in Parser.hs that returns
-- both the parsed string & the remainder of the string)
parseResult :: Parser a -> String -> Either P.ParseError (a, String)
parseResult = P.doParse

-- | Takes a parser, runs it, & skips over any whitespace characters occurring
-- afterwards (from HW5)
wsP :: Parser a -> Parser a
wsP p = p <* many P.space

-- | Accepts only a particular string s & consumes any white space that follows
-- (from HW5)
stringP :: String -> Parser ()
stringP str = wsP (P.string str) $> ()

-- | Accepts a particular string s, returns a given value x,
-- and consume any white space that follows (from HW5)
constP :: String -> a -> Parser a
constP s x = stringP s $> x

-- | Parses between parentheses (from HW5)
parens :: Parser a -> Parser a
parens x = P.between (stringP "(") x (stringP ")")

-- Parses periods
dotP :: Parser Char
dotP = P.satisfy (== '.')

-- | Parses tablenames / colnames (any sequence of upper & lowercase letters, digits,
-- underscores & periods, not beginning with a digit and not being a reserved word)
-- (modified from HW5)
nameP :: Parser ColName
nameP =
  P.filter
    (`notElem` reserved)
    (wsP $ liftA2 (:) startChar $ many remainingChars)
  where
    startChar = P.filter (\c -> c /= '(' && c /= ')') (P.alpha <|> P.char '_')
    remainingChars = startChar <|> P.digit <|> dotP

-- Reserved keywords in SQL
reserved :: [String]
reserved =
  [ "select",
    "from",
    "where",
    "group",
    "by",
    "group_by",
    "true",
    "false",
    "case",
    "if",
    "in",
    "not",
    "and",
    "or",
    "limit",
    "order",
    "order_by",
    "sum",
    "avg",
    "count",
    "min",
    "max",
    "join",
    "on",
    "distinct",
    "minus",
    "as",
    "union",
    "intersect"
  ]

strToDouble :: String -> Double
strToDouble = read :: String -> Double

numberP :: Parser String
numberP =
  P.choice
    [ some P.digit,
      P.char '+' *> some P.digit,
      (:) <$> P.char '-' <*> some P.digit
    ]

number :: Parser String
number = some P.digit

-- | Parses the SELECT token & consumes subsequent whitespace
selectTokenP :: Parser ()
selectTokenP = stringP "select"

-- | Parses the string "SELECT DISTINCT" & consumes subsequent whitespace
selectDistinctTokenP :: Parser ()
selectDistinctTokenP = stringP "select distinct"

-- | Names of aggregate functions in SQL/Pandas
aggFuncNames :: [String]
aggFuncNames = ["count", "avg", "sum", "min", "max"]

-- | Parses SELECT expressions
selectExpP :: Parser SelectExp
selectExpP =
  P.choice
    [ selectTokenP *> (Cols <$> P.sepBy1 colExpP (stringP ",")),
      selectDistinctTokenP *> (DistinctCols <$> P.sepBy1 colExpP (stringP ",")),
      selectTokenP *> stringP "*" $> Star
    ]

-- | Parser for column expressions (parses either a colname
-- or an aggregate function applied to a column)
colExpP :: Parser ColExp
colExpP =
  Agg <$> aggFuncP <*> parens colnameP
    <|> Col <$> colnameP
  where
    -- Parses aggregate function names in SQL
    aggFuncP :: Parser AggFunc
    aggFuncP =
      P.choice
        [ constP "count" Count,
          constP "avg" Avg,
          constP "sum" Sum,
          constP "min" Min,
          constP "max" Max
        ]

-- | Auxiliary parser that ensures that colnames aren't reserved keywords
-- (also called in groupByExpP)
colnameP :: Parser ColName
colnameP =
  P.setErrorMsg
    (P.filter (`notElem` aggFuncNames) nameP)
    "Colnames must be non-empty and not SQL reserved keywords"

-- | Parses commands indicating the style of the join
joinTokenP :: Parser JoinStyle
joinTokenP =
  constP "join" InnerJoin
    <|> constP "left join" LeftJoin
    <|> constP "right join" RightJoin

-- | Parser for FROM expressions
fromExpP :: Parser FromExp
fromExpP =
  P.choice
    [ stringP "from" *> (TableJoin <$> joinExpP),
      stringP "from" *> (Table <$> nameP)
    ]

-- | Parses JOIN expressions
-- (resolves the two tables & columns partaking in the join)
-- For simplicity, we disallow joining the same table with itself
joinExpP :: Parser JoinExp
joinExpP = P.mkParser $ \str -> do
  (leftT, s') <- parseResult nameP str
  (joinStyle, s'') <- parseResult joinTokenP s'
  (rightT, joinCond) <- parseResult nameP s''
  when (null joinCond) (Left "No join condition specified")
  (leftTC, remainder) <- parseResult (stringP "on" *> nameP) joinCond
  (rightTC, tl) <- parseResult (stringP "=" *> nameP) remainder
  case (splitOn "." leftTC, splitOn "." rightTC) of
    ([leftT', leftC], [rightT', rightC]) -> do
      when
        (leftT == rightT || leftT' == rightT')
        (Left "Can't join the same table with itself")
      unless
        (leftT == leftT' && rightT == rightT')
        (Left "Tables being JOINed != tables being selected FROM")
      Right
        ( Join
            { leftTable = leftT,
              leftCol = leftC,
              rightTable = rightT,
              rightCol = rightC,
              style = joinStyle
            },
          tl
        )
    (_, _) -> Left "Malformed JOIN condition"

-- | Parse WHERE expressions (binary/unary operators are left associative)
-- (modified from HW5)
-- We first parse AND/OR operators, then comparison operators,
-- then + & -, then * & /, then (postfix) unary operators
whereExpP :: Parser WhereExp
whereExpP = stringP "where" *> logicP
  where
    logicP = compP `P.chainl1` opAtLevel (level (SQL.Logic SQL.And))
    compP = sumP `P.chainl1` opAtLevel (level (SQL.Comp SQL.Gt))
    sumP = prodP `P.chainl1` opAtLevel (level (SQL.Arith Plus))
    prodP = uopexpP `P.chainl1` opAtLevel (level (SQL.Arith Times))
    uopexpP =
      (SQL.Op1 <$> baseP <*> uopP)
        <|> baseP
    baseP =
      SQL.CompVal <$> comparableP
        <|> parens logicP

-- | Parse an operator at a specified precedence level (from HW5)
opAtLevel :: Int -> Parser (WhereExp -> WhereExp -> WhereExp)
opAtLevel l = flip SQL.Op2 <$> P.filter (\x -> level x == l) bopP

-- | Parses (infix) binary operators
bopP :: Parser SQL.Bop
bopP =
  P.between whitespace bop whitespace
  where
    whitespace = many P.space
    bop =
      P.choice
        [ constP "=" (SQL.Comp SQL.Eq),
          constP "!=" (SQL.Comp SQL.Neq),
          constP ">=" (SQL.Comp SQL.Ge),
          constP ">" (SQL.Comp SQL.Gt),
          constP "<=" (SQL.Comp SQL.Le),
          constP "<" (SQL.Comp SQL.Lt),
          constP "+" (SQL.Arith Plus),
          constP "-" (SQL.Arith Minus),
          constP "*" (SQL.Arith Times),
          constP "/" (SQL.Arith Divide),
          constP "%" (SQL.Arith Modulo),
          constP "and" (SQL.Logic SQL.And),
          constP "or" (SQL.Logic SQL.Or)
        ]

-- | Parses (postfix) unary operators
uopP :: Parser SQL.Uop
uopP =
  P.choice
    [ constP "is null" SQL.IsNull,
      constP "is not null" SQL.IsNotNull
    ]

-- | Parser for Comparable values
comparableP :: Parser SQL.Comparable
comparableP =
  P.choice
    [ SQL.ColName <$> nameP,
      SQL.LitInt <$> wsP P.int,
      SQL.LitString <$> litStringP
    ]
  where
    -- Parses string literals
    -- (non-quote characters enclosed in-between escaped double quotes)
    litStringP :: Parser String
    litStringP = P.between (P.char '\"') (many $ P.satisfy (/= '\"')) (stringP "\"")

-- | Parse GROUP BY expressions
groupByP :: Parser [ColName]
groupByP = stringP "group by" *> P.sepBy1 colnameP (stringP ",")

-- | Parse ORDER BY expressions
-- We stipulate that the query must specify the sort order (ASC or DESC)
orderByP :: Parser (ColName, Order)
orderByP =
  stringP "order by"
    *> P.setErrorMsg
      (liftA2 (,) nameP orderP)
      "Invalid ORDER BY expression"
  where
    orderP :: Parser Order
    orderP = constP "asc" Asc <|> constP "desc" Desc

-- Parser for limit expressions (consumes subsequent whitespace)
limitP :: Parser Int
limitP = stringP "limit" *> P.int

-- Parses the "Limit" token & the subsequent integer
parseLimitExp :: String -> Either P.ParseError Int
parseLimitExp = P.parse limitP

-- | Overall parser for a SQL query
queryP :: Parser Query
queryP =
  Query <$> selectExpP
    <*> fromExpP
    <*> ((whereExpP <&> Just) <|> pure Nothing)
    <*> ((groupByP <&> Just) <|> pure Nothing)
    <*> ((orderByP <&> Just) <|> pure Nothing)
    <*> ((limitP <&> Just) <|> pure Nothing)

-- | Wrapper function for parsing a string as a SQL query
parseQuery :: String -> Either P.ParseError Query
parseQuery str = P.parse queryP (map toLower str)

-- | Checks if a Query contains valid SQL syntax / is semantically correct
-- (this function will be used in QuickCheck properties as a precondition)
validateQuery :: Query -> Either P.ParseError Bool
validateQuery q@(Query s f w gb ob l) = do
  v1 <- selectExpIsNonEmpty s
  v2 <- distinctHasNoAggFuncs s
  v3 <- noDistinctAndGroupBy s gb
  Right (v1 && v2 && v3)

-- Check that we don't have any SELECT / SELECT DISTINCT expressions
-- without any column names specified
selectExpIsNonEmpty :: SelectExp -> Either P.ParseError Bool
selectExpIsNonEmpty (Cols []) = Left "No columns specified in SELECT expression"
selectExpIsNonEmpty (DistinctCols []) =
  Left "No columns specified in SELECT DISTINCT expression"
selectExpIsNonEmpty _ = Right True

-- | Checks if columns in GROUP BY expressions are in SELECT expressions
groupByColsInSelectExp :: Query -> Either P.ParseError Bool
groupByColsInSelectExp (Query s _ _ g _ _) =
  case (s, g) of
    -- GROUP BY contains no columns, all grouped cols are vacuously in SelectExp
    (_, Nothing) -> Right True
    (_, Just []) ->
      Left "Columns not specified in GROUP BY"
    (Star, Just (_ : _)) ->
      Left "SELECT * not allowed in queries involving GROUP BY"
    (DistinctCols _, Just (_ : _)) ->
      Left "Can't have SELECT DISTINCT & GROUP BY in the same query"
    -- Check if columns in SELECT expression == columns in GROUP BY
    (Cols cExps, Just groupByCols@(_ : _)) ->
      let decompedExps = decompColExps cExps
       in if groupByColsAreNotAgg decompedExps groupByCols
            && groupByColsAggColsDisjoint decompedExps groupByCols
            && aggColsSubsetOfAllSelectedCols decompedExps groupByCols
            then Right True
            else Left "Columns in SELECT expression /= columns in GROUP BY"

-- | Takes in a list of deconstructed ColExps (columns in SELECT expression) &
-- a list of (GROUP BY) colnames,
-- and checks that the GROUP BY cols are not aggregated
groupByColsAreNotAgg :: [(ColName, Maybe Func)] -> [ColName] -> Bool
groupByColsAreNotAgg decompedExps groupByCols =
  let nonAggCols = getNonAggCols decompedExps
   in nonAggCols == Set.fromList groupByCols

-- Checks that the GROUP BY & Aggregated cols in SELECT statement are disjoint
groupByColsAggColsDisjoint :: [(ColName, Maybe Func)] -> [ColName] -> Bool
groupByColsAggColsDisjoint decompedExps groupByCols =
  let aggCols = getAggCols decompedExps
   in aggCols == Set.fromList groupByCols

-- Checks that the GROUP BY cols are a subset of all cols in SELECT statement
aggColsSubsetOfAllSelectedCols :: [(ColName, Maybe Func)] -> [ColName] -> Bool
aggColsSubsetOfAllSelectedCols decompedExps groupByCols =
  let (aggCols, allCols) =
        ( getAggCols decompedExps,
          (Set.fromList . getColNames) decompedExps
        )
   in aggCols `isSubsetOf` allCols

-- Check that there are no aggregate functions when the DISTINCT keyword is used
distinctHasNoAggFuncs :: SelectExp -> Either P.ParseError Bool
distinctHasNoAggFuncs (DistinctCols []) = Right True
distinctHasNoAggFuncs (DistinctCols (c : cs)) =
  case c of
    Col _ -> distinctHasNoAggFuncs (DistinctCols cs)
    Agg _ _ -> Left "Can't have aggregate functions in SELECT DISTINCT expression"
distinctHasNoAggFuncs _ = Right True

-- Check that we can't have DISTINCT & GROUP BYs together in the same query
noDistinctAndGroupBy :: SelectExp -> Maybe [ColName] -> Either P.ParseError Bool
noDistinctAndGroupBy select@(DistinctCols _) groupBy@(Just _) =
  Left "Can't have DISTINCT & GROUP BY in the same query"
noDistinctAndGroupBy _ (Just _) = Right True
noDistinctAndGroupBy _ Nothing = Right True

-------------------------------------------------------------------------------
-- Convenience functions for constructing Query records

-- | Construct a Query based on a SelectExp, FromExp & some condition
mkQuery :: SelectExp -> FromExp -> Condition -> Query
mkQuery s f condition =
  case condition of
    Wher w -> Query s f (Just w) Nothing Nothing Nothing
    SQL.GroupBy gb -> Query s f Nothing (Just gb) Nothing Nothing
    OrderBy ob -> Query s f Nothing Nothing (Just ob) Nothing
    Limit l -> Query s f Nothing Nothing Nothing (Just l)

-- | Construct a Query based on a SelectExp, FromExp & two conditions
-- Returns a ParseError if the two conditions are invalid / in the wrong order
mkQuery2 :: SelectExp -> FromExp -> Condition -> Condition -> Either P.ParseError Query
mkQuery2 s f c1 c2 =
  case (c1, c2) of
    (Wher w, SQL.GroupBy gb) -> Right $ Query s f (Just w) (Just gb) Nothing Nothing
    (Wher w, OrderBy ob) -> Right $ Query s f (Just w) Nothing (Just ob) Nothing
    (Wher w, Limit l) -> Right $ Query s f (Just w) Nothing Nothing (Just l)
    (SQL.GroupBy gb, OrderBy ob) -> Right $ Query s f Nothing (Just gb) (Just ob) Nothing
    (SQL.GroupBy gb, Limit l) -> Right $ Query s f Nothing (Just gb) Nothing (Just l)
    (OrderBy o, Limit l) -> Right $ Query s f Nothing Nothing (Just o) (Just l)
    (_, _) -> Left "Malformed SQL query, couldn't infer query conditions"

-- | Construct a Query based on a SelectExp, FromExp & three conditions
-- Returns a ParseError if the two conditions are invalid / in the wrong order
mkQuery3 ::
  SelectExp ->
  FromExp ->
  Condition ->
  Condition ->
  Condition ->
  Either P.ParseError Query
mkQuery3 s f c1 c2 c3 =
  case (c1, c2, c3) of
    (Wher w, SQL.GroupBy gb, OrderBy ob) ->
      Right $ Query s f (Just w) (Just gb) (Just ob) Nothing
    (Wher w, OrderBy ob, Limit l) ->
      Right $ Query s f (Just w) Nothing (Just ob) (Just l)
    (SQL.GroupBy gb, OrderBy ob, Limit l) ->
      Right $ Query s f Nothing (Just gb) (Just ob) (Just l)
    (_, _, _) -> Left "Malformed SQL query, couldn't infer query conditions"
