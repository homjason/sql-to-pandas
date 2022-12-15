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
import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Translator (decompColExps, getAggCols, getAggFuncs, getColNames, getNonAggCols, translateSQL)
import Types.PandasTypes as Pandas
import Types.SQLTypes as SQL
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

-- Parses literal values (ints, booleans, strings, doubles)
-- valueP :: Parser Value
-- valueP = intValP <|> boolValP <|> stringValP <|> doubleValP

-- Parses positive / negative integer literals
litIntP :: Parser Comparable
litIntP = LitInt <$> wsP P.int

boolValP :: Parser Value
boolValP = BoolVal <$> (constP "true" True <|> constP "false" False)

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

-- | Parses the names of aggregate functions and returns a function from ColName -> ColExp
aggFuncTokenP :: Parser (ColName -> ColExp)
aggFuncTokenP =
  constP "count" (Agg Count)
    <|> constP "avg" (Agg Avg)
    <|> constP "sum" (Agg Sum)
    <|> constP "min" (Agg Min)
    <|> constP "max" (Agg Max)

-- | Names of aggregate functions in SQL/Pandas
aggFuncNames :: [String]
aggFuncNames = ["count", "avg", "sum", "min", "max"]

-- | Parses aggregate function names in SQL
aggFuncP :: Parser AggFunc
aggFuncP =
  P.choice
    [ constP "count" Count,
      constP "avg" Avg,
      constP "sum" Sum,
      constP "min" Min,
      constP "max" Max
    ]

-- | Strips leading/trailing whitespace from a string
stripSpace :: String -> String
stripSpace = dropWhileEnd isSpace . dropWhile isSpace

-- | Parses SELECT expressions
selectExpP :: Parser SelectExp
selectExpP =
  P.choice
    [ selectTokenP *> (Cols <$> P.sepBy1 colExpP (stringP ",")),
      selectDistinctTokenP *> (DistinctCols <$> P.sepBy1 colExpP (stringP ","))
    ]

-- | Parser for column expressions (parses either a colname
-- or an aggregate function applied to a column)
colExpP :: Parser ColExp
colExpP =
  Agg <$> aggFuncP <*> parens colnameP
    <|> Col <$> colnameP

-- | Auxiliary parser that ensures that colnames aren't reserved keywords
-- (also called in groupByExpP)
colnameP :: Parser ColName
colnameP =
  P.setErrorMsg
    (P.filter (`notElem` aggFuncNames) nameP)
    "Colnames must be non-empty and not SQL reserved keywords"

-- Copied over colExpP unit tests to this file for the time being
-- TODO: fix the two failing test cases
test_colExpP :: Test
test_colExpP =
  "parsing individual columns in SELECT expressions"
    ~: TestList
      [ P.parse colExpP "col" ~?= Right (Col "col"),
        P.parse colExpP "col2       " ~?= Right (Col "col2"),
        P.parse colExpP "max(col)" ~?= Right (Agg Max "col"),
        P.parse colExpP "avg(col1)" ~?= Right (Agg Avg "col1"),
        P.parse colExpP "count(col2)" ~?= Right (Agg Count "col2"),
        -- Check that the names of aggregate functions are
        -- reserved keywords and can't be used as colnames
        P.parse colExpP "max"
          ~?= Left selectErrorMsg,
        P.parse colExpP "min"
          ~?= Left selectErrorMsg,
        P.parse colExpP "avg"
          ~?= Left selectErrorMsg,
        P.parse colExpP "sum"
          ~?= Left selectErrorMsg,
        P.parse colExpP "count"
          ~?= Left selectErrorMsg,
        P.parse colExpP "max(max)"
          ~?= Left selectErrorMsg,
        P.parse colExpP "count(count)"
          ~?= Left selectErrorMsg,
        P.parse colExpP "select col1, max(max), col3"
          ~?= Left selectErrorMsg,
        -- No column specified in aggregate function
        P.parse colExpP "sum()"
          ~?= Left selectErrorMsg,
        P.parse colExpP "min("
          ~?= Left selectErrorMsg,
        -- Invalid function calls
        P.parse colExpP "dsgds(col)" ~?= Left "no parses",
        P.parse colExpP "col(col)" ~?= Left "no parses"
      ]
  where
    selectErrorMsg =
      "Colnames must be non-empty and not \
      \SQL reserved keywords"

-- | Parses commands indicating the style of the join
joinTokenP :: Parser JoinStyle
joinTokenP =
  constP "join" InnerJoin
    <|> constP "left join" LeftJoin
    <|> constP "right join" RightJoin

-- | Parser for FROM expressions
-- TODO: handle subqueries
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
  unless (null tl) (Left "Invalid JOIN expression")
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
          ""
        )
    (_, _) -> Left "Malformed JOIN condition"

-- | Parse WHERE expressions (binary/unary operators are left associative)
-- (modified from HW5)
-- We first parse AND/OR operators, then comparison operators,
-- then + & -, then * & /, then (postfix) unary operators
whereExpP :: Parser WhereExp
whereExpP = stringP "where" *> logicP
  where
    logicP = compP `P.chainl1` opAtLevel (level (Logic And))
    compP = sumP `P.chainl1` opAtLevel (level (Comp Gt))
    sumP = prodP `P.chainl1` opAtLevel (level (Arith Plus))
    prodP = uopexpP `P.chainl1` opAtLevel (level (Arith Times))
    uopexpP =
      (Op1 <$> baseP <*> uopP)
        <|> baseP
    baseP =
      CompVal <$> comparableP
        <|> parens logicP

-- | Parse an operator at a specified precedence level (from HW5)
opAtLevel :: Int -> Parser (WhereExp -> WhereExp -> WhereExp)
opAtLevel l = flip Op2 <$> P.filter (\x -> level x == l) bopP

-- | Parses (infix) binary operators
bopP :: Parser Bop
bopP =
  P.between whitespace bop whitespace
  where
    whitespace = many P.space
    bop =
      P.choice
        [ constP "=" (Comp Eq),
          constP "!=" (Comp Neq),
          constP ">=" (Comp Ge),
          constP ">" (Comp Gt),
          constP "<=" (Comp Le),
          constP "<" (Comp Lt),
          constP "+" (Arith Plus),
          constP "-" (Arith Minus),
          constP "*" (Arith Times),
          constP "/" (Arith Divide),
          constP "%" (Arith Modulo),
          constP "and" (Logic And),
          constP "or" (Logic Or)
        ]

-- | Parses (postfix) unary operators
uopP :: Parser Uop
uopP =
  P.choice
    [ constP "is null" IsNull,
      constP "is not null" IsNotNull
    ]

-- | Parses string literals
-- (non-quote characters enclosed in-between escaped double quotes)
litStringP :: Parser String
litStringP = P.between (P.char '\"') (many $ P.satisfy (/= '\"')) (stringP "\"")

-- | Parser for Comparable values
-- TODO: handle LitDouble
comparableP :: Parser Comparable
comparableP =
  P.choice
    [ ColName <$> nameP,
      LitInt <$> P.int,
      LitString <$> litStringP
    ]

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

-- TODO: In the function inferCondition, use alternative instead of nested cases
-- (change return type of ParseGroupByExp, parseOrderByExp etc. to
-- Parser a instead of using Either monad)

-- | Infers whether a query condition is a WHERE, a GROUP BY,
-- an ORDER BY or a LIMIT
-- inferCondition :: String -> Either P.ParseError Condition
-- inferCondition str =
--   case parseWhereExp str of
--     Right whereExp -> Right $ Wher whereExp
--     Left e1 ->
--       case parseGroupByExp str of
--         Right groupByCols -> Right $ SQL.GroupBy groupByCols
--         Left e2 ->
--           case parseOrderByExp str of
--             Right orderByExp -> Right $ OrderBy orderByExp
--             Left e3 ->
--               case parseLimitExp str of
--                 Right numRows -> Right $ Limit numRows
--                 Left e4 -> Left "Malformed SQL query"

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

-- | Wrapper function for all the query-parsing business logic
-- s = SELECT expression
-- f = FROM expression
-- w = WHERE expression
-- gb = GROUP BY expression
-- ob = ORDER BY expression
-- l = LIMIT expression
-- c = Arbitrary SQL query condition (WHERE/GROUP BY/ORDER BY/LIMIT)
-- parseQuery :: String -> Either P.ParseError Query
-- parseQuery str =
--   case splitQueryString str of
--     [] -> Left "No parses"
--     [select, from] -> do
--       s <- parseSelectExp select
--       f <- parseFromExp from
--       Right $ Query s f Nothing Nothing Nothing Nothing
--     [select, from, c] -> do
--       s <- parseSelectExp select
--       f <- parseFromExp from
--       case inferCondition c of
--         Left errorMsg -> Left errorMsg
--         Right c' -> Right $ mkQuery s f c'
--     [select, from, c1, c2] -> do
--       s <- parseSelectExp select
--       f <- parseFromExp from
--       case (inferCondition c1, inferCondition c2) of
--         (Right w@(Wher _), Right gb@(SQL.GroupBy _)) -> mkQuery2 s f w gb
--         (Right w@(Wher _), Right ob@(OrderBy _)) -> mkQuery2 s f w ob
--         (Right w@(Wher _), Right l@(Limit _)) -> mkQuery2 s f w l
--         (Right gb@(SQL.GroupBy _), Right ob@(OrderBy _)) -> mkQuery2 s f gb ob
--         (Right gb@(SQL.GroupBy _), Right l@(Limit _)) -> mkQuery2 s f gb l
--         (Right ob@(OrderBy _), Right l@(Limit _)) -> mkQuery2 s f ob l
--         (_, _) -> Left "Malformed SQL query"
--     [select, from, c1, c2, c3] -> do
--       s <- parseSelectExp select
--       f <- parseFromExp from
--       case (inferCondition c1, inferCondition c2, inferCondition c3) of
--         (Right w@(Wher _), Right gb@(SQL.GroupBy _), Right ob@(OrderBy _)) ->
--           mkQuery3 s f w gb ob
--         (Right w@(Wher _), Right ob@(OrderBy _), Right l@(Limit _)) ->
--           mkQuery3 s f w ob l
--         (Right gb@(SQL.GroupBy _), Right ob@(OrderBy _), Right l@(Limit _)) ->
--           mkQuery3 s f gb ob l
--         (_, _, _) -> Left "Malformed SQL query"
--     [select, from, wher, groupBy, orderBy, limit] -> do
--       s <- parseSelectExp select
--       f <- parseFromExp from
--       w <- parseWhereExp wher
--       gb <- parseGroupByExp groupBy
--       ob <- parseOrderByExp orderBy
--       l <- parseLimitExp limit
--       Right $ Query s f (Just w) (Just gb) (Just ob) (Just l)
--     _ -> Left "Invalid SQL Query"

-- Converts query string to lower case, splits on newlines
-- & strips leading/trailing whitespace
-- NB: each clause in a SQL query (SELECT, FROM, etc.) must be on a new line
splitQueryString :: String -> [String]
splitQueryString = map stripSpace . lines . map toLower

-- Parses a SQL file (takes in filename of the SQL file)
-- Returns either a ParseError (Left) or a Query (Right)
-- TODO: fix this function so that it can call parseQuery above
parseSqlFile :: String -> IO (Either P.ParseError Query)
parseSqlFile = undefined "P.parseFromFile (const <$> parseQuery <*> P.eof)"

-- | Checks if a Query contains valid SQL syntax / is semantically correct
-- (this function will be used in QuickCheck properties as a precondition)
validateQuery :: Query -> Either P.ParseError Bool
validateQuery q@(Query s f w gb ob l) = do
  v1 <- selectExpIsNonEmpty s
  v2 <- groupByColsInSelectExp q
  v3 <- distinctHasNoAggFuncs s
  v4 <- noDistinctAndGroupBy s gb
  Right (v1 && v2 && v3 && v4)

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

-- Parses the string into a query and translates it into a Pandas Command
-- runParseAndTranslate :: String -> Either P.ParseError Command
-- runParseAndTranslate s = case parseQuery s of
--   Left str -> Left str
--   Right q ->
--     case validateQuery q of
--       Left validateError -> Left validateError
--       Right False -> Left "Invalid SQL query"
--       Right True -> Right $ translateSQL q
