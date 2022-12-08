module SQLParser where

import Control.Applicative
import Data.Char (isSpace, toLower)
import Data.Either (rights)
import Data.Functor (($>))
import Data.List (dropWhileEnd, foldl')
import Data.List.Split (splitOn)
import Parser (Parser)
import Parser qualified as P
import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Types.SQLTypes
-- import Types.SQLTypes (ColExp (..), FromExp, JoinExp, LimitExp (Limit), Query, RenameOp, SelectExp (..))
import Types.TableTypes
import Types.Types

-- | Takes a parser, runs it, & skips over any whitespace characters occurring
-- afterwards
wsP :: Parser a -> Parser a
wsP p = p <* many P.space

-- QUESTION FOR JOE: Hlint says we should use $> instead of *>, how do
-- we stop Hlint from complaining?

-- | Accepts only a particular string s & consumes any white space that follows
stringP :: String -> Parser ()
stringP str = wsP (P.string str) $> ()

-- | Accepts a particular string s, returns a given value x,
-- and consume any white space that follows.
constP :: String -> a -> Parser a
constP s x = stringP s $> x

-- | Parses between parentheses
parens :: Parser a -> Parser a
parens x = P.between (stringP "(") x (stringP ")")

-- | Parses colnames (any sequence of upper and lowercase letters, digits &
-- underscores, not beginning with a digit and not being a reserved word)
colNameP :: Parser ColName
colNameP = P.filter (`notElem` reserved) (wsP $ liftA2 (:) startChar $ many remainingChars)
  where
    startChar = P.alpha <|> P.char '_'
    remainingChars = startChar <|> P.digit

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
valueP :: Parser Value
valueP = intValP <|> boolValP <|> stringValP <|> doubleValP

-- Parses positive / negative integers
intValP :: Parser Value
intValP = IntVal <$> wsP P.int

boolValP :: Parser Value
boolValP = BoolVal <$> (constP "true" True <|> constP "false" False)

-- TODO: fix this!
doubleValP :: Parser Value
doubleValP = undefined -- "intValP <* stringP "." *> intValP"

-- Between quote characters, we have non-quote characters
stringValP :: Parser Value
stringValP = StringVal <$> P.between (P.char '\"') (many $ P.satisfy (/= '\"')) (stringP "\"")

-- Parses the token "IS NULL"
-- QUESTION FOR JOE: Should we change the type?
-- IsNull :: ColName -> NullOp
-- isNullTokenP :: Parser (ColName -> NullOp)
-- isNullTokenP = constP "is null" IsNull

-- Parses the token "IS NOT NULL"
-- isNotNullTokenP :: Parser (ColName -> NullOp)
-- isNotNullTokenP = constP "is not null" IsNotNull

-- Parses the SELECT token
selectTokenP :: Parser ()
selectTokenP = stringP "select"

-- Parses the names of aggregate functions and returns a function from ColName -> ColExp
aggFuncTokenP :: Parser (ColName -> ColExp)
aggFuncTokenP =
  constP "count" (Agg Count)
    <|> constP "avg" (Agg Avg)
    <|> constP "sum" (Agg Sum)
    <|> constP "min" (Agg Min)
    <|> constP "max" (Agg Max)

-- Names of aggregate functions
aggFuncNames :: [String]
aggFuncNames = ["count", "avg", "sum", "min", "max"]

-- Strips leading/trailing whitespace from a string
stripSpace :: String -> String
stripSpace = dropWhileEnd isSpace . dropWhile isSpace

-- >>> stripSpace "     hello world "
-- "hello world"

-- Parses a string corresponding to a SELECT expression
selectExpP :: String -> Either P.ParseError SelectExp
selectExpP str =
  case P.doParse selectTokenP str of
    Nothing -> Left "No parses"
    Just ((), remainderStr) ->
      case remainderStr of
        "*" -> Right Star
        _ ->
          let cols = map stripSpace (splitOnDelims [",", " "] remainderStr)
           in case cols of
                [] -> Left "No columns selected to Query"
                hd : tl -> case hd of
                  "distinct" -> Right $ DistinctCols (selectExpHelper tl)
                  _ -> Right $ Cols (selectExpHelper cols)

-- Recursive helper for iterating over list of colnames
selectExpHelper :: [String] -> [ColExp]
selectExpHelper strs = rights (map parseSelectAttr strs)

-- >>> P.doParse selectTokenP "select count(col1)"
-- Just ((),"count(col1)")

-- >>> map stripSpace (splitOnDelims [",", " "] "count(col1)")
-- ["count(col1)"]

-- >>> rights (map parseSelectAttr ["count(col1)"])

-- Split a string on multiple delimiters
-- (We use foldl' to force the accumulator argument to be evaluated immediately)
splitOnDelims :: [String] -> String -> [String]
splitOnDelims delims str = filter (not . null) (foldl' (\xs delim -> concatMap (splitOn delim) xs) [str] delims)

-- >>> splitOnDelims ["(", ")"] "count(col1)"

-- Parses a single string as a ColExp
parseSelectAttr :: String -> Either P.ParseError ColExp
parseSelectAttr str =
  let newStrs = splitOnDelims ["(", ")"] str
   in case newStrs of
        [col] -> Col <$> P.parse colNameP str
        agg : cols -> if agg `elem` aggFuncNames then map (P.parse (aggFuncTokenP <*> parens colNameP)) cols else undefined
        _ -> Left "error, tried to parse empty string"

-- str `elem` aggFuncNames = P.parse (aggFuncTokenP <*> parens colNameP) str
-- otherwise = Col <$> P.parse colNameP str

fromTokenP :: Parser ()
fromTokenP = stringP "from"

fromExpP :: Parser FromExp
fromExpP = undefined

whereTokenP :: Parser ()
whereTokenP = stringP "where"

groupByTokenP :: Parser ()
groupByTokenP = stringP "group by"

orderByTokenP :: Parser ()
orderByTokenP = stringP "order by"

-- QUESTION FOR JOE: Is the type of the limitP parser bad (since it parses a function?)
-- Parses the "Limit" token & the subsequent integer
limitP :: Parser (Int -> LimitExp)
limitP = constP "limit" Limit

-- Initial query prior to parsing
-- initialQuery :: Query
-- initialQuery =
--   Query
--     { select = EmptySelect,
--       from = EmptyFrom,
--       wher = Nothing,
--       groupBy = Nothing,
--       limit = Nothing,
--       orderBy = Nothing
--     }

orderP :: Parser (ColName, Order)
orderP = undefined

joinStyleP :: Parser JoinStyle
joinStyleP = undefined

comparableP :: Parser Comparable
comparableP = undefined

-- Parses Join expressions
joinExpP :: Parser JoinExp
joinExpP = undefined

-- Parses Where expressions
whereExpP :: Parser BoolExp
whereExpP = undefined

-- Parsers for various operations
renameOpP :: Parser RenameOp
renameOpP = undefined

aggFuncOp :: Parser AggFunc
aggFuncOp = undefined

nullOpP :: Parser NullOp
nullOpP = undefined

compOpP :: Parser CompOp
compOpP = undefined

arithOpP :: Parser ArithOp
arithOpP = undefined

logicOpP :: Parser LogicOp
logicOpP = undefined

-- Wrapper function for all the query business logic

parseQuery :: Parser Query
parseQuery = undefined "wsP $ selectExpP <|> fromExpP"

-- Converts query string to lower case & splits on new lines
splitQueryString :: String -> [String]
splitQueryString str = lines (map toLower str)

-- >>> splitQueryString "SELECT DISTINCT col"

-- Parses a SQL file (takes in filename of the SQL file)
-- Returns either a ParseError (Left) or a Query (Right)
parseSqlFile :: String -> IO (Either P.ParseError Query)
parseSqlFile = P.parseFromFile (const <$> parseQuery <*> P.eof)

-- nullOpP = wsP $ P.string "IS NULL" *> pure isNull

{-
select = Nothing
From = NOthing
Where = Nothing

WE parse the string --> where we find select = val, from = val, where = val

return Query {select = select, from = from, where=from}

-}
