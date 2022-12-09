module SQLParser where

import Control.Applicative
import Control.Monad
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

-- Use a parser for a particular string
-- (Modification of P.parser in Parser.hs that returns
-- both the parsed string & the remainder of the string)
parseResult :: Parser a -> String -> Either P.ParseError (a, String)
parseResult parser str = case P.doParse parser str of
  Nothing -> Left "No parses"
  Just (a, remainder) -> Right (a, remainder)

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
nameP :: Parser String
nameP = P.filter (`notElem` reserved) (wsP $ liftA2 (:) startChar $ many remainingChars)
  where
    startChar = P.alpha <|> P.char '_'
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
-- isNullTokenP :: Parser NullOp
-- isNullTokenP = constP "is null" IsNull <* nameP

-- Parses the token "IS NOT NULL"
-- isNotNullTokenP :: Parser (ColName -> NullOp)
-- isNotNullTokenP = constP "is not null" IsNotNull

-- | Parses the SELECT token
selectTokenP :: Parser ()
selectTokenP = stringP "select"

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

-- | Strips leading/trailing whitespace from a string
stripSpace :: String -> String
stripSpace = dropWhileEnd isSpace . dropWhile isSpace

-- | Parses a string corresponding to a SELECT expression
parseSelectExp :: String -> Either P.ParseError SelectExp
parseSelectExp str =
  case P.doParse selectTokenP str of
    Nothing -> Left "No parses"
    Just ((), remainderStr) ->
      case remainderStr of
        "*" -> Right Star
        _ ->
          let cols = map stripSpace (splitOnDelims [",", " "] remainderStr)
           in case cols of
                [] -> Left "No columns selected in Query"
                hd : tl -> case hd of
                  "distinct" -> Right $ DistinctCols (selectExpHelper tl)
                  _ -> Right $ Cols (selectExpHelper cols)

-- | Split a string on multiple delimiters
-- (Use foldl' to force the accumulator argument to be evaluated immediately)
splitOnDelims :: [String] -> String -> [String]
splitOnDelims delims str =
  filter
    (not . null)
    (foldl' (\xs delim -> concatMap (splitOn delim) xs) [str] delims)

-- | Recursive helper for iterating over list of colnames
selectExpHelper :: [String] -> [ColExp]
selectExpHelper strs = rights (map parseSelectAttr strs)

-- | Parses a single string as a ColExp
-- (If there's a function call,
-- split the string on parens to identify the aggregate function & column)
parseSelectAttr :: String -> Either P.ParseError ColExp
parseSelectAttr str =
  let newStrs = splitOnDelims ["(", ")"] str
   in case newStrs of
        [col] -> Col <$> P.parse nameP str
        [agg, col] ->
          if agg `elem` aggFuncNames
            then P.parse (aggFuncTokenP <*> parens nameP) str
            else Left "Error: tried to call undefined function on a column"
        _ -> Left "Error: Malformed SelectExp"

-- | Parses the token "from", consumes subsequent whitespace
fromTokenP :: Parser ()
fromTokenP = stringP "from"

-- | Parses the token "on", consumes subsequent whitespace
onTokenP :: Parser ()
onTokenP = stringP "on"

-- | Parses the equal sign
eqTokenP :: Parser ()
eqTokenP = stringP "="

-- | Parses commands indicating the style of the join
joinTokenP :: Parser JoinStyle
joinTokenP =
  constP "join" InnerJoin
    <|> constP "left join" LeftJoin
    <|> constP "right join" RightJoin

-- | Parses FROM expressions
-- TODO: handle subqueries!
parseFromExp :: String -> Either P.ParseError FromExp
parseFromExp str = case P.doParse fromTokenP str of
  Nothing -> Left "Error: No parses, malformed FROM expression"
  Just ((), remainder) ->
    case words remainder of
      [] -> Left "Error: No table selected in FROM expression"
      [tableName] -> Right $ Table tableName Nothing
      _ -> case parseJoinExp remainder of
        Right joinExp@(Join leftT _ _ _ _) -> Right $ Table leftT (Just joinExp)
        Left errorMsg -> Left errorMsg

-- | Parses JOIN expressions
-- (resolves the two tables & columns partaking in the join)
-- For simplicity, we explciitly disallow joining the same table with itself
parseJoinExp :: String -> Either P.ParseError JoinExp
parseJoinExp str = do
  (leftT, s') <- parseResult nameP str
  (joinStyle, s'') <- parseResult joinTokenP s'
  (rightT, joinCond) <- parseResult nameP s''
  when (null joinCond) (Left "No join condition specified")
  (leftTC, remainder) <- parseResult (onTokenP *> nameP) joinCond
  (rightTC, tl) <- parseResult (eqTokenP *> nameP) remainder
  unless (null tl) (Left "Invalid JOIN expression")
  case (splitOn "." leftTC, splitOn "." rightTC) of
    ([leftT', leftC], [rightT', rightC]) -> do
      when
        (leftT == rightT || leftT' == rightT')
        (Left "Can't join the same table with itself")
      unless
        (leftT == leftT' && rightT == rightT')
        (Left "Tables being JOINed != tables being selected FROM")
      Right $
        Join
          { leftTable = leftT,
            leftCol = leftC,
            rightTable = rightT,
            rightCol = rightC,
            style = joinStyle
          }
    (_, _) -> Left "Malformed JOIN condition"

whereTokenP :: Parser ()
whereTokenP = stringP "where"

compOpP :: Parser CompOp
compOpP = constP "=" Eq <|> constP ">" Gt <|> constP ">=" Ge <|> constP "<" Lt <|> constP "<=" Le

arithOpP :: Parser ArithOp
arithOpP = wsP $ constP "+" Plus <|> constP "-" Minus <|> constP "*" Times <|> constP "//" Divide <|> constP "%" Modulo

logicOpP :: Parser LogicOp
logicOpP = constP "and" And <|> constP "or" Or

-- Parses Where expressions
parseWhereExp :: String -> Either P.ParseError BoolExp
parseWhereExp str = case P.doParse whereTokenP str of
  Nothing -> Left "No parses"
  Just ((), remainderStr) ->
    let cols = map stripSpace (splitOnDelims [",", " "] remainderStr)
     in case cols of
          [] -> Left "No columns selected to Group By"
          hd : tl -> undefined

groupByTokenP :: Parser ()
groupByTokenP = stringP "group by"

-- Parse GROUP BY expressions
parseGroupByExp :: String -> Either P.ParseError [ColName]
parseGroupByExp str = case P.doParse groupByTokenP str of
  Nothing -> Left "No parses"
  Just ((), remainderStr) ->
    let cols = map stripSpace (splitOnDelims [",", " "] remainderStr)
     in case cols of
          [] -> Left "No columns selected to Group By"
          hd : tl -> Right cols

orderByTokenP :: Parser ()
orderByTokenP = stringP "order by"

-- Parse ORDER BY expressions
-- Note: we specify that we can only sort by one column
parseOrderByExp :: String -> Either P.ParseError (ColName, Order)
parseOrderByExp str =
  case P.doParse orderByTokenP str of
    Nothing -> Left "no parses"
    Just ((), remainder) ->
      let orderByExp = map stripSpace (splitOnDelims [",", " "] remainder)
       in case orderByExp of
            [] -> Left "Incomplete Order By expression"
            [col] -> Right (col, Asc)
            [col, order] ->
              case stringToOrder order of
                Just ord -> Right (col, ord)
                Nothing -> Left "Error: invalid sort order"
            _ -> Left "Too many tokens in Order By expression"

-- Helper function: converts a string representing a sort order to an Order
stringToOrder :: String -> Maybe Order
stringToOrder "asc" = Just Asc
stringToOrder "desc" = Just Desc
stringToOrder _ = Nothing

-- Parser for limit expressions (consumes subsequent whitespace)
limitP :: Parser Int
limitP = stringP "limit" *> P.int

-- Parses the "Limit" token & the subsequent integer
parseLimitExp :: String -> Either P.ParseError Int
parseLimitExp = P.parse limitP

-- Parsers for various operations
renameOpP :: Parser RenameOp
renameOpP = undefined

aggFuncOp :: Parser AggFunc
aggFuncOp = undefined

nullOpP :: Parser NullOp
nullOpP = undefined "TODO"

-- Datatype that encapsulates various SQL query conditions
-- (WHERE / GROUP BY / ORDER BY / LIMIT)
data Condition
  = Wher BoolExp
  | GroupBy [ColName]
  | OrderBy (ColName, Order)
  | Limit Int
  deriving (Eq, Show)

-- TODO: In the function inferCondition, use alternative instead of nested cases
-- (change return type of ParseGroupByExp, parseOrderByExp etc. to
-- Parser a instead of using Either monad)

-- | Infers whether a query condition is a WHERE, a GROUP BY,
-- an ORDER BY or a LIMIT
inferCondition :: String -> Either P.ParseError Condition
inferCondition str =
  case parseWhereExp str of
    Right whereExp -> Right $ Wher whereExp
    Left e1 ->
      case parseGroupByExp str of
        Right groupByCols -> Right $ GroupBy groupByCols
        Left e2 ->
          case parseOrderByExp str of
            Right orderByExp -> Right $ OrderBy orderByExp
            Left e3 ->
              case parseLimitExp str of
                Right numRows -> Right $ Limit numRows
                Left e4 -> Left "Malformed SQL query"

-- | Construct a Query based on a SelectExp, FromExp & some condition
mkQuery :: SelectExp -> FromExp -> Condition -> Query
mkQuery s f condition =
  case condition of
    Wher w -> Query s f (Just w) Nothing Nothing Nothing
    GroupBy g -> Query s f Nothing (Just g) Nothing Nothing
    OrderBy o -> Query s f Nothing Nothing (Just o) Nothing
    Limit l -> Query s f Nothing Nothing Nothing (Just l)

-- | Construct a Query based on a SelectExp, FromExp & two conditions
-- Returns a ParseError if the two conditions are invalid / in the wrong order
mkQuery2 :: SelectExp -> FromExp -> Condition -> Condition -> Either P.ParseError Query
mkQuery2 s f c1 c2 =
  case (c1, c2) of
    (Wher w, GroupBy g) -> Right $ Query s f (Just w) (Just g) Nothing Nothing
    (Wher w, OrderBy o) -> Right $ Query s f (Just w) Nothing (Just o) Nothing
    (Wher w, Limit l) -> Right $ Query s f (Just w) Nothing Nothing (Just l)
    (GroupBy g, OrderBy o) -> Right $ Query s f Nothing (Just g) (Just o) Nothing
    (GroupBy g, Limit l) -> Right $ Query s f Nothing (Just g) Nothing (Just l)
    (OrderBy o, Limit l) -> Right $ Query s f Nothing Nothing (Just o) (Just l)
    (_, _) -> Left "Malformed SQL query, couldn't infer query conditions"

-- | Construct a Query based on a SelectExp, FromExp & three conditions
-- Returns a ParseError if the two conditions are invalid / in the wrong order
mkQuery3 :: SelectExp -> FromExp -> Condition -> Condition -> Condition -> Either P.ParseError Query
mkQuery3 s f c1 c2 c3 =
  case (c1, c2, c3) of
    (Wher w, GroupBy g, OrderBy o) -> Right $ Query s f (Just w) (Just g) (Just o) Nothing
    (Wher w, OrderBy o, Limit l) -> Right $ Query s f (Just w) Nothing (Just o) (Just l)
    (GroupBy g, OrderBy o, Limit l) -> Right $ Query s f Nothing (Just g) (Just o) (Just l)
    (_, _, _) -> Left "Malformed SQL query, couldn't infer query conditions"

-- | Wrapper function for all the query-parsing business logic
-- s = SELECT expression
-- f = FROM expression
-- w = WHERE expression
-- g = GROUP BY expression
-- o = ORDER BY expression
-- l = LIMIT expression
-- c = Arbitrary SQL query condition (WHERE/GROUP BY/ORDER BY/LIMIT)
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
        (Right w@(Wher _), Right g@(GroupBy _)) -> mkQuery2 s f w g
        (Right w@(Wher _), Right o@(OrderBy _)) -> mkQuery2 s f w o
        (Right w@(Wher _), Right l@(Limit _)) -> mkQuery2 s f w l
        (Right g@(GroupBy _), Right o@(OrderBy _)) -> mkQuery2 s f g o
        (Right g@(GroupBy _), Right l@(Limit _)) -> mkQuery2 s f g l
        (Right o@(OrderBy _), Right l@(Limit _)) -> mkQuery2 s f o l
        (_, _) -> Left "Malformed SQL query"
    [select, from, c1, c2, c3] -> do
      s <- parseSelectExp select
      f <- parseFromExp from
      case (inferCondition c1, inferCondition c2, inferCondition c3) of
        (Right w@(Wher _), Right g@(GroupBy _), Right o@(OrderBy _)) -> mkQuery3 s f w g o
        (Right w@(Wher _), Right o@(OrderBy _), Right l@(Limit _)) -> mkQuery3 s f w o l
        (Right g@(GroupBy _), Right o@(OrderBy _), Right l@(Limit _)) -> mkQuery3 s f g o l
        (_, _, _) -> Left "Malformed SQL query"
    [select, from, wher, groupBy, orderBy, limit] -> do
      s <- parseSelectExp select
      f <- parseFromExp from
      w <- parseWhereExp wher
      g <- parseGroupByExp groupBy
      o <- parseOrderByExp orderBy
      l <- parseLimitExp limit
      Right $ Query s f (Just w) (Just g) (Just o) (Just l)
    _ -> Left "Invalid SQL Query"

-- Converts query string to lower case, splits on newlines
-- & strips leading/trailing whitespace
-- NB: each clause in a SQL query (SELECT, FROM, etc.) must be on a new line
splitQueryString :: String -> [String]
splitQueryString = map stripSpace . lines . map toLower

-- Parses a SQL file (takes in filename of the SQL file)
-- Returns either a ParseError (Left) or a Query (Right)
-- parseSqlFile :: String -> IO (Either P.ParseError Query)
-- parseSqlFile = P.parseFromFile (const <$> parseQuery <*> P.eof)

-- nullOpP = wsP $ P.string "IS NULL" *> pure isNull
