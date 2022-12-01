module SQLParser where

import Control.Applicative
import Data.Char qualified as Char
import Parser (Parser)
import Parser qualified as P
import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Types.SQLTypes
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
stringP str = wsP (P.string str) *> pure ()

-- | Accepts a particular string s, returns a given value x,
-- and consume any white space that follows.
constP :: String -> a -> Parser a
constP s x = stringP s *> pure x

parens :: Parser a -> Parser a
parens x = P.between (stringP "(") x (stringP ")")

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
isNullTokenP :: Parser (ColName -> NullOp)
isNullTokenP = constP "IS NULL" IsNull

-- Parses the token "IS NOT NULL"
isNotNullTokenP :: Parser (ColName -> NullOp)
isNotNullTokenP = constP "IS NOT NULL" IsNotNull

-- Parses the select token
selectTokenP :: Parser ()
selectTokenP = stringP "SELECT"

fromTokenP :: Parser ()
fromTokenP = stringP "FROM"

whereTokenP :: Parser ()
whereTokenP = stringP "WHERE"

groupByTokenP :: Parser ()
groupByTokenP = stringP "GROUP BY"

orderByTokenP :: Parser ()
orderByTokenP = stringP "ORDER BY"

-- QUESTION FOR JOE: Is the type of the limitP parser bad (since it parses a function?)
-- Parses the "Limit" token & the subsequent integer
limitP :: Parser (Int -> LimitExp)
limitP = constP "LIMIT" Limit

-- Initial query prior to parsing
initialQuery :: Query
initialQuery =
  Query
    { select = EmptySelect,
      from = EmptyFrom,
      wher = Nothing,
      groupBy = Nothing,
      limit = Nothing,
      orderBy = Nothing
    }

orderP :: Parser (ColName, Order)
orderP = undefined

selectExpP :: Parser SelectExp
selectExpP = undefined

fromExpP :: Parser FromExp
fromExpP = undefined

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
-- Returns either a ParseError (Left) or a Query(Right)
parseQuery :: String -> Either P.ParseError Query
parseQuery = undefined

-- nullOpP = wsP $ P.string "IS NULL" *> pure isNull

{-
select = Nothing
From = NOthing
Where = Nothing

WE parse the string --> where we find select = val, from = val, where = val

return Query {select = select, from = from, where=from}

-}
