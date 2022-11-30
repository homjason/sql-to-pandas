module SQLParser where

import Control.Applicative
import Data.Char qualified as Char
import Parser (Parser)
import Parser qualified as P
import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Types.SQLTypes
import Types.TableTypes

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

-- Parses the command "IS NULL"
-- QUESTION FOR JOE: Should we change the type?
-- IsNull :: ColName -> NullOp
isNullP :: Parser (ColName -> NullOp)
isNullP = constP "IS NULL" IsNull
