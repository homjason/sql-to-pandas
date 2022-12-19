module Lib (runParseAndTranslate, runSqlToPandas) where

import Data.Either
import Parser (Parser)
import Parser qualified as P
import Print
import SQLParser
import Translator
import Types.PandasTypes

-- Parses the string into a query and translates it into a Pandas Command
runParseAndTranslate :: String -> Either P.ParseError Command
runParseAndTranslate s = case parseQuery s of
  Left str -> Left str
  Right q ->
    case validateQuery q of
      Left validateError -> Left validateError
      Right False -> Left "Invalid SQL query"
      Right True -> Right $ translateSQL q

-- Takes in a string, parses it to a SQL query, translates to a Pandas command, and prints the Pandas command
runSqlToPandas :: String -> String
runSqlToPandas s = case parseQuery s of
  Left str -> str
  Right q -> case validateQuery q of
    Left validateError -> validateError
    Right False -> "Invalid SQL query"
    Right True -> pretty $ translateSQL q
