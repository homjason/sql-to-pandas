module Main where

import Control.Monad
-- import Lib

import Lib (runSqlToPandas)
import Print
import SQLParser
import Translator

main :: IO ()
main = do
  putStrLn "Enter a SQL query you would like to translate to Pandas!"
  str <- getLine
  putStrLn "\n Pandas Result: "
  putStrLn $ runSqlToPandas str
