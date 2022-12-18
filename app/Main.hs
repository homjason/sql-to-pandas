module Main where

import Control.Monad
import Lib
import Print
import SQLParser
import Translator

main :: IO ()
main = do
  putStrLn "Enter a SQL query you would like to translate to Pandas! Note the following requirements..."
  str <- getLine
  case parseQuery str of
    Left error -> putStrLn error
    Right q' -> do
      case validateQuery q' of
        Left err -> putStrLn err
        Right boo -> do
          putStrLn "\nPandas Result:"
          putStrLn $ pretty (translateSQL q')
          return ()

-- unless (Right $ validateQuery q') (putStrLn "Query is malformed!")

-- when (not (validateQuery q')) (putStrLn "Query is malformed!")
