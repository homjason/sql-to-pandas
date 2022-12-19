import Lib
import QuickCheckTests
import Test.HUnit
import Test.QuickCheck
import UnitTests

main :: IO ()
main = do
  putStrLn "RUN SQL Parser Tests"
  test_sql_parser
  putStrLn "----------------------------"
  putStrLn "RUN TRANSLATOR TESTS"
  test_translator
  putStrLn "----------------------------"
  putStrLn "RUN PRINT TESTS"
  test_print
  putStrLn "----------------------------"
  putStrLn "RUN TABLE TESTS"
  test_table
  putStrLn "----------------------------"
  return ()
