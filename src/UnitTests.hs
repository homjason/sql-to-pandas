module UnitTests where

import SQLParser
import Test.HUnit (Counts, Test (..), runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Types.PandasTypes
import Types.SQLTypes
import Types.TableTypes
import Types.Types ()

test_parseQuerySimple :: Test
test_parseQuerySimple =
  "simple SQL SELECT & FROM queries"
    ~: TestList
      [ parseQuery "SELECT col FROM table"
          ~?= Right
            Query
              { select = Cols ["col"],
                from = TableName "table" Nothing,
                wher = Nothing,
                groupBy = Nothing,
                limit = Nothing,
                orderBy = Nothing
              }
      ]

test_simpleSelectFrom :: Test
test_simpleSelectFrom =
  "simple SELECT & FROM"
    ~: TestList
      []

test_orderP :: Test
test_orderP =
  "parsing ORDER BY clauses"
    ~: TestList
      [ orderP "ORDER BY col0" ~?= ("col0", ASC),
        orderP "ORDER BY col2 ASC" ~?= ("col2", ASC),
        orderP "ORDER BY col1 DESC" ~?= ("col1", DESC)
      ]

test_doubleValP :: Test
test_doubleValP =
  "parsing literal doubles"
    ~: TestList
      [ doubleValP "5.52" ~?= (5.52 :: Double),
        doubleValP "-2.25" ~?= (2.25 :: Double),
        doubleValP "0.00" ~?= (0.0 :: Double),
        doubleValP "1.00" ~?= (1.0 :: Double),
        doubleValP "-3.00" ~?= (-3.0 :: Double)
      ]
