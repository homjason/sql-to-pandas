module UnitTests where

import Parser qualified as P
import SQLParser
import Test.HUnit (Counts, Test (..), runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Types.PandasTypes
import Types.SQLTypes as SQL
import Types.TableTypes
import Types.Types

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

-- test_simpleSelectFrom :: Test
-- test_simpleSelectFrom =
--   "simple SELECT & FROM"
--     ~: TestList
--       []

test_selectExpP :: Test
test_selectExpP =
  "parsing SELECT expressions"
    ~: TestList
      [ P.parse selectExpP "SELECT col1" ~?= Right (Cols ["col1"]),
        P.parse selectExpP "SELECT col1, col2, col3"
          ~?= Right (Cols ["col1", "col2", "col3"]),
        P.parse selectExpP "SELECT DISTINCT col1"
          ~?= Right (DistinctCols ["col1"]),
        P.parse selectExpP "SELECT DISTINCT col1, col2, col3"
          ~?= Right (DistinctCols ["col1", "col2", "col3"]),
        P.parse selectExpP "SELECT COUNT(col1) AS c"
          ~?= Right (SQL.Agg Count "col1" "c")
      ]

test_fromExpP :: Test
test_fromExpP =
  "parsing FROM expressions"
    ~: TestList
      [ P.parse fromExpP "FROM A" ~?= Right (TableName "A" Nothing),
        P.parse fromExpP "FROM A JOIN B ON A.col = B.col"
          ~?= Right
            ( TableName
                "A"
                ( Just $
                    Join
                      { table = "B",
                        condition = (("A", "col"), ("B", "col")),
                        style = InnerJoin
                      }
                )
            ),
        P.parse fromExpP "FROM (SELECT col FROM B)"
          ~?= Right
            ( SubQuery
                Query
                  { select = Cols ["col"],
                    from = TableName "B" Nothing,
                    wher = Nothing,
                    groupBy = Nothing,
                    limit = Nothing,
                    orderBy = Nothing
                  }
                Nothing
            )
      ]

test_orderP :: Test
test_orderP =
  "parsing ORDER BY clauses"
    ~: TestList
      [ P.parse orderP "ORDER BY col0" ~?= Right ("col0", Asc),
        P.parse orderP "ORDER BY col2 ASC" ~?= Right ("col2", Asc),
        P.parse orderP "ORDER BY col1 DESC" ~?= Right ("col1", Desc)
      ]

test_doubleValP :: Test
test_doubleValP =
  "parsing literal doubles"
    ~: TestList
      [ P.parse doubleValP "5.52" ~?= Right (DoubleVal (5.52 :: Double)),
        P.parse doubleValP "-2.25" ~?= Right (DoubleVal (2.25 :: Double)),
        P.parse doubleValP "0.00" ~?= Right (DoubleVal (0.0 :: Double)),
        P.parse doubleValP "1.00" ~?= Right (DoubleVal (1.0 :: Double)),
        P.parse doubleValP "-3.00" ~?= Right (DoubleVal (-3.0 :: Double))
      ]
