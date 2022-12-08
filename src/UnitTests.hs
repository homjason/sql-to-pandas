module UnitTests where

import Parser
import Parser qualified as P
import SQLParser
import Test.HUnit (Counts, Test (..), runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Translator
import Types.PandasTypes as Pandas
import Types.SQLTypes as SQL
import Types.TableTypes
import Types.Types

-- SELECT Expression Tests
test_selectTokenP :: Test
test_selectTokenP =
  "parsing SELECT token"
    ~: TestList
      [ P.parse selectTokenP "select col1" ~?= Right (),
        P.parse selectTokenP "from tableA" ~?= Left "No parses"
      ]

test_splitOnDelims :: Test
test_splitOnDelims =
  "split on comma and spaces"
    ~: TestList
      [ splitOnDelims [",", " "] "select distinct col1, col2" ~?= ["select", "distinct", "col1", "col2"],
        splitOnDelims [",", " "] "select" ~?= ["select"],
        splitOnDelims [",", " "] "" ~?= []
      ]

test_selectExpP :: Test
test_selectExpP =
  "parsing SELECT expressions"
    ~: TestList
      [ selectExpP "select col1" ~?= Right (Cols [Col "col1"]),
        selectExpP "select col1, col2, col3"
          ~?= Right (Cols [Col "col1", Col "col2", Col "col3"]),
        selectExpP "select distinct col1"
          ~?= Right (DistinctCols [Col "col1"]),
        selectExpP "select distinct col1, col2, col3"
          ~?= Right (DistinctCols [Col "col1", Col "col2", Col "col3"]),
        selectExpP "select count(col1)"
          ~?= Right (Cols [SQL.Agg Count "col1"])
      ]

-- >>> runTestTT test_selectExpP
-- Counts {cases = 5, tried = 5, errors = 0, failures = 1}

test_parseQuerySimple :: Test
test_parseQuerySimple =
  "simple SQL SELECT & FROM queries"
    ~: TestList
      [ P.parse parseQuery "SELECT col FROM table"
          ~?= Right
            Query
              { select = Cols [Col "col"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Nothing,
                limit = Nothing,
                orderBy = Nothing
              }
      ]

-- >>> runTestTT test_selectExpP

-- test_fromExpP :: Test
-- test_fromExpP =
--   "parsing FROM expressions"
--     ~: TestList
--       [ P.parse fromExpP "FROM A" ~?= Right (TableName "A" Nothing),
--         P.parse fromExpP "FROM A JOIN B ON A.col = B.col"
--           ~?= Right
--             ( TableName
--                 "A"
--                 ( Just $
--                     Join
--                       { leftTable = "A",
--                         leftCol = "col",
--                         rightTable = "B",
--                         rightCol = "col",
--                         style = InnerJoin
--                       }
--                 )
--             ),
--         P.parse fromExpP "FROM (SELECT col FROM B)"
--           ~?= Right
--             ( SubQuery
--                 Query
--                   { select = Cols [Col "col"],
--                     from = Table "B" Nothing,
--                     wher = Nothing,
--                     groupBy = Nothing,
--                     limit = Nothing,
--                     orderBy = Nothing
--                   }
--                 Nothing
--             )
--       ]

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

-- TRANSLATOR unit tests

selectStarQ :: Query
selectStarQ =
  Query
    { select = Star,
      from = Table "df" Nothing,
      wher = Nothing,
      groupBy = Nothing,
      limit = Nothing,
      orderBy = Nothing
    }

selectStarCommand :: Command
selectStarCommand =
  Command
    { df = "df",
      cols = Nothing,
      fn = Nothing
    }

test_translateSQLSimple :: Test
test_translateSQLSimple =
  "simple SQL queries to Pandas"
    ~: TestList
      [ translateSQL selectStarQ ~?= Block [selectStarCommand]
      ]

-- Converting "SELECT" expressions into list of colnames in Pandas
-- test_selectExpToCols :: Test
-- test_selectExpToCols =
--   "translating SQL SELECT to Pandas"
--     ~: TestList
--       [ selectExpToCols $ Star ~?= ([] :: [ColName]),
--         selectExpToCols $ Cols ["colA", "colB", "colC"] ~?= ["colA", "colB", "colC"]
--       ]
