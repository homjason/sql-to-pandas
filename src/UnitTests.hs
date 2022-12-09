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

test_parseSelectExp :: Test
test_parseSelectExp =
  "parsing SELECT expressions"
    ~: TestList
      [ parseSelectExp "select col1" ~?= Right (Cols [Col "col1"]),
        parseSelectExp "select col1, col2, col3"
          ~?= Right (Cols [Col "col1", Col "col2", Col "col3"]),
        parseSelectExp "select distinct col1"
          ~?= Right (DistinctCols [Col "col1"]),
        parseSelectExp "select distinct col1, col2, col3"
          ~?= Right (DistinctCols [Col "col1", Col "col2", Col "col3"]),
        parseSelectExp "select count(col1)"
          ~?= Right (Cols [SQL.Agg Count "col1"])
      ]

-- >>> runTestTT test_parseSelectExp
-- Counts {cases = 5, tried = 5, errors = 0, failures = 1}

-- GROUP BY Expression Tests
test_groupByP :: Test
test_groupByP =
  "parsing GROUP BY expressions"
    ~: TestList
      [ groupByP "group by col1" ~?= Right ["col1"],
        groupByP "group by col1, col2" ~?= Right ["col1", "col2"],
        groupByP "group by" ~?= Left "No columns selected to Group By",
        groupByP "hello world" ~?= Left "No parses"
      ]

test_orderByP :: Test
test_orderByP =
  "parsing ORDER BY clauses"
    ~: TestList
      [ orderByP "arbitrary_string" ~?= Left "no parses",
        orderByP "order by" ~?= Left "Error: incomplete Order By expression",
        orderByP "order by col0" ~?= Right ("col0", Asc),
        orderByP "order by col2 asc" ~?= Right ("col2", Asc),
        orderByP "order by col1 desc" ~?= Right ("col1", Desc),
        orderByP "order by col1 wrongOrder" ~?= Left "Error: invalid sort order",
        orderByP "order by nonexistentCol wrongOrder" ~?= Left "Error: invalid sort order",
        orderByP "order by col1, col2 asc" ~?= Left "Error: too many tokens in Order By expression",
        orderByP "order by col1, col2, col3 desc" ~?= Left "Error: too many tokens in Order By expression"
      ]

-- >>> runTestTT test_orderByP

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

-- >>> runTestTT test_parseFromExp
-- Counts {cases = 3, tried = 3, errors = 0, failures = 2}

test_parseFromExp :: Test
test_parseFromExp =
  "parsing FROM expressions"
    ~: TestList
      [ parseFromExp "from A" ~?= Right (Table "A" Nothing),
        parseFromExp "from a join b on a.col = b.col"
          ~?= Right
            ( Table
                "a"
                ( Just $
                    Join
                      { leftTable = "a",
                        leftCol = "col",
                        rightTable = "b",
                        rightCol = "col",
                        style = InnerJoin
                      }
                )
            ),
        parseFromExp "from df1 left join df2 on df1.col1 = df2.col2"
          ~?= Right
            ( Table
                "df1"
                ( Just $
                    Join
                      { leftTable = "df1",
                        leftCol = "col1",
                        rightTable = "df2",
                        rightCol = "col2",
                        style = LeftJoin
                      }
                )
            )
            -- parseFromExp "from (select col from B)"
            --   ~?= Right
            --     ( SubQuery
            --         Query
            --           { select = Cols [Col "col"],
            --             from = Table "B" Nothing,
            --             wher = Nothing,
            --             groupBy = Nothing,
            --             limit = Nothing,
            --             orderBy = Nothing
            --           }
            --         Nothing
            --     )
      ]

test_parseJoinExp :: Test
test_parseJoinExp =
  "parsing join expressions"
    ~: TestList
      [ parseJoinExp "a join b on a.col = b.col"
          ~?= Right (Join {leftTable = "a", leftCol = "col", rightTable = "b", rightCol = "col", style = InnerJoin}),
        parseJoinExp "a left join b on a.col = b.col"
          ~?= Right (Join {leftTable = "a", leftCol = "col", rightTable = "b", rightCol = "col", style = LeftJoin}),
        parseJoinExp "a right join b on a.col = b.col"
          ~?= Right (Join {leftTable = "a", leftCol = "col", rightTable = "b", rightCol = "col", style = RightJoin}),
        parseJoinExp "a left join b"
          ~?= Left "No join condition specified",
        parseJoinExp "a join b on c.col = d.col"
          ~?= Left "Tables being JOINed != tables being selected FROM",
        parseJoinExp "a join a on a.col = a.col"
          ~?= Left "Can't join the same table with itself",
        parseJoinExp "a join a on a.col = b.col"
          ~?= Left "Can't join the same table with itself",
        parseJoinExp "a join b"
          ~?= Left "No join condition specified",
        parseJoinExp "a join b on a.col = b.col arbitrarySuffix"
          ~?= Left "Invalid JOIN expression",
        parseJoinExp "a invalidJoin b on a.col = b.col"
          ~?= Left "No parses",
        parseJoinExp "a join b on aCol = bCol"
          ~?= Left "Malformed JOIN condition",
        parseJoinExp "a join b on c"
          ~?= Left "No parses"
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
