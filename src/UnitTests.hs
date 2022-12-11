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

{- Parser Tests -}
-- SELECT Expression Tests
test_selectTokenP :: Test
test_selectTokenP =
  "parsing SELECT token"
    ~: TestList
      [ P.parse selectTokenP "select col1" ~?= Right (),
        P.parse selectTokenP "from tableA" ~?= Left "No parses"
      ]

test_comparableP :: Test
test_comparableP =
  "parsing Comparable values"
    ~: TestList
      [ P.parse comparableP "col" ~?= Right (ColName "col"),
        P.parse comparableP "52" ~?= Right (LitInt 52),
        P.parse comparableP "\"string literal\"" ~?= Right (LitString "string literal")
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

-- TODO: fix failing test cases
test_parseWhereExp :: Test
test_parseWhereExp =
  "parsing WHERE expressions"
    ~: TestList
      [ parseWhereExp "where 1 + 2"
          ~?= Right (Op2 (CompVal (LitInt 1)) (Arith Plus) (CompVal (LitInt 2))),
        parseWhereExp "where (1) + (2)"
          ~?= Right (Op2 (CompVal (LitInt 1)) (Arith Plus) (CompVal (LitInt 2))),
        parseWhereExp "where col = \"hello\""
          ~?= Right (Op2 (CompVal (ColName "col")) (Comp Eq) (CompVal (LitString "hello"))),
        parseWhereExp "where col is null"
          ~?= Right (Op1 (CompVal (ColName "col")) IsNull)
      ]

-- GROUP BY Expression Tests
test_parseGroupByExp :: Test
test_parseGroupByExp =
  "parsing GROUP BY expressions"
    ~: TestList
      [ parseGroupByExp "group by col1" ~?= Right ["col1"],
        parseGroupByExp "group by col1, col2" ~?= Right ["col1", "col2"],
        parseGroupByExp "group by" ~?= Left "No columns selected to Group By",
        parseGroupByExp "hello world" ~?= Left "No parses"
      ]

test_parseOrderByExp :: Test
test_parseOrderByExp =
  "parsing ORDER BY clauses"
    ~: TestList
      [ parseOrderByExp "arbitrary_string" ~?= Left "no parses",
        parseOrderByExp "order by" ~?= Left "Error: incomplete Order By expression",
        parseOrderByExp "order by col0" ~?= Right ("col0", Asc),
        parseOrderByExp "order by col2 asc" ~?= Right ("col2", Asc),
        parseOrderByExp "order by col1 desc" ~?= Right ("col1", Desc),
        parseOrderByExp "order by col1 wrongOrder" ~?= Left "Error: invalid sort order",
        parseOrderByExp "order by nonexistentCol wrongOrder" ~?= Left "Error: invalid sort order",
        parseOrderByExp "order by col1, col2 asc" ~?= Left "Error: too many tokens in Order By expression",
        parseOrderByExp "order by col1, col2, col3 desc" ~?= Left "Error: too many tokens in Order By expression"
      ]

test_parseQuerySimple :: Test
test_parseQuerySimple =
  "simple SQL SELECT & FROM queries"
    ~: TestList
      [ parseQuery "SELECT col\nFROM table"
          ~?= Right
            Query
              { select = Cols [Col "col"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              }
      ]

test_parseQuery :: Test
test_parseQuery =
  "parsing SQL Queries"
    ~: TestList
      [ parseQuery "SELECT col1\nFROM table\nLIMIT 5"
          ~?= Right
            Query
              { select = Cols [Col "col"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Just 5
              },
        parseQuery "SELECT col1, COUNT(col2)\nFROM table\nGROUP BY col1"
          ~?= Right
            Query
              { select = Cols [Col "col1", Agg Count "col2"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Just ["col1"],
                orderBy = Nothing,
                limit = Nothing
              },
        parseQuery "SELECT col1, COUNT(col2)\n     FROM table   \n      GROUP BY col1"
          ~?= Right
            Query
              { select = Cols [Col "col1", Agg Count "col2"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Just ["col1"],
                orderBy = Nothing,
                limit = Nothing
              },
        parseQuery "SELECT col\nFROM table\nORDER BY col"
          ~?= Right
            Query
              { select = Cols [Col "col"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Just ("col", Asc),
                limit = Nothing
              },
        parseQuery "SELECT col, col2\nFROM table\nWHERE col > 4"
          ~?= Right
            Query
              { select = Cols [Col "col", Col "col2"],
                from = Table "table" Nothing,
                wher = Just $ Op2 (CompVal $ ColName "col") (Comp Gt) (CompVal $ LitInt 4),
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              }
      ]

-- >>> runTestTT test_parseQuery

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

-- test_doubleValP :: Test
-- test_doubleValP =
--   "parsing literal doubles"
--     ~: TestList
--       [ P.parse doubleValP "5.52" ~?= Right (DoubleVal (5.52 :: Double)),
--         P.parse doubleValP "-2.25" ~?= Right (DoubleVal (2.25 :: Double)),
--         P.parse doubleValP "0.00" ~?= Right (DoubleVal (0.0 :: Double)),
--         P.parse doubleValP "1.00" ~?= Right (DoubleVal (1.0 :: Double)),
--         P.parse doubleValP "-3.00" ~?= Right (DoubleVal (-3.0 :: Double))
--       ]

{- TRANSLATOR unit tests -}

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
    ~: TestList []

--       [ translateSQL selectStarQ ~?= Block [selectStarCommand]
--       ]

-- Converting "SELECT" expressions into list of colnames in Pandas
test_selectExpToCols :: Test
test_selectExpToCols =
  "translating SQL SELECT to Pandas"
    ~: TestList
      [ selectExpToCols Star ~?= ([] :: [ColName], Nothing),
        selectExpToCols (Cols [Col "colA", Col "colB"]) ~?= (["colA", "colB"], Nothing),
        selectExpToCols (DistinctCols [Col "colA", Col "colB"])
          ~?= (["colA", "colB"], Just [Unique ["colA", "colB"]]),
        selectExpToCols (Cols [Col "colA", Agg Count "colB"])
          ~?= (["colA", "colB"], Just [Aggregate Count "colB"]),
        selectExpToCols (Cols []) ~?= ([], Nothing),
        selectExpToCols (DistinctCols []) ~?= ([], Nothing)
      ]

test_translateJoinExp :: Test
test_translateJoinExp =
  "translating SQL JOIN ON to Pandas Merge"
    ~: TestList
      [ translateJoinExp (Join "A" "id" "B" "id" InnerJoin)
          ~?= Merge
            MkMerge
              { rightDf = "B",
                leftOn = "id",
                rightOn = "id",
                how = InnerJoin
              }
      ]

test_translateFromExp :: Test
test_translateFromExp =
  "translating SQL FROM to Pandas"
    ~: TestList
      [ translateFromExp (Table "A" Nothing) ~?= ("A", Nothing),
        translateFromExp (Table "A" (Just $ Join "A" "id" "B" "id" InnerJoin))
          ~?= ( "A",
                Just $
                  Merge $
                    MkMerge
                      { rightDf = "B",
                        leftOn = "id",
                        rightOn = "id",
                        how = InnerJoin
                      }
              )
      ]

test_whereExpToLoc :: Test
test_whereExpToLoc =
  "translating SQL WHERE to Pandas Loc"
    ~: TestList
      [ whereExpToLoc (Just $ Op2 (CompVal (ColName "col")) (Comp Eq) (CompVal (LitString "hello")))
          ~?= [Loc (Op2 (CompVal (ColName "col")) (Comp Eq) (CompVal (LitString "hello")))],
        whereExpToLoc Nothing ~?= []
      ]

test_limitExpToHead :: Test
test_limitExpToHead =
  "translating SQL LIMIT to Pandas Head"
    ~: TestList
      [ limitExpToHead (Just 5) ~?= [Head 5],
        limitExpToHead Nothing ~?= []
      ]

test_orderByToSortValues :: Test
test_orderByToSortValues =
  "translating SQL ORDER BY to Pandas Sort Values"
    ~: TestList
      [ orderByToSortValues (Just ("col1", Asc)) ~?= [SortValues "col1" Asc],
        orderByToSortValues Nothing ~?= []
      ]

test_groupByToPandasGroupBy :: Test
test_groupByToPandasGroupBy =
  "translating SQL GROUP BY to Pandas Group By"
    ~: TestList
      [ groupByToPandasGroupBy Nothing ~?= [],
        groupByToPandasGroupBy (Just ["col1", "col2"]) ~?= [Pandas.GroupBy ["col1", "col2"]]
      ]