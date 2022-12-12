module UnitTests where

import Data.Set (Set)
import Data.Set qualified as Set
import Parser
import Parser qualified as P
-- import Print
import Print (PP (pp))
import SQLParser
import Test.HUnit (Counts, Test (..), runTestTT, (~:), (~?=))
import Test.QuickCheck qualified as QC
import Text.PrettyPrint qualified as PP
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
              { select = Cols [Col "col1"],
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
              },
        parseQuery "SELECT col1, col2\nFROM table1 JOIN table2 ON table1.col1 = table2.col1"
          ~?= Right
            Query
              { select = Cols [Col "col1", Col "col2"],
                from = Table "table1" (Just $ Join "table1" "col1" "table2" "col1" InnerJoin),
                wher = Nothing,
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

--------------------------------------------------------------------------------
-- Unit Tests for validateQuery & its helper functions

-- >>> runTestTT test_validateQuery
-- Counts {cases = 8, tried = 8, errors = 0, failures = 0}

test_validateQuery :: Test
test_validateQuery =
  "checking SQL query validation"
    ~: TestList
      [ validateQuery (mkQuery (Cols []) df (SQL.GroupBy []))
          ~?= Left "No columns specified in SELECT expression",
        validateQuery (mkQuery (Cols [Col "col1"]) df (SQL.GroupBy []))
          ~?= Left "Columns not specified in GROUP BY",
        validateQuery (mkQuery Star df (SQL.GroupBy ["col1"]))
          ~?= Left "SELECT * not allowed in queries involving GROUP BY",
        validateQuery (mkQuery Star df (SQL.GroupBy []))
          ~?= Left "Columns not specified in GROUP BY",
        validateQuery
          ( mkQuery
              (Cols [Col "col1"])
              df
              (SQL.GroupBy ["col2"])
          )
          ~?= Left "Columns in SELECT expression /= columns in GROUP BY",
        validateQuery
          ( mkQuery
              (Cols [Col "col1", Col "col2"])
              df
              (SQL.GroupBy ["col1"])
          )
          ~?= Left "Columns in SELECT expression /= columns in GROUP BY",
        validateQuery
          ( mkQuery
              (Cols [Agg Count "col1", Col "col2"])
              df
              (SQL.GroupBy ["col1"])
          )
          ~?= Left "Columns in SELECT expression /= columns in GROUP BY",
        validateQuery
          ( mkQuery
              (Cols [Agg Count "col1", Agg Count "col2"])
              df
              (SQL.GroupBy ["col1", "col2"])
          )
          ~?= Left "Columns in SELECT expression /= columns in GROUP BY"
      ]
  where
    df = Table "df" Nothing

test_getNonAggCols :: Test
test_getNonAggCols =
  "checking that correct non-aggregated cols in SELECT expressions are retrieved"
    ~: TestList
      [ (getNonAggCols . decompColExps) []
          ~?= Set.empty,
        (getNonAggCols . decompColExps)
          [Col "col1", Col "col2"]
          ~?= Set.fromList ["col1", "col2"],
        (getNonAggCols . decompColExps) [Col "col1", Agg Count "col2"]
          ~?= Set.singleton "col1",
        (getNonAggCols . decompColExps) [Agg Count "col1"]
          ~?= Set.empty
      ]

test_getAggCols :: Test
test_getAggCols =
  "checking that correct aggregated cols in SELECT expressions are retrieved"
    ~: TestList
      [ (getAggCols . decompColExps) [] ~?= Set.empty,
        (getAggCols . decompColExps) [Col "col1"] ~?= Set.empty,
        (getAggCols . decompColExps) [Col "col1", Col "col2"] ~?= Set.empty,
        (getAggCols . decompColExps) [Col "col1", Col "col2"] ~?= Set.empty,
        (getAggCols . decompColExps) [Col "col1", Agg Count "col2"]
          ~?= Set.singleton "col2",
        (getAggCols . decompColExps) [Agg Avg "col1", Agg Count "col2"]
          ~?= Set.fromList ["col1", "col2"]
      ]

test_selectExpIsNonEmpty :: Test
test_selectExpIsNonEmpty =
  "checking that SELECT expressions are non-empty"
    ~: TestList
      [ selectExpIsNonEmpty Star ~?= Right True,
        selectExpIsNonEmpty (Cols [Col "col1"]) ~?= Right True,
        selectExpIsNonEmpty (Cols [Col "col1", Col "col2"]) ~?= Right True,
        selectExpIsNonEmpty (DistinctCols [Col "col1"]) ~?= Right True,
        selectExpIsNonEmpty (DistinctCols [Col "col1", Col "Col2"]) ~?= Right True,
        selectExpIsNonEmpty (Cols []) ~?= Left "No columns specified in SELECT expression",
        selectExpIsNonEmpty (DistinctCols []) ~?= Left "No columns specified in SELECT DISTINCT expression"
      ]

test_groupByColsInSelectExp :: Test
test_groupByColsInSelectExp =
  "checking if cols in GROUP BY = cols in SELECT expression"
    ~: TestList
      [ groupByColsInSelectExp (Query Star df Nothing Nothing Nothing Nothing)
          ~?= Right True,
        groupByColsInSelectExp (mkQuery Star df (SQL.GroupBy []))
          ~?= Left "Columns not specified in GROUP BY",
        groupByColsInSelectExp (mkQuery Star df (SQL.GroupBy ["col1", "col2"]))
          ~?= Left "SELECT * not allowed in queries involving GROUP BY",
        groupByColsInSelectExp
          ( mkQuery
              (DistinctCols [])
              df
              (SQL.GroupBy ["col1", "col2"])
          )
          ~?= Left "Can't have SELECT DISTINCT & GROUP BY in the same query",
        groupByColsInSelectExp
          ( mkQuery
              (DistinctCols [Col "col1"])
              df
              (SQL.GroupBy ["col1", "col2"])
          )
          ~?= Left "Can't have SELECT DISTINCT & GROUP BY in the same query",
        groupByColsInSelectExp
          ( mkQuery
              (Cols [Col "col1"])
              df
              (SQL.GroupBy ["col1", "col2"])
          )
          ~?= Left "Columns in SELECT expression /= columns in GROUP BY",
        groupByColsInSelectExp
          ( mkQuery
              (Cols [Col "col1", Col "col2"])
              df
              (SQL.GroupBy ["col1", "col2"])
          )
          ~?= Right True,
        groupByColsInSelectExp
          ( mkQuery
              (Cols [Col "col2", Col "col1"])
              df
              (SQL.GroupBy ["col1", "col2"])
          )
          ~?= Right True,
        groupByColsInSelectExp
          ( mkQuery
              (Cols [Col "col1", Agg Count "col2"])
              df
              (SQL.GroupBy ["col1", "col2"])
          )
          ~?= Right True,
        groupByColsInSelectExp
          ( mkQuery
              (Cols [Agg Avg "col1", Agg Count "col2"])
              df
              (SQL.GroupBy ["col1", "col2"])
          )
          ~?= Right True
      ]
  where
    df = Table "df" Nothing

test_distinctHasNoAggFuncs :: Test
test_distinctHasNoAggFuncs =
  "checking that DISTINCT doesn't coincide w/ aggregate functions"
    ~: TestList
      [ distinctHasNoAggFuncs (DistinctCols [Agg Count "col1"])
          ~?= Left "Can't have aggregate functions in SELECT DISTINCT expression",
        distinctHasNoAggFuncs (DistinctCols [Col "col1", Agg Count "col2"])
          ~?= Left "Can't have aggregate functions in SELECT DISTINCT expression",
        distinctHasNoAggFuncs (DistinctCols [Agg Count "col1", Agg Count "col2"])
          ~?= Left "Can't have aggregate functions in SELECT DISTINCT expression",
        distinctHasNoAggFuncs (DistinctCols [Col "col1", Col "col2"])
          ~?= Right True,
        distinctHasNoAggFuncs (DistinctCols [])
          ~?= Right True,
        distinctHasNoAggFuncs (DistinctCols [Col "col1", Col "col2", Col "col3"])
          ~?= Right True
      ]

test_noDistinctAndGroupBy :: Test
test_noDistinctAndGroupBy =
  "checking that DISTINCTs & GROUP BYs never appear together"
    ~: TestList
      [ noDistinctAndGroupBy (DistinctCols [Col "col1"]) (Just ["col1"])
          ~?= Left "Can't have DISTINCT & GROUP BY in the same query",
        noDistinctAndGroupBy (DistinctCols [Col "col1"]) (Just [])
          ~?= Left "Can't have DISTINCT & GROUP BY in the same query",
        noDistinctAndGroupBy (DistinctCols [Col "col1"]) Nothing ~?= Right True,
        noDistinctAndGroupBy (Cols [Col "Col1"]) Nothing ~?= Right True,
        noDistinctAndGroupBy Star Nothing ~?= Right True
      ]

--------------------------------------------------------------------------------
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

test_translateSQL :: Test
test_translateSQL =
  "translate SQL query to Pandas command"
    ~: TestList
      [ translateSQL
          ( Query
              { select = Cols [Col "col"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= Command
            { df = "table",
              cols = Just ["col"],
              fn = Nothing
            },
        translateSQL
          ( Query
              { select = Cols [Col "col"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Just 5
              }
          )
          ~?= Command
            { df = "table",
              cols = Just ["col"],
              fn = Just [Head 5]
            },
        translateSQL
          ( Query
              { select = Cols [Col "col1", Agg Count "col2"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Just ["col1"],
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= Command
            { df = "table",
              cols = Just ["col1", "col2"],
              fn = Just [Pandas.GroupBy ["col1"], Aggregate Count "col2", ResetIndex]
            },
        translateSQL
          ( Query
              { select = Cols [Col "col"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Just ("col", Asc),
                limit = Nothing
              }
          )
          ~?= Command
            { df = "table",
              cols = Just ["col"],
              fn = Just [SortValues "col" Asc]
            },
        translateSQL
          ( Query
              { select = Cols [Col "col", Col "col2"],
                from = Table "table" Nothing,
                wher = Just $ Op2 (CompVal $ ColName "col") (Comp Gt) (CompVal $ LitInt 4),
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= Command
            { df = "table",
              cols = Just ["col", "col2"],
              fn = Just [Loc $ Op2 (CompVal $ ColName "col") (Comp Gt) (CompVal $ LitInt 4)]
            },
        translateSQL
          ( Query
              { select = Cols [Col "col1", Col "col2"],
                from = Table "table1" (Just $ Join "table1" "col1" "table2" "col1" InnerJoin),
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= Command
            { df = "table1",
              cols = Just ["col1", "col2"],
              fn =
                Just
                  [ Merge
                      MkMerge
                        { rightDf = "table2",
                          leftOn = "col1",
                          rightOn = "col1",
                          how = InnerJoin
                        }
                  ]
            }
      ]

test_getFuncs :: Test
test_getFuncs =
  "translate SQL functions to Pandas functions"
    ~: TestList
      [ getFuncs
          ( Query
              { select = Cols [Col "col"],
                from = Table "table" Nothing,
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= Nothing
      ]

-- TODO: add more test cases for runParseAndTranslate (SQLParser.hs)
test_runParseAndTranslate :: Test
test_runParseAndTranslate =
  "Testing wrapper function that takes care of Parsing + Translating"
    ~: TestList
      [ runParseAndTranslate "SELECT col1, COUNT(col2)\nFROM table\nGROUP BY col1"
          ~?= Right
            ( Command
                "table"
                (Just ["col1", "col2"])
                (Just [Pandas.GroupBy ["col1"], Aggregate Count "col2"])
            )
      ]

--------------------------------------------------------------------------------
-- PRINT unit tests
test_printPandasCommands :: Test
test_printPandasCommands =
  "pretty printing Pandas commands"
    ~: TestList
      [ pp (Command "table" (Just ["col"]) Nothing) ~?= PP.text "table[\"col\"]",
        pp (Command "table" (Just ["col"]) (Just [Head 5])) ~?= PP.text "table[\"col\"].head(5)",
        pp (Pandas.Command "table" (Just ["col1", "col2"]) (Just [Pandas.GroupBy ["col1"], Pandas.Aggregate Count "col2", Pandas.ResetIndex])) ~?= PP.text "table[\"col1\",\"col2\"].groupBy(by=[\"col1\"]).agg({\"col2\":\"count\"}).reset_index()",
        pp (Pandas.Command "table" (Just ["col"]) (Just [Pandas.SortValues "col" Asc])) ~?= PP.text "table[\"col\"].sort_values(by=[\"col\"], ascending=True)",
        pp
          ( Command
              { df = "table1",
                cols = Just ["col1", "col2"],
                fn =
                  Just
                    [ Merge
                        MkMerge
                          { rightDf = "table2",
                            leftOn = "col1",
                            rightOn = "col1",
                            how = InnerJoin
                          }
                    ]
              }
          )
          ~?= PP.text "table1[\"col1\",\"col2\"].merge(table2, left_on=\"col1\", right_on=\"col1\", how=\"inner\")"
      ]
