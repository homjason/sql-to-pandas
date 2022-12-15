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

test_selectExpP :: Test
test_selectExpP =
  "parsing overall SELECT expressions"
    ~: TestList
      [ P.parse selectExpP "select col1"
          ~?= Right (Cols [Col "col1"]),
        P.parse selectExpP "select col1, col2, col3"
          ~?= Right (Cols [Col "col1", Col "col2", Col "col3"]),
        P.parse selectExpP "select distinct col1"
          ~?= Right (DistinctCols [Col "col1"]),
        P.parse selectExpP "select distinct col1, col2, col3"
          ~?= Right (DistinctCols [Col "col1", Col "col2", Col "col3"]),
        P.parse selectExpP "select count(col1)"
          ~?= Right (Cols [SQL.Agg Count "col1"])
      ]

test_colExpP :: Test
test_colExpP =
  "parsing individual columns in SELECT expressions"
    ~: TestList
      [ P.parse colExpP "col" ~?= Right (Col "col"),
        P.parse colExpP "col2       " ~?= Right (Col "col2"),
        P.parse colExpP "max(col)" ~?= Right (Agg Max "col"),
        P.parse colExpP "avg(col1)" ~?= Right (Agg Avg "col1"),
        P.parse colExpP "count(col2)" ~?= Right (Agg Count "col2"),
        -- Check that the names of aggregate functions are
        -- reserved keywords and can't be used as colnames
        P.parse colExpP "max"
          ~?= Left selectErrorMsg,
        P.parse colExpP "min"
          ~?= Left selectErrorMsg,
        P.parse colExpP "avg"
          ~?= Left selectErrorMsg,
        P.parse colExpP "sum"
          ~?= Left selectErrorMsg,
        P.parse colExpP "count"
          ~?= Left selectErrorMsg,
        P.parse colExpP "max(max)"
          ~?= Left selectErrorMsg,
        P.parse colExpP "count(count)"
          ~?= Left selectErrorMsg,
        P.parse colExpP "select col1, max(max), col3"
          ~?= Left selectErrorMsg,
        -- No column specified in aggregate function
        P.parse colExpP "sum()"
          ~?= Left selectErrorMsg,
        P.parse colExpP "min("
          ~?= Left selectErrorMsg,
        -- Invalid function calls
        P.parse colExpP "dsgds(col)" ~?= Left "no parses",
        P.parse colExpP "col(col)" ~?= Left "no parses"
      ]
  where
    selectErrorMsg =
      "Colnames must be non-empty and not \
      \SQL reserved keywords"

-- TODO: fix failing test cases
test_whereExpP :: Test
test_whereExpP =
  "parsing WHERE expressions"
    ~: TestList
      [ P.parse whereExpP "where 1 + 2"
          ~?= Right
            ( Op2
                (CompVal (LitInt 1))
                (Arith Plus)
                (CompVal (LitInt 2))
            ),
        P.parse whereExpP "where (1) + (2)"
          ~?= Right
            ( Op2
                (CompVal (LitInt 1))
                (Arith Plus)
                (CompVal (LitInt 2))
            ),
        P.parse whereExpP "where 1 * 2"
          ~?= Right
            ( Op2
                (CompVal (LitInt 1))
                (Arith Times)
                (CompVal (LitInt 2))
            ),
        P.parse whereExpP "where 1*2"
          ~?= Right
            ( Op2
                (CompVal (LitInt 1))
                (Arith Times)
                (CompVal (LitInt 2))
            ),
        P.parse whereExpP "where     2          *   3"
          ~?= Right
            ( Op2
                (CompVal (LitInt 2))
                (Arith Times)
                (CompVal (LitInt 3))
            ),
        P.parse whereExpP "where 4 / 2"
          ~?= Right
            ( Op2
                (CompVal (LitInt 4))
                (Arith Divide)
                (CompVal (LitInt 2))
            ),
        P.parse whereExpP "where 3 - 2"
          ~?= Right
            ( Op2
                (CompVal (LitInt 3))
                (Arith Minus)
                (CompVal (LitInt 2))
            ),
        P.parse whereExpP "where 1 < 2"
          ~?= Right
            ( Op2
                (CompVal (LitInt 1))
                (Comp Lt)
                (CompVal (LitInt 2))
            ),
        P.parse whereExpP "where col > 0"
          ~?= Right
            ( Op2
                (CompVal (ColName "col"))
                (Comp Gt)
                (CompVal (LitInt 0))
            ),
        P.parse whereExpP "where col < 1"
          ~?= Right
            ( Op2
                (CompVal (ColName "col"))
                (Comp Lt)
                (CompVal (LitInt 1))
            ),
        P.parse whereExpP "where col = \"hello\""
          ~?= Right
            ( Op2
                (CompVal (ColName "col"))
                (Comp Eq)
                (CompVal (LitString "hello"))
            ),
        P.parse whereExpP "where col != \"invalid\""
          ~?= Right
            ( Op2
                (CompVal (ColName "col"))
                (Comp Neq)
                (CompVal (LitString "invalid"))
            ),
        P.parse whereExpP "where col = 5"
          ~?= Right
            ( Op2
                (CompVal (ColName "col"))
                (Comp Eq)
                (CompVal (LitInt 5))
            ),
        P.parse whereExpP "where col != 0"
          ~?= Right
            ( Op2
                (CompVal (ColName "col"))
                (Comp Neq)
                (CompVal (LitInt 0))
            ),
        P.parse whereExpP "where col1 != col2"
          ~?= Right
            ( Op2
                (CompVal (ColName "col1"))
                (Comp Neq)
                (CompVal (ColName "col2"))
            ),
        P.parse whereExpP "where col is null"
          ~?= Right (Op1 (CompVal (ColName "col")) IsNull),
        P.parse whereExpP "where col is not null"
          ~?= Right (Op1 (CompVal (ColName "col")) IsNotNull),
        P.parse whereExpP "where col              is null"
          ~?= Right (Op1 (CompVal (ColName "col")) IsNull),
        P.parse whereExpP "where col > 5"
          ~?= Right
            ( Op2
                (CompVal (ColName "col"))
                (Comp Gt)
                (CompVal (LitInt 5))
            ),
        P.parse whereExpP "where col                <= 5"
          ~?= Right
            ( Op2
                (CompVal (ColName "col"))
                (Comp Le)
                (CompVal (LitInt 5))
            ),
        P.parse whereExpP "where col + 1 = 2"
          ~?= Right
            ( Op2
                (Op2 (CompVal (ColName "col")) (Arith Plus) (CompVal (LitInt 1)))
                (Comp Eq)
                (CompVal (LitInt 2))
            ),
        P.parse whereExpP "where (col1 is null) and (col2 is not null)"
          ~?= Right
            ( Op2
                (Op1 (CompVal (ColName "col1")) IsNull)
                (Logic And)
                (Op1 (CompVal (ColName "col2")) IsNotNull)
            ),
        P.parse whereExpP "where (col1 is null) or (col2 >= 0)"
          ~?= Right
            ( Op2
                (Op1 (CompVal (ColName "col1")) IsNull)
                (Logic Or)
                (Op2 (CompVal (ColName "col2")) (Comp Ge) (CompVal (LitInt 0)))
            ),
        P.parse whereExpP "where (col1 / col2) and (col2 != 0)"
          ~?= Right
            ( Op2
                (Op2 (CompVal (ColName "col1")) (Arith Divide) (CompVal (ColName "col2")))
                (Logic And)
                (Op2 (CompVal (ColName "col2")) (Comp Neq) (CompVal (LitInt 0)))
            ),
        P.parse whereExpP "where col1 + col2 > 3"
          ~?= Right
            ( Op2
                (Op2 (CompVal (ColName "col1")) (Arith Plus) (CompVal (ColName "col2")))
                (Comp Gt)
                (CompVal (LitInt 3))
            ),
        P.parse whereExpP "where col1 - col2 = col3 - col4"
          ~?= Right
            ( Op2
                (Op2 (CompVal (ColName "col1")) (Arith Minus) (CompVal (ColName "col2")))
                (Comp Eq)
                (Op2 (CompVal (ColName "col3")) (Arith Minus) (CompVal (ColName "col4")))
            ),
        P.parse whereExpP "where type = \"savings\" and balance < 100"
          ~?= Right
            ( Op2
                (Op2 (CompVal (ColName "type")) (Comp Eq) (CompVal (LitString "savings")))
                (Logic And)
                (Op2 (CompVal (ColName "balance")) (Comp Lt) (CompVal (LitInt 100)))
            ),
        P.parse whereExpP "where C.balance > 1000 and D.amount > 100"
          ~?= Right
            ( Op2
                ( Op2
                    (CompVal (ColName "C.balance"))
                    (Comp Gt)
                    (CompVal (LitInt 1000))
                )
                (Logic And)
                ( Op2
                    (CompVal (ColName "D.amount"))
                    (Comp Gt)
                    (CompVal (LitInt 100))
                )
            ),
        -- Check that arithmetic operations are
        -- parsed in a left associative manner
        P.parse whereExpP "where 10 * 2 + 1"
          ~?= Right
            ( Op2
                ( Op2
                    (CompVal (LitInt 10))
                    (Arith Times)
                    (CompVal (LitInt 2))
                )
                (Arith Plus)
                (CompVal (LitInt 1))
            ),
        -- Check that parens in arithmetic operations
        -- are parsed correctly
        P.parse whereExpP "where 10 * (2 + 1)"
          ~?= Right
            ( Op2
                (CompVal (LitInt 10))
                (Arith Times)
                ( Op2
                    (CompVal (LitInt 2))
                    (Arith Plus)
                    (CompVal (LitInt 1))
                )
            ),
        -- Should be parsed as "(1 + (10 * 2)) + 100"
        P.parse whereExpP "where 1 + 10 * 2 + 100"
          ~?= Right
            ( Op2
                ( Op2
                    (CompVal (LitInt 1))
                    (Arith Plus)
                    ( Op2
                        (CompVal (LitInt 10))
                        (Arith Times)
                        (CompVal (LitInt 2))
                    )
                )
                (Arith Plus)
                (CompVal (LitInt 100))
            ),
        -- Should be parsed as "1 + ((10 * 2) + 100)"
        P.parse whereExpP "where 1 + (10 * 2 + 100)"
          ~?= Right
            ( Op2
                (CompVal (LitInt 1))
                (Arith Plus)
                ( Op2
                    ( Op2
                        (CompVal (LitInt 10))
                        (Arith Times)
                        (CompVal (LitInt 2))
                    )
                    (Arith Plus)
                    (CompVal (LitInt 100))
                )
            )
      ]

-- GROUP BY Expression Tests
test_groupByP :: Test
test_groupByP =
  "parsing GROUP BY expressions"
    ~: TestList
      [ P.parse groupByP "group by col1" ~?= Right ["col1"],
        P.parse groupByP "group by col1, col2" ~?= Right ["col1", "col2"],
        P.parse groupByP "group by" ~?= Left "Colnames must be non-empty and not SQL reserved keywords",
        P.parse groupByP "hello world" ~?= Left "Parsing results don't satisfy predicate"
      ]

test_orderByP :: Test
test_orderByP =
  "parsing ORDER BY clauses"
    ~: TestList
      [ P.parse orderByP "arbitrary_string"
          ~?= Left "Parsing results don't satisfy predicate",
        P.parse orderByP "order by"
          ~?= Left "Invalid ORDER BY expression",
        P.parse orderByP "order by col0"
          ~?= Left "Invalid ORDER BY expression",
        P.parse orderByP "order by col2 asc"
          ~?= Right ("col2", Asc),
        P.parse orderByP "order by col1 desc"
          ~?= Right ("col1", Desc),
        P.parse orderByP "order by col1 wrongOrder"
          ~?= Left "Invalid ORDER BY expression",
        P.parse orderByP "order by nonexistentCol wrongOrder"
          ~?= Left "Invalid ORDER BY expression",
        P.parse orderByP "order by col1, col2 asc"
          ~?= Left "Invalid ORDER BY expression",
        P.parse orderByP "order by col1, col2, col3 desc"
          ~?= Left "Invalid ORDER BY expression"
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

-- >>> runTestTT test_P.parse fromExpP
-- Counts {cases = 3, tried = 3, errors = 0, failures = 0}

test_fromExpP :: Test
test_fromExpP =
  "parsing FROM expressions"
    ~: TestList
      [ P.parse fromExpP "from A" ~?= Right (Table "A"),
        P.parse fromExpP "from a join b on a.col = b.col"
          ~?= Right
            ( TableJoin $
                Join
                  { leftTable = "a",
                    leftCol = "col",
                    rightTable = "b",
                    rightCol = "col",
                    style = InnerJoin
                  }
            ),
        P.parse fromExpP "from df1 left join df2 on df1.col1 = df2.col2"
          ~?= Right
            ( TableJoin $
                Join
                  { leftTable = "df1",
                    leftCol = "col1",
                    rightTable = "df2",
                    rightCol = "col2",
                    style = LeftJoin
                  }
            )
            -- P.parse fromExpP "from (select col from B)"
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

test_joinExpP :: Test
test_joinExpP =
  "parsing join expressions"
    ~: TestList
      [ P.parse joinExpP "a join b on a.col = b.col"
          ~?= Right
            ( Join
                { leftTable = "a",
                  leftCol = "col",
                  rightTable = "b",
                  rightCol = "col",
                  style = InnerJoin
                }
            ),
        P.parse joinExpP "a left join b on a.col = b.col"
          ~?= Right
            ( Join
                { leftTable = "a",
                  leftCol = "col",
                  rightTable = "b",
                  rightCol = "col",
                  style = LeftJoin
                }
            ),
        P.parse joinExpP "a right join b on a.col = b.col"
          ~?= Right
            ( Join
                { leftTable = "a",
                  leftCol = "col",
                  rightTable = "b",
                  rightCol = "col",
                  style = RightJoin
                }
            ),
        P.parse joinExpP "a left join b"
          ~?= Left "No join condition specified",
        P.parse joinExpP "a join b on c.col = d.col"
          ~?= Left "Tables being JOINed != tables being selected FROM",
        P.parse joinExpP "a join a on a.col = a.col"
          ~?= Left "Can't join the same table with itself",
        P.parse joinExpP "a join a on a.col = b.col"
          ~?= Left "Can't join the same table with itself",
        P.parse joinExpP "a join b"
          ~?= Left "No join condition specified",
        P.parse joinExpP "a join b on a.col = b.col arbitrarySuffix"
          ~?= Left "Invalid JOIN expression",
        P.parse joinExpP "a invalidJoin b on a.col = b.col"
          ~?= Left "Parsing results don't satisfy predicate",
        P.parse joinExpP "a join b on aCol = bCol"
          ~?= Left "Malformed JOIN condition",
        P.parse joinExpP "a join b on c"
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
