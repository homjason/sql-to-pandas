module UnitTests where

import Data.Array (Array, array)
import Data.Set (Set)
import Data.Set qualified as Set
import Parser
import Parser qualified as P
-- import Print
import Print (PP (pp))
import SQLParser
import Test.HUnit (Counts, Test (..), runTestTT, runTestTTAndExit, (~:), (~?=))
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
      [ P.parse comparableP "col" ~?= Right (SQL.ColName "col"),
        P.parse comparableP "52" ~?= Right (SQL.LitInt 52),
        P.parse comparableP "-4" ~?= Right (SQL.LitInt (-4)),
        P.parse comparableP "100       " ~?= Right (SQL.LitInt 100),
        P.parse comparableP "\"string literal\"" ~?= Right (SQL.LitString "string literal")
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

-- TODO: fix the two failing test cases
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
      [ P.parse whereExpP "where 1 + true"
          ~?= Left "Parsing results don't satisfy predicate",
        P.parse whereExpP "where 1 + 2"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.LitInt 1))
                (SQL.Arith Plus)
                (SQL.CompVal (SQL.LitInt 2))
            ),
        P.parse whereExpP "where (1) + (2)"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.LitInt 1))
                (SQL.Arith Plus)
                (SQL.CompVal (SQL.LitInt 2))
            ),
        P.parse whereExpP "where 1 * 2"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.LitInt 1))
                (SQL.Arith Times)
                (SQL.CompVal (SQL.LitInt 2))
            ),
        P.parse whereExpP "where 1*2"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.LitInt 1))
                (SQL.Arith Times)
                (SQL.CompVal (SQL.LitInt 2))
            ),
        P.parse whereExpP "where     2          *   3"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.LitInt 2))
                (SQL.Arith Times)
                (SQL.CompVal (SQL.LitInt 3))
            ),
        P.parse whereExpP "where 4 / 2"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.LitInt 4))
                (SQL.Arith Divide)
                (SQL.CompVal (SQL.LitInt 2))
            ),
        P.parse whereExpP "where 3 - 2"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.LitInt 3))
                (SQL.Arith Minus)
                (SQL.CompVal (SQL.LitInt 2))
            ),
        P.parse whereExpP "where 1 < 2"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.LitInt 1))
                (SQL.Comp SQL.Lt)
                (SQL.CompVal (SQL.LitInt 2))
            ),
        P.parse whereExpP "where col > 0"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.ColName "col"))
                (SQL.Comp SQL.Gt)
                (SQL.CompVal (SQL.LitInt 0))
            ),
        P.parse whereExpP "where col < 1"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.ColName "col"))
                (SQL.Comp SQL.Lt)
                (SQL.CompVal (SQL.LitInt 1))
            ),
        P.parse whereExpP "where col = \"hello\""
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.ColName "col"))
                (SQL.Comp SQL.Eq)
                (SQL.CompVal (SQL.LitString "hello"))
            ),
        P.parse whereExpP "where col != \"invalid\""
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.ColName "col"))
                (SQL.Comp SQL.Neq)
                (SQL.CompVal (SQL.LitString "invalid"))
            ),
        P.parse whereExpP "where col = 5"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.ColName "col"))
                (SQL.Comp SQL.Eq)
                (SQL.CompVal (SQL.LitInt 5))
            ),
        P.parse whereExpP "where col != 0"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.ColName "col"))
                (SQL.Comp SQL.Neq)
                (SQL.CompVal (SQL.LitInt 0))
            ),
        P.parse whereExpP "where col1 != col2"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.ColName "col1"))
                (SQL.Comp SQL.Neq)
                (SQL.CompVal (SQL.ColName "col2"))
            ),
        P.parse whereExpP "where col is null"
          ~?= Right (SQL.Op1 (SQL.CompVal (SQL.ColName "col")) SQL.IsNull),
        P.parse whereExpP "where col is not null"
          ~?= Right (SQL.Op1 (SQL.CompVal (SQL.ColName "col")) SQL.IsNotNull),
        P.parse whereExpP "where col              is null"
          ~?= Right (SQL.Op1 (SQL.CompVal (SQL.ColName "col")) SQL.IsNull),
        P.parse whereExpP "where col > 5"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.ColName "col"))
                (SQL.Comp SQL.Gt)
                (SQL.CompVal (SQL.LitInt 5))
            ),
        P.parse whereExpP "where col                <= 5"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.ColName "col"))
                (SQL.Comp SQL.Le)
                (SQL.CompVal (SQL.LitInt 5))
            ),
        P.parse whereExpP "where col + 1 = 2"
          ~?= Right
            ( SQL.Op2
                (SQL.Op2 (SQL.CompVal (SQL.ColName "col")) (SQL.Arith Plus) (SQL.CompVal (SQL.LitInt 1)))
                (SQL.Comp SQL.Eq)
                (SQL.CompVal (SQL.LitInt 2))
            ),
        P.parse whereExpP "where (col1 is null) and (col2 is not null)"
          ~?= Right
            ( SQL.Op2
                (SQL.Op1 (SQL.CompVal (SQL.ColName "col1")) SQL.IsNull)
                (SQL.Logic SQL.And)
                (SQL.Op1 (SQL.CompVal (SQL.ColName "col2")) SQL.IsNotNull)
            ),
        P.parse whereExpP "where (col1 is null) or (col2 >= 0)"
          ~?= Right
            ( SQL.Op2
                (SQL.Op1 (SQL.CompVal (SQL.ColName "col1")) SQL.IsNull)
                (SQL.Logic SQL.Or)
                (SQL.Op2 (SQL.CompVal (SQL.ColName "col2")) (SQL.Comp SQL.Ge) (SQL.CompVal (SQL.LitInt 0)))
            ),
        P.parse whereExpP "where (col1 / col2) and (col2 != 0)"
          ~?= Right
            ( SQL.Op2
                (SQL.Op2 (SQL.CompVal (SQL.ColName "col1")) (SQL.Arith Divide) (SQL.CompVal (SQL.ColName "col2")))
                (SQL.Logic SQL.And)
                (SQL.Op2 (SQL.CompVal (SQL.ColName "col2")) (SQL.Comp SQL.Neq) (SQL.CompVal (SQL.LitInt 0)))
            ),
        P.parse whereExpP "where col1 + col2 > 3"
          ~?= Right
            ( SQL.Op2
                (SQL.Op2 (SQL.CompVal (SQL.ColName "col1")) (SQL.Arith Plus) (SQL.CompVal (SQL.ColName "col2")))
                (SQL.Comp SQL.Gt)
                (SQL.CompVal (SQL.LitInt 3))
            ),
        P.parse whereExpP "where col1 - col2 = col3 - col4"
          ~?= Right
            ( SQL.Op2
                (SQL.Op2 (SQL.CompVal (SQL.ColName "col1")) (SQL.Arith Minus) (SQL.CompVal (SQL.ColName "col2")))
                (SQL.Comp SQL.Eq)
                (SQL.Op2 (SQL.CompVal (SQL.ColName "col3")) (SQL.Arith Minus) (SQL.CompVal (SQL.ColName "col4")))
            ),
        P.parse whereExpP "where type = \"savings\" and balance < 100"
          ~?= Right
            ( SQL.Op2
                (SQL.Op2 (SQL.CompVal (SQL.ColName "type")) (SQL.Comp SQL.Eq) (SQL.CompVal (SQL.LitString "savings")))
                (SQL.Logic SQL.And)
                (SQL.Op2 (SQL.CompVal (SQL.ColName "balance")) (SQL.Comp SQL.Lt) (SQL.CompVal (SQL.LitInt 100)))
            ),
        P.parse whereExpP "where C.balance > 1000 and D.amount > 100"
          ~?= Right
            ( SQL.Op2
                ( SQL.Op2
                    (SQL.CompVal (SQL.ColName "C.balance"))
                    (SQL.Comp SQL.Gt)
                    (SQL.CompVal (SQL.LitInt 1000))
                )
                (SQL.Logic SQL.And)
                ( SQL.Op2
                    (SQL.CompVal (SQL.ColName "D.amount"))
                    (SQL.Comp SQL.Gt)
                    (SQL.CompVal (SQL.LitInt 100))
                )
            ),
        -- Check that arithmetic operations are
        -- parsed in a left associative manner
        P.parse whereExpP "where 10 * 2 + 1"
          ~?= Right
            ( SQL.Op2
                ( SQL.Op2
                    (SQL.CompVal (SQL.LitInt 10))
                    (SQL.Arith Times)
                    (SQL.CompVal (SQL.LitInt 2))
                )
                (SQL.Arith Plus)
                (SQL.CompVal (SQL.LitInt 1))
            ),
        -- Check that parens in arithmetic operations
        -- are parsed correctly
        P.parse whereExpP "where 10 * (2 + 1)"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.LitInt 10))
                (SQL.Arith Times)
                ( SQL.Op2
                    (SQL.CompVal (SQL.LitInt 2))
                    (SQL.Arith Plus)
                    (SQL.CompVal (SQL.LitInt 1))
                )
            ),
        -- Should be parsed as "(1 + (10 * 2)) + 100"
        P.parse whereExpP "where 1 + 10 * 2 + 100"
          ~?= Right
            ( SQL.Op2
                ( SQL.Op2
                    (SQL.CompVal (SQL.LitInt 1))
                    (SQL.Arith Plus)
                    ( SQL.Op2
                        (SQL.CompVal (SQL.LitInt 10))
                        (SQL.Arith Times)
                        (SQL.CompVal (SQL.LitInt 2))
                    )
                )
                (SQL.Arith Plus)
                (SQL.CompVal (SQL.LitInt 100))
            ),
        -- Should be parsed as "1 + ((10 * 2) + 100)"
        P.parse whereExpP "where 1 + (10 * 2 + 100)"
          ~?= Right
            ( SQL.Op2
                (SQL.CompVal (SQL.LitInt 1))
                (SQL.Arith Plus)
                ( SQL.Op2
                    ( SQL.Op2
                        (SQL.CompVal (SQL.LitInt 10))
                        (SQL.Arith Times)
                        (SQL.CompVal (SQL.LitInt 2))
                    )
                    (SQL.Arith Plus)
                    (SQL.CompVal (SQL.LitInt 100))
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

-- >>> runTestTT test_parseQuery

test_parseQuery :: Test
test_parseQuery =
  "parsing SQL Queries"
    ~: TestList
      [ parseQuery "SELECT col\nFROM table"
          ~?= Right
            Query
              { select = Cols [Col "col"],
                from = Table "table",
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              },
        parseQuery "SELECT col1\nFROM table\nLIMIT 5"
          ~?= Right
            Query
              { select = Cols [Col "col1"],
                from = Table "table",
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Just 5
              },
        parseQuery "SELECT col1, COUNT(col2)\nFROM table\nGROUP BY col1"
          ~?= Right
            Query
              { select = Cols [Col "col1", Agg Count "col2"],
                from = Table "table",
                wher = Nothing,
                groupBy = Just ["col1"],
                orderBy = Nothing,
                limit = Nothing
              },
        parseQuery "SELECT col1, COUNT(col2)\n     FROM table   \n      GROUP BY col1"
          ~?= Right
            Query
              { select = Cols [Col "col1", Agg Count "col2"],
                from = Table "table",
                wher = Nothing,
                groupBy = Just ["col1"],
                orderBy = Nothing,
                limit = Nothing
              },
        parseQuery "SELECT col\nFROM table\nORDER BY col ASC"
          ~?= Right
            Query
              { select = Cols [Col "col"],
                from = Table "table",
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Just ("col", Asc),
                limit = Nothing
              },
        parseQuery "SELECT col, col2\nFROM table\nWHERE col > 4"
          ~?= Right
            Query
              { select = Cols [Col "col", Col "col2"],
                from = Table "table",
                wher = Just $ SQL.Op2 (SQL.CompVal $ SQL.ColName "col") (SQL.Comp SQL.Gt) (SQL.CompVal $ SQL.LitInt 4),
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              },
        parseQuery "SELECT col1, col2\nFROM table1 JOIN table2 ON table1.col1 = table2.col1"
          ~?= Right
            Query
              { select = Cols [Col "col1", Col "col2"],
                from = TableJoin $ Join "table1" "col1" "table2" "col1" InnerJoin,
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              },
        parseQuery "select col1 \n from table \n where col1 > 0\n group by col1 \n order by col1 asc \n limit 5"
          ~?= Right
            ( Query
                { select = Cols [Col "col1"],
                  from = Table "table",
                  wher = Just (SQL.Op2 (SQL.CompVal (SQL.ColName "col1")) (SQL.Comp SQL.Gt) (SQL.CompVal (SQL.LitInt 0))),
                  groupBy = Just ["col1"],
                  orderBy = Just ("col1", Asc),
                  limit = Just 5
                }
            )
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
          ~?= Left "Columns in SELECT expression /= columns in GROUP BY",
        validateQuery
          (Query (Cols [Col "col1"]) (Table "df") Nothing (Just ["col1"]) Nothing Nothing)
          ~?= Right True,
        validateQuery
          ( mkQuery
              (Cols [Col "col1", Agg Count "col2"])
              df
              (SQL.GroupBy ["col1"])
          )
          ~?= Right True,
        validateQuery
          ( mkQuery
              (Cols [Col "col1", Agg Count "col2"])
              df
              (SQL.GroupBy ["col1"])
          )
          ~?= Right True,
        validateQuery
          ( mkQuery
              (DistinctCols [Col "col1"])
              df
              (SQL.GroupBy ["col1"])
          )
          ~?= Left "Can't have aggregate functions in SELECT DISTINCT expression",
        validateQuery
          ( mkQuery
              (Cols [Col "col1"])
              df
              (SQL.GroupBy ["col1"])
          )
          ~?= Right True
      ]
  where
    df = Table "df"

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
    df = Table "df"

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
      from = Table "df",
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
      [ selectExpToCols Star ~?= ([], Nothing), -- ([] :: [ColName], Nothing),
        selectExpToCols (Cols [Col "colA", Col "colB"]) ~?= (["colA", "colB"], Nothing),
        selectExpToCols (DistinctCols [Col "colA", Col "colB"])
          ~?= (["colA", "colB"], Just [Unique ["colA", "colB"]]),
        selectExpToCols (Cols [Col "colA", Agg Count "colB"])
          ~?= (["colA", "colB"], Just [Aggregate Count "colB"]),
        selectExpToCols (Cols []) ~?= ([], Nothing)
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
      [ translateFromExp (Table "A") ~?= ("A", Nothing),
        translateFromExp (TableJoin $ Join "A" "id" "B" "id" InnerJoin)
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
      [ whereExpToLoc (Just $ SQL.Op2 (SQL.CompVal (SQL.ColName "col")) (SQL.Comp SQL.Eq) (SQL.CompVal (SQL.LitString "hello"))) "table"
          ~?= [Loc (Pandas.Op2 (Pandas.CompVal (Pandas.ColName "col" "table")) (Pandas.Comp Pandas.Eq) (Pandas.CompVal (Pandas.LitString "hello")))],
        whereExpToLoc Nothing "table" ~?= []
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
        groupByToPandasGroupBy (Just ["col1", "col2"]) ~?= [Group ["col1", "col2"], ResetIndex]
      ]

test_translateSQL :: Test
test_translateSQL =
  "translate SQL query to Pandas command"
    ~: TestList
      [ translateSQL
          ( Query
              { select = Cols [Col "col"],
                from = Table "table",
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
                from = Table "table",
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
                from = Table "table",
                wher = Nothing,
                groupBy = Just ["col1"],
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= Command
            { df = "table",
              cols = Just ["col1", "col2"],
              fn = Just [Group ["col1"], Aggregate Count "col2", ResetIndex]
            },
        translateSQL
          ( Query
              { select = Cols [Col "col"],
                from = Table "table",
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
                from = Table "table",
                wher = Just $ SQL.Op2 (SQL.CompVal $ SQL.ColName "col") (SQL.Comp SQL.Gt) (SQL.CompVal $ SQL.LitInt 4),
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= Command
            { df = "table",
              cols = Just ["col", "col2"],
              fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.ColName "col" "table") (Pandas.Comp Pandas.Gt) (Pandas.CompVal $ Pandas.LitInt 4)]
            },
        translateSQL
          ( Query
              { select = Cols [Col "col1", Col "col2"],
                from = TableJoin (Join "table1" "col1" "table2" "col1" InnerJoin),
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
                from = Table "table",
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              }
          )
          "table"
          ~?= Nothing
      ]

------------------------------------------------------------------------------
-- PRINT unit tests

test_printPandasCommands :: Test
test_printPandasCommands =
  "pretty printing Pandas commands"
    ~: TestList
      [ pp (Command "table" (Just ["col"]) Nothing) ~?= PP.text "table[\"col\"]",
        pp (Command "table" (Just ["col"]) (Just [Head 5])) ~?= PP.text "table[\"col\"].head(5)",
        pp (Pandas.Command "table" (Just ["col1", "col2"]) (Just [Group ["col1"], Pandas.Aggregate Count "col2", Pandas.ResetIndex])) ~?= PP.text "table[\"col1\",\"col2\"].groupBy(by=[\"col1\"]).agg({\"col2\":\"count\"}).reset_index()",
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
          ~?= PP.text
            "table1[\"col1\",\"col2\"].merge(table2, left_on=\"col1\", right_on=\"col1\", how=\"inner\")",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.ColName "col" "table") (Pandas.Comp Pandas.Gt) (Pandas.CompVal $ Pandas.LitInt 4)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[table[\"col\"] > 4]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.ColName "col" "table") (Pandas.Comp Pandas.Ge) (Pandas.CompVal $ Pandas.LitInt 4)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[table[\"col\"] >= 4]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.ColName "col" "table") (Pandas.Comp Pandas.Eq) (Pandas.CompVal $ Pandas.LitInt 4)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[table[\"col\"] == 4]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.ColName "col" "table") (Pandas.Comp Pandas.Neq) (Pandas.CompVal $ Pandas.LitInt 4)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[table[\"col\"] != 4]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.ColName "col" "table") (Pandas.Comp Pandas.Lt) (Pandas.CompVal $ Pandas.LitInt 4)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[table[\"col\"] < 4]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.ColName "col" "table") (Pandas.Comp Pandas.Le) (Pandas.CompVal $ Pandas.LitInt 4)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[table[\"col\"] <= 4]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.Op1 (Pandas.CompVal $ Pandas.ColName "col" "table") Pandas.IsNull) (Pandas.Logic Pandas.And) (Pandas.Op2 (Pandas.CompVal $ Pandas.ColName "col" "table") (Pandas.Comp Pandas.Gt) (Pandas.CompVal $ Pandas.LitInt 4))]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[table[\"col\"].isnull() & table[\"col\"] > 4]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.Op1 (Pandas.CompVal $ Pandas.ColName "col" "table") Pandas.IsNotNull) (Pandas.Logic Pandas.And) (Pandas.Op2 (Pandas.CompVal $ Pandas.ColName "col" "table") (Pandas.Comp Pandas.Gt) (Pandas.CompVal $ Pandas.LitInt 4))]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[table[\"col\"].notnull() & table[\"col\"] > 4]",
        pp (Command {df = "table", cols = Just ["col", "col2"], fn = Just [Pandas.Unique ["col", "col2"]]}) ~?= PP.text "table[\"col\",\"col2\"].drop_duplicates(subset=[\"col\",\"col2\"])",
        pp (Command {df = "table", cols = Just ["col", "col2"], fn = Just [Pandas.Head 5]}) ~?= PP.text "table[\"col\",\"col2\"].head(5)",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.LitInt 2) (Pandas.Arith Plus) (Pandas.CompVal $ Pandas.LitInt 0)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[2 + 0]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.LitInt 2) (Pandas.Arith Minus) (Pandas.CompVal $ Pandas.LitInt 0)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[2 - 0]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.LitInt 2) (Pandas.Arith Times) (Pandas.CompVal $ Pandas.LitInt 0)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[2 * 0]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.LitInt 0) (Pandas.Arith Divide) (Pandas.CompVal $ Pandas.LitInt 2)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[0 / 2]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.LitInt 2) (Pandas.Arith Modulo) (Pandas.CompVal $ Pandas.LitInt 2)]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[2 % 2]",
        pp
          ( Command
              { df = "table",
                cols = Just ["col", "col2"],
                fn = Just [Loc $ Pandas.Op2 (Pandas.CompVal $ Pandas.LitInt 2) (Pandas.Arith Plus) (Pandas.CompVal $ Pandas.LitInt 0), Pandas.Head 6]
              }
          )
          ~?= PP.text "table[\"col\",\"col2\"].loc[2 + 0].head(6)"
      ]

test_printSQLQuery :: Test
test_printSQLQuery =
  "pretty printing SQL Queries"
    ~: TestList
      [ pp
          ( Query
              { select = Cols [Col "col"],
                from = Table "table",
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= PP.text "select col from table",
        pp
          ( Query
              { select = Cols [Col "col", Col "col2"],
                from = Table "table",
                wher = Just $ SQL.Op2 (SQL.CompVal $ SQL.ColName "col") (SQL.Comp SQL.Gt) (SQL.CompVal $ SQL.LitInt 4),
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= PP.text "select col,col2 from table where col > 4",
        pp
          ( Query
              { select = Cols [Col "col1", Agg Count "col2"],
                from = Table "table",
                wher = Nothing,
                groupBy = Just ["col1"],
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= PP.text "select col1,count(col2) from table group by col1",
        pp
          ( Query
              { select = Cols [Col "col1", Col "col2", Agg Count "col3"],
                from = Table "table",
                wher = Nothing,
                groupBy = Just ["col1", "col2"],
                orderBy = Nothing,
                limit = Nothing
              }
          )
          ~?= PP.text "select col1,col2,count(col3) from table group by col1,col2",
        pp
          ( Query
              { select = Cols [Col "col"],
                from = Table "table",
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Just ("col", Asc),
                limit = Nothing
              }
          )
          ~?= PP.text "select col from table order by col asc",
        pp
          ( Query
              { select = Cols [Col "col"],
                from = Table "table",
                wher = Nothing,
                groupBy = Nothing,
                orderBy = Nothing,
                limit = Just 3
              }
          )
          ~?= PP.text "select col from table limit 3",
        pp
          ( Query
              { select = Cols [Col "col", Agg Count "col2"],
                from = Table "table",
                wher = Just $ SQL.Op1 (SQL.CompVal $ SQL.ColName "col") SQL.IsNotNull,
                groupBy = Just ["col"],
                orderBy = Just ("col", Asc),
                limit = Just 10
              }
          )
          ~?= PP.text "select col,count(col2) from table where col is not null group by col order by col asc limit 10"
      ]

--------------------------------------------------------------------------------
-- TABLE unit tests

test_dimensions :: Test
test_dimensions =
  "testing fetching table dimensions"
    ~: TestList
      [ -- 1x1 table
        dimensions
          ( array
              ((0, 0), (0, 0))
              [ ((0, 0), Just (IntVal 1))
              ]
          )
          ~?= (1, 1),
        -- 2x1 table
        dimensions
          ( array
              ((0, 0), (1, 0))
              [ ((0, 0), Just (IntVal 1)),
                ((1, 0), Just (IntVal 2))
              ]
          )
          ~?= (2, 1),
        -- 1x2 table
        dimensions
          ( array
              ((0, 0), (0, 1))
              [ ((0, 0), Just (IntVal 1)),
                ((0, 1), Just (IntVal 2))
              ]
          )
          ~?= (1, 2),
        -- 2x2 table
        dimensions
          ( array
              ((0, 0), (1, 1))
              [ ((0, 0), Just (StringVal "(0, 0)")),
                ((1, 0), Just (StringVal "(1, 0)")),
                ((0, 1), Just (StringVal "(0, 1)")),
                ((1, 1), Just (StringVal "(1, 1)"))
              ]
          )
          ~?= (2, 2),
        -- 4x1 table
        dimensions
          ( array
              ((0, 0), (3, 0))
              [ ((0, 0), Just (IntVal 3)),
                ((1, 0), Just (IntVal 1)),
                ((2, 0), Just (IntVal 3)),
                ((3, 0), Just (IntVal 4))
              ]
          )
          ~?= (4, 1),
        -- 2x3 table
        dimensions
          ( array
              ((0, 0), (1, 2))
              [ ((0, 0), Just (StringVal "(0, 0)")),
                ((1, 0), Just (StringVal "(1, 0)")),
                ((0, 1), Just (StringVal "(0, 1)")),
                ((1, 1), Just (StringVal "(1, 1)")),
                ((0, 2), Just (StringVal "(0, 2)")),
                ((1, 2), Just (StringVal "(1, 2)"))
              ]
          )
          ~?= (2, 3)
      ]

test_tableToList :: Test
test_tableToList =
  "testing tableToList"
    ~: TestList
      [ -- 1x2 table
        tableToList
          ( array
              ((0, 0), (0, 1))
              [ ((0, 0), Just (IntVal 1)),
                ((0, 1), Just (IntVal 2))
              ]
          )
          ~?= [[Just (IntVal 1), Just (IntVal 2)]],
        -- 2x1 table
        tableToList
          ( array
              ((0, 0), (1, 0))
              [ ((0, 0), Just (IntVal 1)),
                ((1, 0), Just (IntVal 2))
              ]
          )
          ~?= [[Just (IntVal 1)], [Just (IntVal 2)]],
        -- 4x1 table
        tableToList
          ( array
              ((0, 0), (3, 0))
              [ ((0, 0), Just (IntVal 3)),
                ((1, 0), Just (IntVal 1)),
                ((2, 0), Just (IntVal 3)),
                ((3, 0), Just (IntVal 4))
              ]
          )
          ~?= [ [Just (IntVal 3)],
                [Just (IntVal 1)],
                [Just (IntVal 3)],
                [Just (IntVal 4)]
              ],
        -- 2x2 table
        tableToList
          ( array
              ((0, 0), (1, 1))
              [ ((0, 0), Just (StringVal "(0, 0)")),
                ((1, 0), Just (StringVal "(1, 0)")),
                ((0, 1), Just (StringVal "(0, 1)")),
                ((1, 1), Just (StringVal "(1, 1)"))
              ]
          )
          ~?= [[Just (StringVal "(0, 0)"), Just (StringVal "(0, 1)")], [Just (StringVal "(1, 0)"), Just (StringVal "(1, 1)")]],
        -- 2x3 table
        tableToList
          ( array
              ((0, 0), (1, 2))
              [ ((0, 0), Just (StringVal "(0, 0)")),
                ((1, 0), Just (StringVal "(1, 0)")),
                ((0, 1), Just (StringVal "(0, 1)")),
                ((1, 1), Just (StringVal "(1, 1)")),
                ((0, 2), Just (StringVal "(0, 2)")),
                ((1, 2), Just (StringVal "(1, 2)"))
              ]
          )
          ~?= [ [ Just (StringVal "(0, 0)"),
                  Just (StringVal "(0, 1)"),
                  Just (StringVal "(0, 2)")
                ],
                [ Just (StringVal "(1, 0)"),
                  Just (StringVal "(1, 1)"),
                  Just (StringVal "(1, 2)")
                ]
              ]
      ]

-- test_colToValue :: Test
-- test_colToValue =
--   "testing colToValue"
--     ~: TestList
--       [ colToValue (IntCol [])
--           ~?= [],
--         colToValue (IntCol [Just 1, Nothing, Just 3])
--           ~?= [Just (IntVal 1), Nothing, Just (IntVal 3)],
--         colToValue (IntCol [Nothing, Nothing, Nothing])
--           ~?= [Nothing, Nothing, Nothing],
--         colToValue (StringCol [Just "1st", Just "2nd", Just "3rd"])
--           ~?= [ Just (StringVal "1st"),
--                 Just (StringVal "2nd"),
--                 Just (StringVal "3rd")
--               ],
--         colToValue (DoubleCol [Just 1.11, Just 2.22, Just 3.33])
--           ~?= [ Just (DoubleVal 1.11),
--                 Just (DoubleVal 2.22),
--                 Just (DoubleVal 3.33)
--               ]
--       ]

--------------------------------------------------------------------------------
-- Overall Testing
test_sql_parser :: IO Counts
test_sql_parser =
  runTestTT $
    TestList
      [ test_selectTokenP,
        test_comparableP,
        test_selectExpP,
        test_fromExpP,
        test_joinExpP,
        test_colExpP,
        test_whereExpP,
        test_groupByP,
        test_orderByP,
        test_parseQuery
      ]

test_validate_query :: IO Counts
test_validate_query =
  runTestTT $
    TestList
      [ test_validateQuery,
        test_getNonAggCols,
        test_getAggCols,
        test_selectExpIsNonEmpty,
        test_groupByColsInSelectExp,
        test_distinctHasNoAggFuncs,
        test_noDistinctAndGroupBy
      ]

-- test_translator =
--   runTestTT $
--     TestList
--       [ test_selectExpToCols,
--         test_translateJoinExp,
--         test_translateFromExp,
--         test_whereExpToLoc,
--         test_limitExpToHead,
--         test_orderByToSortValues,
--         test_groupByToPandasGroupBy,
--         test_translateSQL,
--         test_getFuncs
--       ]

test_translator =
  runTestTT $
    TestList
      [ test_selectExpToCols,
        test_translateJoinExp,
        test_translateFromExp,
        test_whereExpToLoc,
        test_limitExpToHead,
        test_orderByToSortValues,
        test_groupByToPandasGroupBy,
        test_translateSQL,
        test_getFuncs
      ]

test_print :: IO Counts
test_print = runTestTT $ TestList [test_printPandasCommands, test_printSQLQuery]

test_table :: IO Counts
test_table = runTestTT $ TestList [test_dimensions, test_tableToList]