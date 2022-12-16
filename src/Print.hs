-- Module for pretty printing Pandas
module Print where

import Control.Monad (mapM_)
import Data.Char qualified as Char
import Data.List
import Data.Map (Map)
import Data.Map qualified as Map
import Test.HUnit
import Test.QuickCheck (Arbitrary (..), Gen)
import Test.QuickCheck qualified as QC
import Text.PrettyPrint (Doc, (<+>))
import Text.PrettyPrint qualified as PP
import Types.PandasTypes qualified as Pandas
import Types.SQLTypes as SQL
import Types.TableTypes
import Types.Types

-- Prints (translated) Pandas commands

-- pretty-printer that displays datatypes as concise text
class PP a where
  pp :: a -> Doc

-- | Default operation for the pretty printer. Displays using standard formatting
-- rules, with generous use of indentation and newlines.
pretty :: PP a => a -> String
pretty = PP.render . pp

-- | Compact version. Displays its argument without newlines.
oneLine :: PP a => a -> String
oneLine = PP.renderStyle (PP.style {PP.mode = PP.OneLineMode}) . pp

{-
  Pretty printing for Pandas Commands
-}
convertOrdToBool :: Order -> Bool
convertOrdToBool o = o == Asc

lowerAggName :: String -> String
lowerAggName f = case f of
  [] -> []
  hd : tl -> Char.toLower hd : tl

mapJoinStyleToPandasSyntax :: JoinStyle -> String
mapJoinStyleToPandasSyntax js = case js of
  LeftJoin -> "left"
  RightJoin -> "right"
  InnerJoin -> "inner"

-- TODO: define instances of the PP typeclass
-- (eg. PP for binary / unary operators, etc.)
-- instance PP Pandas.Func where
-- pp (DropDuplicates colNames) =
--   case colNames of
--     Nothing -> PP.text ".drop_duplicates()"
--     Just ss -> PP.text (".drop_duplicates(subset=" ++ show ss ++ "))")
-- pp (Pandas.SortValues cName o) = PP.text (".sort_values(by=[\"" ++ show cName ++ "\"], ascending=" ++ show (convertOrdToBool o) ++ ")")
-- pp (Pandas.Rename colNameMap) = undefined
-- pp (Pandas.GroupBy colNames) = PP.text (".groupBy(by=" ++ show colNames ++ ")")
-- pp (Pandas.Aggregate fn col) = undefined
-- pp (Pandas.Loc whereExp) = undefined
-- pp (Pandas.Merge mergeExp) = pp mergeExp
-- pp (Pandas.Unique colNames) = PP.text (".drop_duplicates(subset=" ++ show colNames ++ ")")
-- pp (Pandas.Head n) = PP.text (".head(" ++ show n ++ ")")
-- pp Pandas.ResetIndex = PP.text ".reset_index()"

printFn :: Pandas.Func -> String
printFn (Pandas.SortValues cName o) = ".sort_values(by=[" ++ show cName ++ "], ascending=" ++ show (convertOrdToBool o) ++ ")"
printFn (Pandas.Rename colNameMap) = undefined
printFn (Pandas.GroupBy colNames) = ".groupBy(by=" ++ show colNames ++ ")"
printFn (Pandas.Aggregate fn col) = ".agg({" ++ show col ++ ":" ++ show (lowerAggName (show fn)) ++ "})"
printFn (Pandas.Loc whereExp) = undefined
printFn (Pandas.Merge mergeExp@(Pandas.MkMerge rightDf leftOn rightOn how)) = ".merge(" ++ rightDf ++ ", left_on=" ++ show leftOn ++ ", right_on=" ++ show rightOn ++ ", how=" ++ show (mapJoinStyleToPandasSyntax how) ++ ")"
printFn (Pandas.Unique colNames) = ".drop_duplicates(subset=" ++ show colNames ++ ")"
printFn (Pandas.Head n) = ".head(" ++ show n ++ ")"
printFn Pandas.ResetIndex = ".reset_index()"

instance PP Pandas.MergeExp where
  pp (Pandas.MkMerge rightDf leftOn rightOn how) = PP.text (".merge(" ++ show rightDf ++ ", left_on=\"" ++ show leftOn ++ "\", right_on=\"" ++ show rightOn ++ "\", how=\"" ++ show how ++ "\")")

instance PP WhereExp where
  pp = undefined

-- TODO: figure out how to print a table
instance PP Pandas.Block where
  pp (Pandas.Block [s]) = pp s
  pp (Pandas.Block ss) = PP.vcat (map pp ss)

-- Prints an entire Pandas Command (represented as a Haskell record)
instance PP Pandas.Command where
  pp (Pandas.Command df cols fns) = case (cols, fns) of
    (Nothing, Nothing) -> PP.text df
    (Just cs, Nothing) -> PP.text (df ++ show cs)
    (Nothing, Just fs) -> PP.text (foldr (\f acc -> printFn f ++ acc) "" fs)
    (Just cs, Just fs) -> PP.text (df ++ show cs ++ foldr (\f acc -> printFn f ++ acc) "" fs)

-- >>> pp (Command "table" (Just ["col"]) Nothing)
-- table["col"]

-- >>> pp (Command "table" (Just ["col"]) (Just [Head 5]))
-- table["col"].head(5)

-- >>> pp (Pandas.Command "table" (Just ["col1", "col2"]) (Just [Pandas.GroupBy ["col1"], Pandas.Aggregate Count "col2", Pandas.ResetIndex]))
-- table["col1","col2"].groupBy(by=["col1"]).agg({"col2":"count"}).reset_index()

-- >>> pp (Pandas.Command "table" (Just ["col"]) (Just [Pandas.SortValues "col" Asc]))
-- table["col"].sort_values(by=["col"], ascending=True)

{-
  Pretty printing for SQL Queries. This will be used for QuickCheck testing
-}
instance PP Query where
  pp = undefined

listToDoc :: [ColExp] -> Doc
listToDoc cExps = case cExps of
  [] -> PP.empty
  hd : tl -> pp hd <> PP.text ", " <> listToDoc tl

instance PP SelectExp where
  -- pp (Cols cExps) = PP.text $ "SELECT " <> show (map pp cExps)
  pp (Cols cExps) = PP.text $ "SELECT " <> tail (init (show (map pp cExps)))
  -- pp (Cols cExps) = PP.text $ "SELECT " <> foldr (\x acc -> acc ++ pp x ++ PP.text ", ") PP.empty cExps
  -- pp (Cols cExps) = PP.text "SELECT " <> listToDoc cExps
  -- pp (Cols cExps) = PP.text "SELECT " <> intercalate ", " ((map (show . pp) cExps))
  pp (DistinctCols cExps) = undefined
  pp Star = undefined

instance PP ColExp where
  pp (Col cName) = PP.text cName
  pp (Agg fn cName) = PP.text $ lowerAggName (show fn) <> "(" <> cName <> ")"

-- colExpToString :: ColExp -> String
--   colExpToString (Col cName) = cName
--   colExpToString (Agg fn cName) = lowerAggName (show fn) <> "(" <> cName <> ")"

-- >>> pp (Cols [Col "col1", Col "col2", Col "col3", Col "col4", Col "col5"])
-- SELECT col1,col2,col3,col4,col5

{-
data ColExp
  = Col ColName
  | Agg AggFunc ColName -- Aggregate functions (used with GROUP BY clauses)
  deriving
    (Eq, Show)
-}

instance PP Row where
  pp = undefined

instance PP Table where
  pp = undefined

instance PP Value where
  pp = undefined
