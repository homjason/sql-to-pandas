-- Module for pretty printing Pandas
module Print where

import Control.Monad (mapM_)
import Data.Char qualified as Char
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

convertOrdToBool :: Order -> Bool
convertOrdToBool o = o == Asc

-- lowerAggName f =

-- TODO: define instances of the PP typeclass
-- (eg. PP for binary / unary operators, etc.)
instance PP Pandas.Func where
  -- pp (DropDuplicates colNames) =
  --   case colNames of
  --     Nothing -> PP.text ".drop_duplicates()"
  --     Just ss -> PP.text (".drop_duplicates(subset=" ++ show ss ++ "))")
  -- QUESTION FOR JOE: How to incorporate list of strings in pretty printing? (are there f-strings in Haskell?)
  pp (Pandas.SortValues cName o) = PP.text (".sort_values(by=[\"" ++ show cName ++ "\"], ascending=" ++ show (convertOrdToBool o) ++ ")")
  pp (Pandas.Rename colNameMap) = undefined
  pp (Pandas.GroupBy colNames) = PP.text (".groupBy(by=" ++ show colNames ++ ")")
  pp (Pandas.Aggregate fn col) = undefined
  pp (Pandas.Loc whereExp) = undefined
  pp (Pandas.Merge mergeExp) = pp mergeExp
  pp (Pandas.Unique colNames) = PP.text (".drop_duplicates(subset=" ++ show colNames ++ ")")
  pp (Pandas.Head n) = PP.text (".head(" ++ show n ++ ")")
  pp Pandas.ResetIndex = PP.text ".reset_index()"

printFn :: Pandas.Func -> String
printFn (Pandas.SortValues cName o) = ".sort_values(by=[\"" ++ show cName ++ "\"], ascending=" ++ show (convertOrdToBool o) ++ ")"
printFn (Pandas.Rename colNameMap) = undefined
printFn (Pandas.GroupBy colNames) = ".groupBy(by=" ++ show colNames ++ ")"
printFn (Pandas.Aggregate fn col) = ".agg({" ++ show col ++ ":" ++ show fn ++ "})"
printFn (Pandas.Loc whereExp) = undefined
printFn (Pandas.Merge mergeExp@(Pandas.MkMerge rightDf leftOn rightOn how)) = ".merge(" ++ show rightDf ++ ", left_on=\"" ++ show leftOn ++ "\", right_on=\"" ++ show rightOn ++ "\", how=\"" ++ show how ++ "\")"
printFn (Pandas.Unique colNames) = ".drop_duplicates(subset=" ++ show colNames ++ ")"
printFn (Pandas.Head n) = ".head(" ++ show n ++ ")"
printFn Pandas.ResetIndex = ".reset_index()"

instance PP Pandas.MergeExp where
  pp (Pandas.MkMerge rightDf leftOn rightOn how) = PP.text (".merge(" ++ show rightDf ++ ", left_on=\"" ++ show leftOn ++ "\", right_on=\"" ++ show rightOn ++ "\", how=\"" ++ show how ++ "\")")

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

-- >>> pp (Pandas.Command "table" (Just ["col1", "col2"]) (Just [Pandas.GroupBy ["col1"], Pandas.ResetIndex, Pandas.Aggregate Count "col2"]))
-- Prelude.undefined

instance PP Row where
  pp = undefined

instance PP Table where
  pp = undefined

instance PP Value where
  pp = undefined

instance PP Query where
  pp = undefined
