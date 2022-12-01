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
import Types.PandasTypes as Pandas
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

-- TODO: define instances of the PP typeclass
-- (eg. PP for binary / unary operators, etc.)
instance PP Func where
  pp (DropDuplicates colNames) =
    case colNames of
      Nothing -> PP.text ".drop_duplicates()"
      Just ss -> PP.text (".drop_duplicates(subset=" ++ show ss ++ "))")
  -- QUESTION FOR JOE: How to incorporate list of strings in pretty printing? (are there f-strings in Haskell?)
  pp (SortValues cName o) = undefined
  pp (Rename colNameMap) = undefined
  pp (GroupBy colNames) = undefined
  pp (Pandas.Agg fn col newCol) = undefined
  pp (Loc boolExp) = undefined
  pp (Merge mergeExp) = undefined
  pp (Unique colNames) = undefined
  pp (Head n) = undefined
  pp ResetIndex = undefined

instance PP MergeExp where
  pp = undefined

-- TODO: figure out how to print a table
instance PP Block where
  pp (Block [s]) = pp s
  pp (Block ss) = PP.vcat (map pp ss)

-- Prints an entire Pandas Command (represented as a Haskell record)
instance PP Command where
  pp = undefined

instance PP Row where
  pp = undefined

instance PP Table where
  pp = undefined

instance PP Value where
  pp = undefined

instance PP Query where
  pp = undefined