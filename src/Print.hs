-- Module for pretty printing Pandas
module Print where

import Control.Monad (mapM_)
import Data.Char qualified as Char
import Data.List
import Data.Map (Map)
import Data.Map qualified as Map
import Data.Maybe
import Test.HUnit ()
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

lowerName :: String -> String
lowerName f = case f of
  [] -> []
  hd : tl -> Char.toLower hd : tl

mapJoinStyleToPandasSyntax :: JoinStyle -> String
mapJoinStyleToPandasSyntax js = case js of
  LeftJoin -> "left"
  RightJoin -> "right"
  InnerJoin -> "inner"

instance PP Pandas.Func where
  pp (Pandas.SortValues cName o) = PP.text $ ".sort_values(by=[" ++ show cName ++ "], ascending=" ++ show (convertOrdToBool o) ++ ")"
  pp (Pandas.Rename colNameMap) = undefined
  pp (Pandas.Group colNames) = PP.text $ ".groupby(by=" ++ show colNames ++ ")"
  pp (Pandas.Aggregate fn col) = PP.text $ ".agg({" ++ show col ++ ":" ++ show (lowerName (show fn)) ++ "})"
  pp (Pandas.Loc boolExp) = PP.text ".loc[" <> pp boolExp <> PP.text "]"
  pp (Pandas.Merge mergeExp@(Pandas.MkMerge rightDf leftOn rightOn how)) = PP.text $ ".merge(" ++ rightDf ++ ", left_on=" ++ show leftOn ++ ", right_on=" ++ show rightOn ++ ", how=" ++ show (mapJoinStyleToPandasSyntax how) ++ ")"
  pp (Pandas.Unique colNames) = PP.text $ ".drop_duplicates(subset=" ++ show colNames ++ ")"
  pp (Pandas.Head n) = PP.text $ ".head(" ++ show n ++ ")"
  pp Pandas.ResetIndex = PP.text ".reset_index()"

instance PP Pandas.BoolExp where
  pp (Pandas.Op1 be uop) = pp be <> pp uop
  pp (Pandas.Op2 be1 bop be2) = pp be1 <> PP.char ' ' <> pp bop <> PP.char ' ' <> pp be2
  pp (Pandas.CompVal c) = pp c

instance PP Pandas.Comparable where
  pp (Pandas.ColName cn df) = PP.text $ df <> "[" <> show cn <> "]"
  pp (Pandas.LitInt i) = PP.int i
  pp (Pandas.LitString s) = PP.text $ show s
  pp (Pandas.LitDouble d) = PP.double d

instance PP Pandas.Uop where
  pp Pandas.IsNull = PP.text ".isnull()"
  pp Pandas.IsNotNull = PP.text ".notnull()"

instance PP Pandas.Bop where
  pp (Pandas.Comp op) = pp op
  pp (Pandas.Arith op) = pp op
  pp (Pandas.Logic op) = pp op

instance PP Pandas.CompOp where
  pp Pandas.Eq = PP.text "=="
  pp Pandas.Neq = PP.text "!="
  pp Pandas.Gt = PP.char '>'
  pp Pandas.Ge = PP.text ">="
  pp Pandas.Lt = PP.char '<'
  pp Pandas.Le = PP.text "<="

instance PP Pandas.LogicOp where
  pp Pandas.And = PP.char '&'
  pp Pandas.Or = PP.char '|'

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
    (Just cs, Nothing) -> PP.text (df <> show cs)
    (Nothing, Just fs) -> PP.text df <> foldr (\f acc -> pp f <> acc) PP.empty fs
    (Just cs, Just fs) -> PP.text (df <> show cs) <> foldr (\f acc -> pp f <> acc) PP.empty fs

{-
  Pretty printing for SQL Queries. This will be used for QuickCheck testing
-}
instance PP Query where
  pp (Query s f w gb ob l) = case (w, gb, ob, l) of
    (Nothing, Nothing, Nothing, Nothing) -> pp s <> PP.char ' ' <> pp f
    (Just wher, Nothing, Nothing, Nothing) -> pp s <> PP.char ' ' <> pp f <> PP.text " where " <> pp wher
    (Nothing, Just g, Nothing, Nothing) -> pp s <> PP.char ' ' <> pp f <> PP.text " group by " <> PP.text (intercalate "," g)
    (Nothing, Nothing, Just o@(cName, or), Nothing) -> pp s <> PP.char ' ' <> pp f <> PP.text " order by " <> PP.text cName <> PP.text " " <> PP.text (lowerName $ show or)
    (Nothing, Nothing, Nothing, Just x) -> pp s <> PP.char ' ' <> pp f <> PP.text " limit " <> PP.text (show x)
    (Just wher, Just g, Nothing, Nothing) -> pp s <> PP.char ' ' <> pp f <> PP.text " where " <> pp wher <> PP.text " group by " <> PP.text (intercalate "," g)
    (Nothing, Just g, Just o@(cName, or), Nothing) -> pp s <> PP.char ' ' <> pp f <> PP.text " group by " <> PP.text (intercalate "," g) <> PP.text " order by " <> PP.text cName <> PP.text " " <> PP.text (lowerName $ show or)
    (Nothing, Nothing, Just o@(cName, or), Just x) -> pp s <> PP.char ' ' <> pp f <> PP.text " order by " <> PP.text cName <> PP.text " " <> PP.text (lowerName $ show or) <> PP.text " limit " <> PP.text (show x)
    (Nothing, Just g, Nothing, Just x) -> pp s <> PP.char ' ' <> pp f <> PP.text " group by " <> PP.text (intercalate "," g) <> PP.text " limit " <> PP.text (show x)
    (Nothing, Just g, Just o@(cName, or), Just x) -> pp s <> PP.char ' ' <> pp f <> PP.text " group by " <> PP.text (intercalate "," g) <> PP.text " order by " <> PP.text cName <> PP.text " " <> PP.text (lowerName $ show or) <> PP.text " limit " <> PP.text (show x)
    (Just wher, Nothing, Nothing, Just x) -> pp s <> PP.char ' ' <> pp f <> PP.text " where " <> pp wher <> PP.text " limit " <> PP.text (show x)
    (Just wher, Nothing, Just o@(cName, or), Nothing) -> pp s <> PP.char ' ' <> pp f <> PP.text "w here " <> pp wher <> PP.text " order by " <> PP.text cName <> PP.text " " <> PP.text (lowerName $ show or)
    (Just wher, Nothing, Just o@(cName, or), Just x) -> pp s <> PP.char ' ' <> pp f <> PP.text " where " <> pp wher <> PP.text " order by " <> PP.text cName <> PP.text " " <> PP.text (lowerName $ show or) <> PP.text " limit " <> PP.text (show x)
    (Just wher, Just g, Nothing, Just x) -> pp s <> PP.char ' ' <> pp f <> PP.text " where " <> pp wher <> PP.text " group by " <> PP.text (intercalate "," g) <> PP.text " limit " <> PP.text (show x)
    (Just wher, Just g, Just o@(cName, or), Nothing) -> pp s <> PP.char ' ' <> pp f <> PP.text " where " <> pp wher <> PP.text " group by " <> PP.text (intercalate "," g) <> PP.text " order by " <> PP.text cName <> PP.text " " <> PP.text (lowerName $ show or)
    (Just wher, Just g, Just o@(cName, or), Just x) -> pp s <> PP.char ' ' <> pp f <> PP.text " where " <> pp wher <> PP.text " group by " <> PP.text (intercalate "," g) <> PP.text " order by " <> PP.text cName <> PP.text " " <> PP.text (lowerName $ show or) <> PP.text " limit " <> PP.text (show x)

listToDoc :: [ColExp] -> Doc
listToDoc cExps = case cExps of
  [] -> PP.empty
  hd : tl -> pp hd <> PP.text ", " <> listToDoc tl

instance PP SelectExp where
  -- pp (Cols cExps) = PP.text $ "SELECT " <> show (map pp cExps)
  pp (Cols cExps) = PP.text $ "select " <> tail (init (show (map pp cExps)))
  -- pp (Cols cExps) = PP.text $ "SELECT " <> foldr (\x acc -> acc ++ pp x ++ PP.text ", ") PP.empty cExps
  -- pp (Cols cExps) = PP.text "SELECT " <> listToDoc cExps
  -- pp (Cols cExps) = PP.text "SELECT " <> intercalate ", " ((map (show . pp) cExps))
  pp (DistinctCols cExps) = PP.text $ "select distinct " <> tail (init (show (map pp cExps)))
  pp Star = PP.text "select *\n"

instance PP ColExp where
  pp (Col cName) = PP.text cName
  pp (Agg fn cName) = PP.text $ lowerName (show fn) <> "(" <> cName <> ")"

mapJoinStyleToSQLSyntax :: JoinStyle -> String
mapJoinStyleToSQLSyntax js = case js of
  LeftJoin -> " left join "
  RightJoin -> " right join "
  InnerJoin -> " join "

instance PP FromExp where
  pp (Table n) = PP.text $ "from " <> n
  pp (TableJoin (Join lTable lCol rTable rCol st)) =
    PP.text $ "from " <> lTable <> mapJoinStyleToSQLSyntax st <> rTable <> " on " <> lTable <> "." <> lCol <> " = " <> rTable <> "." <> rCol

instance PP WhereExp where
  pp (Op2 we1 bop we2) = pp we1 <> PP.char ' ' <> pp bop <> PP.char ' ' <> pp we2
  pp (Op1 we uop) = pp we <> PP.char ' ' <> pp uop
  pp (CompVal c) = pp c

instance PP SQL.Comparable where
  pp (ColName cn) = PP.text cn
  pp (LitInt i) = PP.int i
  pp (LitString s) = PP.text $ show s

-- pp (LitDouble d) = PP.double d

instance PP SQL.Uop where
  pp IsNull = PP.text "is null"
  pp IsNotNull = PP.text "is not null"

instance PP SQL.Bop where
  pp (Comp op) = pp op
  pp (Arith op) = pp op
  pp (Logic op) = pp op

instance PP SQL.CompOp where
  pp Eq = PP.char '='
  pp Neq = PP.text "!="
  pp Gt = PP.char '>'
  pp Ge = PP.text ">="
  pp Lt = PP.char '<'
  pp Le = PP.text "<="

instance PP ArithOp where
  pp Plus = PP.char '+'
  pp Minus = PP.char '-'
  pp Times = PP.char '*'
  pp Divide = PP.char '/'
  pp Modulo = PP.text "%"

instance PP SQL.LogicOp where
  pp And = PP.text "and"
  pp Or = PP.text "or"
