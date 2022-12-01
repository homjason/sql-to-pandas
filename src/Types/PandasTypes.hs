module Types.PandasTypes where

import Control.Applicative ((<|>))
import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.Map
import Data.String (IsString (..))
import Test.QuickCheck
import Types.TableTypes
import Types.Types

{-
Things we (might) need types for:
- Dataframe names (eg. `df`)
- Column names (regular strings)
- Projecting one or more columns (eg. `df[['col1', 'col2']]`)
- Pandas function names (eg. drop_duplicates(), sort_values(), )
- Applying a Boolean filter (eg. countries[countries['area'] > 1000])
-}

-- An atomic Pandas command
data Command = Command
  { df :: TableName,
    cols :: Maybe [ColName],
    fn :: Maybe Func
  }
  deriving (Eq, Show)

-- A block is a sequence of Pandas commands
newtype Block = Block [Command]
  deriving (Eq, Show)

-- Instances for joining Blocks together
instance Semigroup Block where
  Block s1 <> Block s2 = Block (s1 <> s2)

instance Monoid Block where
  mempty = Block []

-- Pandas functions
-- For AggFunc, the 1st ColName arg is the column we're aggregating over
-- and the 2nd ColName arg is the new (alias) name for the aggregated column
data Func
  = DropDuplicates (Maybe [ColName])
  | SortValues ColName Order
  | Rename (Map ColName ColName)
  | GroupBy [ColName]
  | Agg AggFunc ColName ColName
  | Loc BoolExp -- filtering on rows (akin to WHERE in SQL)
  | Merge MergeExp
  | Unique [ColName]
  | Head Int -- limit no. of rows in output
  | ResetIndex
  deriving (Show, Eq)

-- Merge expressions for joining tables in Pandas
data MergeExp = MkMerge
  { rightDf :: TableName,
    leftOn :: ColName,
    rightOn :: ColName,
    how :: JoinStyle
  }
  deriving (Show, Eq)