-- File to declare types

module SQLTypes where

import Control.Applicative ((<|>))
import Data.Char qualified as Char
import Data.Data (Data)
import Data.Foldable (toList)
import Data.String (IsString (..))
import Test.QuickCheck

-- A SQL Query is a collection of statements
newtype Query = Query [Statement]
  deriving (Eq, Show)

-- instance Semigroup SQLQuery where
--   SQLQuery s1 <> SQLQuery s2 = Block (s1 <> s2)

-- instance Monoid SQLQuery where
--   mempty = SQLQuery []

data Statement
  = Select Table Var -- x = e
  -- = Select [Col] Query
  | Empty -- ';'
  deriving (Eq, Show)

-- stuff that comes after the Select clause
data SelectExp
  = ColNames [Name] -- colnames are a list of string names
  | Val Value -- literal values
  | Alias Name -- equivalent to "AS" keyword in SQL
  deriving (Eq, Show)

data FromExp
  = TableName Name
  | SubQuery Query
  deriving (Eq, Show)

data WhereExp
  = Op2 Comparable Comparable

data Value
  = NilVal -- nil
  | IntVal Int -- 1
  | BoolVal Bool -- false, true
  | StringVal String -- "abd"
  | TableVal Name -- <not used in source programs>
  deriving (Eq, Show, Ord)

data Uop
  = Neg -- `-` :: Int -> Int
  | Not -- `not` :: a -> Bool
  | Len -- `#` :: String -> Int / Table -> Int
  deriving (Eq, Show, Enum, Bounded)

-- Type representing values that can be compared in SQL queries
data Comparable
  = ColName Name -- column name
  | Int

-- Boolean operations
data BoolBop
  = Eq -- `==` :: Comparable -> Comparable -> Bool
  | Gt -- `>`  :: Comparable -> Comparable -> Bool
  | Ge -- `>=` :: Comparable -> Comparable -> Bool
  | Lt -- `<`  :: Comparable -> Comparable -> Bool
  | Le -- `<=` :: Comparable -> Comparable -> Bool
  deriving (Eq, Show, Enum, Bounded)

-- Arithmetic operations
data ArithBop
  = Plus -- `+`  :: Comparable -> Comparable -> Comparable
  | Minus -- `-`  :: Comparable -> Comparable -> Comparable
  | Times -- `*`  :: Comparable -> Comparable -> Comparable
  | Divide -- `//` :: Comparable -> Comparable -> Comparable   -- floor division
  | Modulo -- `%`  :: Comparable -> Comparable -> Comparable   -- modulo

data Sop
  = In

-- (ColA `Gt` ColB) `And` (ColC `Gt` ColD)
-- And (ColA `Gt` ColB) (ColC `Gt` ColD)

-- Logical operators
data Lop
  = And Bool Bool
  | Or Bool Bool
  | IsNull Name -- Check whether a column contains null values
  | IsNotNull Name -- Check whether a column contains non-null values

type Name = String -- column names

data Var
  = Name Name -- x, global variable
  | Dot Expression Name -- t.x, access table using string key
  | Proj Expression Expression -- t[1], access table table using any type of key
  deriving (Eq, Show)