{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use lambda-case" #-}

-- NOTE: this library does not export the `P` data constructor.
-- All `Parser`s must be built using the following functions
-- exported by this file, as well as the `Functor`, `Applicative` and
-- `Alternative` operations.

-- | Modified the applicative-based parsing library from HW5
-- so that doParse uses the Either monad instead of Maybe
module Parser
  ( Parser,
    doParse,
    get,
    eof,
    filter,
    parse,
    parseFromFile,
    ParseError,
    satisfy,
    alpha,
    digit,
    upper,
    lower,
    space,
    char,
    string,
    int,
    chainl1,
    chainl,
    choice,
    between,
    sepBy1,
    sepBy,
  )
where

import Control.Applicative (Alternative (..))
import Control.Monad (guard, when)
import Data.Char
import Data.Foldable (asum)
import System.IO qualified as IO
import System.IO.Error qualified as IO
import Prelude hiding (filter)

-- definition of the parser type
newtype Parser a = P {doParse :: String -> Either ParseError (a, String)}

instance Functor Parser where
  fmap :: (a -> b) -> Parser a -> Parser b
  fmap f p = P $ \s -> do
    (c, cs) <- doParse p s
    return (f c, cs)

instance Applicative Parser where
  pure :: a -> Parser a
  pure x = P $ \s -> Right (x, s)

  (<*>) :: Parser (a -> b) -> Parser a -> Parser b
  p1 <*> p2 = P $ \s -> do
    (f, s') <- doParse p1 s
    (x, s'') <- doParse p2 s'
    return (f x, s'')

instance Alternative Parser where
  empty :: Parser a
  empty = P $ const $ Left "No parses"

  (<|>) :: Parser a -> Parser a -> Parser a
  p1 <|> p2 = P $ \s -> doParse p1 s `firstRight` doParse p2 s

-- We make Parser a Monad instance so that we can use bind
instance Monad Parser where
  return :: a -> Parser a
  return = pure

  -- Definition of bind inspired by Module 10 lecture solutions
  (>>=) :: Parser a -> (a -> Parser b) -> Parser b
  p >>= k = P $ \s -> do
    (a, s') <- doParse p s
    doParse (k a) s'

-- | Combine two Either values together, producing the first
-- successful result
firstRight :: Either a b -> Either a b -> Either a b
firstRight (Right x) _ = Right x
firstRight (Left _) y = y

-- | Return the next character from the input
get :: Parser Char
get = P $ \s -> case s of
  (c : cs) -> Right (c, cs)
  [] -> Left "No parses"

-- | This parser *only* succeeds at the end of the input.
eof :: Parser ()
eof = P $ \s -> case s of
  [] -> Right ((), [])
  _ : _ -> Left "No parses"

-- | Filter the parsing results by a predicate
filter :: (a -> Bool) -> Parser a -> Parser a
filter f p = P $ \s -> do
  (c, cs) <- doParse p s
  if f c
    then return (c, cs)
    else Left "Parsing results don't satisfy predicate"

---------------------------------------------------------------
---------------------------------------------------------------
---------------------------------------------------------------

type ParseError = String

-- | Use a parser for a particular string. Note that this parser
-- combinator library doesn't support descriptive parse errors, but we
-- give it a type similar to other Parsing libraries.
parse :: Parser a -> String -> Either ParseError a
parse parser str = case doParse parser str of
  Left _ -> Left "No parses"
  Right (a, _) -> Right a

-- | parseFromFile p filePath runs a string parser p on the input
-- read from filePath using readFile. Returns either a
-- ParseError (Left) or a value of type a (Right).
parseFromFile :: Parser a -> String -> IO (Either ParseError a)
parseFromFile parser filename = do
  IO.catchIOError
    ( do
        handle <- IO.openFile filename IO.ReadMode
        str <- IO.hGetContents handle
        pure $ parse parser str
    )
    ( \e ->
        pure $ Left $ "Error:" ++ show e
    )

-- | Return the next character if it satisfies the given predicate
satisfy :: (Char -> Bool) -> Parser Char
satisfy p = filter p get

-- | Parsers for specific sorts of characters
alpha, digit, upper, lower, space :: Parser Char
alpha = satisfy isAlpha
digit = satisfy isDigit
upper = satisfy isUpper
lower = satisfy isLower
space = satisfy isSpace

-- | Parses and returns the specified character
-- succeeds only if the input is exactly that character
char :: Char -> Parser Char
char c = satisfy (c ==)

-- | Parses and returns the specified string.
-- Succeeds only if the input is the given string
string :: String -> Parser String
string = foldr (\c p -> (:) <$> char c <*> p) (pure "")

-- | succeed only if the input is a (positive or negative) integer
int :: Parser Int
int = read <$> ((++) <$> string "-" <*> some digit <|> some digit)

-- | Parses one or more occurrences of @p@ separated by binary operator
-- parser @pop@.  Returns a value produced by a /left/ associative application
-- of all functions returned by @pop@.
-- See the end of the `Parsers` lecture for explanation of this operator.
chainl1 :: Parser a -> Parser (a -> a -> a) -> Parser a
p `chainl1` pop = foldl comb <$> p <*> rest
  where
    -- comb :: t -> (t -> t1 -> t2, t1) -> t2
    comb x (op, y) = x `op` y
    -- rest :: [(a -> a -> a, a)]
    rest = many ((,) <$> pop <*> p)

-- | @chainl p pop x@ parses zero or more occurrences of @p@, separated by @pop@.
-- If there are no occurrences of @p@, then @x@ is returned.
chainl :: Parser b -> Parser (b -> b -> b) -> b -> Parser b
chainl p pop x = chainl1 p pop <|> pure x

-- | Combine all parsers in the list (sequentially)
choice :: [Parser a] -> Parser a
choice = asum -- equivalent to: foldr (<|>) empty

-- | @between open close p@ parses @open@, followed by @p@ and finally
--   @close@. Only the value of @p@ is pureed.
between :: Parser open -> Parser a -> Parser close -> Parser a
between open p close = open *> p <* close

-- | @sepBy p sep@ parses zero or more occurrences of @p@, separated by @sep@.
--   Returns a list of values returned by @p@.
sepBy :: Parser a -> Parser sep -> Parser [a]
sepBy p sep = sepBy1 p sep <|> pure []

-- | @sepBy1 p sep@ parses one or more occurrences of @p@, separated by @sep@.
--   Returns a list of values returned by @p@.
sepBy1 :: Parser a -> Parser sep -> Parser [a]
sepBy1 p sep = (:) <$> p <*> many (sep *> p)
