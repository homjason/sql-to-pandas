# SQL to Pandas Translator
Ernest Ng (`ngernest`) and Jason Hom (`homjason`)

Our CIS 5520 final project parses SQL queries and translates them into Pandas
commands.

<!-- This is an "Empty project" for Haskell. It is configured in the same way as
the lecture demo and homework assignments for CIS 5520, but contains no
code. Feel free to use this project for experimentation!

If you want to change the name of this project, look for all occurrences of
`project-cis5520` in the `project-cis5520.cabal` file and in the `hie.yaml` 
file. (And change the name of the cabal file to match your new name!) -->

## Module organization

<!-- Haskell packages typically divide their source code into three separate places:
  - The bulk of your code should be developed as a reusable library in 
    modules in the `src` directory. We've created [Lib.hs](src/Lib.hs) 
    for you to get started. You can add additional modules here.
  
  - The entry point for your executable is in [Main.hs](app/Main.hs). 
  
  - All of your test cases should be in [the test directory](test/Spec.hs). -->

### `app`
This folder has the entry point for the executable of our project in [Main.hs](app/Main.hs).
This allows the user to input a SQL query as a String and outputs a Pandas command.

### `src`

* `Types` folder defines the abstract syntax trees (ASTs) and algebraic data types (ADTs) for our project.
  * `SQLTypes.hs` specifies the ADTs of our internal SQL Query representation
  * `PandasTypes.hs` specifies the ADTs of our internal Pandas Command representation
  * `Types.hs` specifies common ADTs between all of the types
  * `TableTypes.hs` specifies the ADT for our internal Table representation. This is used for testing purposes
* `Parser.hs` contains the code for parsers. This is adapted from Homework 5's `Parser.hs`
* `SQLParser.hs` contains the code for SQL Query parsers
* `Translator.hs` contains the code for translating from the SQL Query AST to the Pandas Command AST
* `Print.hs` contains the code for pretty printing our Query and Command ASTs

### `test`
This folder defines HUnit and QuickCheck tests for our project.

* `UnitTests.hs` contains the code for all of our unit tests. Specifically for all functions and helper functions in `SQLParser.hs`, `Translator.hs`, and `Print.hs`
* `QuickCheckTests.hs` contains the code for our QuickCheck tests. 

## Order to be Read
1. [Types](src/Types)
    * [SQLTypes.hs](src/Types/SQLTypes.hs)
    * [PandasTypes.hs](src/Types/PandasTypes.hs)
    * [Types.hs](src/Types/Types.hs)
    * [TableTypes.hs](src/Types/TableTypes.hs)
2. [Parser.hs](src/Parser.hs)
3. [SQLParser.hs](src/SQLParser.hs)
4. [Translator.hs](src/Translator.hs)
5. [Print.hs](src/Print.hs)
6. UnitTests.hs
7. QuickCheckTests.hs

## Building, running, and testing

This project compiles with `stack build`. 
You can run the main executable with `stack run`.
You can run the tests with `stack test`. 

Finally, you can start a REPL with `stack ghci`.

## Importing additional libraries

This project is designed to run with stackage: you can easily use any library
in https://www.stackage.org/lts-19.19 by adding an entry to the
`build-depends` list of the `common-stanza` in the cabal file. If you want to
use a library that is not on stackage, you'll need to update the common-stanza
*and* add information to `stack.yaml` about where to find that library.

* PrettyPrint

