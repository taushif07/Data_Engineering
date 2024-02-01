What is Python?\*
Python is a popular programming language. It was created by Guido van Rossum, and released in 1991.
It is used for:
web development (server-side),
software development,
mathematics,
system scripting.

What can Python do?\*
Python can be used on a server to create web applications.
Python can be used alongside software to create workflows.
Python can connect to database systems. It can also read and modify files.
Python can be used to handle big data and perform complex mathematics.
Python can be used for rapid prototyping, or for production-ready software development.

Why Python?\*
Python works on different platforms (Windows, Mac, Linux, Raspberry Pi, etc).
Python has a simple syntax similar to the English language.
Python has syntax that allows developers to write programs with fewer lines than some other programming languages.
Python runs on an interpreter system, meaning that code can be executed as soon as it is written. This means that prototyping can be very quick.
Python can be treated in a procedural way, an object-oriented way or a functional way.

Python Indentation\*
Indentation refers to the spaces at the beginning of a code line.
Where in other programming languages the indentation in code is for readability only, the indentation in Python is very important.
Python uses indentation to indicate a block of code.

Creating a Comment\*
Comments starts with a #, and Python will ignore them:

Creating Variables\*
Python has no command for declaring a variable.
A variable is created the moment you first assign a value to it.
Variables do not need to be declared with any particular type, and can even change type after they have been set.

Variable Names\*
A variable can have a short name (like x and y) or a more descriptive name (age, carname, total*volume). Rules for Python variables:
A variable name must start with a letter or the underscore character
A variable name cannot start with a number
A variable name can only contain alpha-numeric characters and underscores (A-z, 0-9, and * )
Variable names are case-sensitive (age, Age and AGE are three different variables)
A variable name cannot be any of the Python keywords.

and - A logical operator
as - To create an alias
assert -For debugging
break- To break out of a loop
class- To define a class
continue- To continue to the next iteration of a loop
def -To define a function
del- To delete an object
elif- Used in conditional statements, same as else if
else- Used in conditional statements
except- Used with exceptions, what to do when an exception occurs
False- Boolean value, result of comparison operations
finally -Used with exceptions, a block of code that will be executed no matter if there is an exception or not
for- To create a for loop
from- To import specific parts of a module
global- To declare a global variable
if -To make a conditional statement
import- To import a module
in -To check if a value is present in a list, tuple, etc.
is -To test if two variables are equal
lambda -To create an anonymous function
None -Represents a null value
nonlocal- To declare a non-local variable
not -A logical operator
or -A logical operator
pass- A null statement, a statement that will do nothing
raise- To raise an exception
return- To exit a function and return a value
True- Boolean value, result of comparison operations
try -To make a try...except statement
while -To create a while loop
with- Used to simplify exception handling
yield -To return a list of values from a generator

Global Variables\*
Variables that are created outside of a function (as in all of the examples above) are known as global variables.
Global variables can be used by everyone, both inside of functions and outside.

Built-in Data Types\*
In programming, data type is an important concept.
Variables can store data of different types, and different types can do different things.
Python has the following data types built-in by default, in these categories:

Text Type: str
Numeric Types: int, float, complex
Sequence Types: list, tuple, range
Mapping Type: dict
Set Types: set, frozenset
Boolean Type: bool
Binary Types: bytes, bytearray, memoryview
None Type: NoneType

x = "Hello World" str
x = 20 int
x = 20.5 float
x = 1j complex
x = ["apple", "banana", "cherry"] list
x = ("apple", "banana", "cherry") tuple
x = range(6) range
x = {"name" : "John", "age" : 36} dict
x = {"apple", "banana", "cherry"} set
x = frozenset({"apple", "banana", "cherry"}) frozenset
x = True bool
x = b"Hello" bytes
x = bytearray(5) bytearray
x = memoryview(bytes(5)) memoryview
x = None NoneType

x = str("Hello World") str
x = int(20) int
x = float(20.5) float
x = complex(1j) complex
x = list(("apple", "banana", "cherry")) list
x = tuple(("apple", "banana", "cherry")) tuple
x = range(6) range
x = dict(name="John", age=36) dict
x = set(("apple", "banana", "cherry")) set
x = frozenset(("apple", "banana", "cherry")) frozenset
x = bool(5) bool
x = bytes(5) bytes
x = bytearray(5) bytearray
x = memoryview(bytes(5)) memoryview

Python Numbers\*
There are three numeric types in Python:

int
float
complex

You can convert from one type to another with the int(), float(), and complex() methods

Random Number\*
Python does not have a random() function to make a random number, but Python has a built-in module called random that can be used to make random numbers

Specify a Variable Type\*
There may be times when you want to specify a type on to a variable. This can be done with casting. Python is an object-orientated language, and as such it uses classes to define data types, including its primitive types.

Casting in python is therefore done using constructor functions:

int() - constructs an integer number from an integer literal, a float literal (by removing all decimals), or a string literal (providing the string represents a whole number)
float() - constructs a float number from an integer literal, a float literal or a string literal (providing the string represents a float or an integer)
str() - constructs a string from a wide variety of data types, including strings, integer literals and float literals

Strings\*
Strings in python are surrounded by either single quotation marks, or double quotation marks.

'hello' is the same as "hello".

You can display a string literal with the print() function:

Strings are Arrays\*
Like many other popular programming languages, strings in Python are arrays of bytes representing unicode characters.

However, Python does not have a character data type, a single character is simply a string with a length of 1.

Square brackets can be used to access elements of the string.

Slicing\*
You can return a range of characters by using the slice syntax.

Specify the start index and the end index, separated by a colon, to return a part of the string.
