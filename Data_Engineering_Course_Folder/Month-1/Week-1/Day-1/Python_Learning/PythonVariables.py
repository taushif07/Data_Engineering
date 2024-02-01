import random

# not specific declaration of type
a = "tutorial"

# specific declaration of type
# b = int("tutorial")
# c = str(1)     These are invalid due to type declaration

b = int(1)
c = str("Learning")

print(type(b), type(c), type(a))

# Assign multiple values : Python allows you to assign values to multiple variables in one line:

id = 1
x, y, z = "name", id, True

# And you can assign the same value to multiple variables in one line:
q = w = e = id

print(x, y, z, q, w, e)
# If you have a collection of values in a list, tuple etc. Python allows you to extract the values into variables. This is called unpacking.

Arr = ["name", True, 1]

A, B, C = Arr

print(A, B, C)

# Same as js as it is run line by line
newVal = "newValue"
newVal = 1
print(newVal)

# Global Variables

glb = "global variables"


def newFunction():
    glb = "new blocked scope global variables"
    print("new inside glb", glb)


newFunction()
print("outside glb", glb)


def glbKeyFunction():
    global glbKeyUse
    glbKeyUse = "global Key for everywhere"


glbKeyFunction()
print("global key", glbKeyUse)


xyz = "awesome"


def myfunc():
    global xyz
    xyz = "fantastic"


myfunc()

print("Python is " + xyz)


# Data types in python
# 1 Integers (int, float, complex)

num1 = 4
num2 = 0.8
num3 = 5 + 4j

print(num1, num2, num3)
print(type(num1), type(num2), type(num3))

newNum1 = float(num1)
newNum2 = int(num2)
newNum3 = complex(num2)

print(newNum1, newNum2, newNum3)

# Random Number

print(random.randrange(1, 20))


# 2 Strings

# multi-line string

multiStr = """This is the very beginning of my data eng. learning journey 
And I am going not to be the best but GOAT."""

print(multiStr)

# String as Array

newArrStr = "TaushifDeveloper"

print(newArrStr[len(newArrStr) - 1])

# for loop in string
Str1 = ""
for x in "banana":
    Str1 += x + " "

print(Str1)

# check a certain phrase in a string

textStr = "This a new certain string"
certainStr = "certain" in textStr

print(certainStr)  # boolean

# combining this to if statements

if certainStr:
    print("include certain")
else:
    print("not include certain")


# combing this with not in

if "not new" not in textStr:
    print("not new")
else:
    print("new")


# Slicing the string
