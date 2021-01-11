#******************************************************NOTE*************************************************************************
# A function which invokes itself within the function body. 
# python has a limit for how many times a function can call itself. by default limit is 3000. but the limit is configurable.
# In the initial step, we must specify the terminating condition.
# The recursive function may or may not return a value.
import sys 
sys.setrecursionlimit(100) 	#to set the recursion limit to 100

