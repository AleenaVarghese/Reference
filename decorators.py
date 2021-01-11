# decorators are a technique to add functionality to code without modifying the code itself.
# closers are used to implement decorators.

import random 
import math 

def Make_Highlighted(func): 	# 	parent method.
	annotations = ['+','?','*','$','#','&','@']
	annotate = random.choice(annotations)
	
	def Highlighted(): 	# 	defining a closure
		print(annotate * 50)
		func()
		print(annotate * 50)
	return Highlighted	 # 	returns the object of closure greeting.  
	

@Make_Highlighted
def print_message():
	print("hi print_message")

@Make_Highlighted
def second_message():
	print("hi second_message")
	
Highlight = Make_Highlighted(print_message) # will return closure .
Highlight() # calling the closure fun returned by main function. 

second_message() # 	by using, decorators we can call it directly.
print_message()

#**************************************

def calculate_fn(fun):
	def calculate(*args): 	# can take variable number of arguments as a tuple. otherwise use, def calculate(r):
		for arg in args:		
			if arg <= 0:
				raise valueError("enter +ve no")
			return fun(*args) # can take variable number of arguments 
	return calculate

@calculate_fn
def area_circle_fun(radius):
	return math.pi* radius* radius #	pi*r*r

@calculate_fn
def perimeter_circle_fun(radius): # 	2*pi*r
	return 2 * math.pi* radius

@calculate_fn
def area_rectangle_fn(length,breadth): 	# calculate area of rectangle
	return length * breadth


perimeter_fn = calculate_fn(perimeter_circle_fun)
perimeter_fn(5)

print(area_rectangle_fn(4,5))
print(perimeter_circle_fun(3)) #can be used directly by decorators
