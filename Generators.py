# A function that is used to create an iterator, allowing iteration over the elements in a sequence.
# A memory efficient way to iterate over infinite sequences 
# A generator function has 'yield' command in the function body instead of return statement. This is to yield control back to the calling program.
# Invoking a generator function does not execute the code of the function, it returns a generator object.
# code is executed when next() is called using generator object.
# The generator remembers the state of the local variables across function invocations.

def generator():
	n = 1
	print("one...!")
	yield n
	
	n+=1
	print("Two...!")
	yield n
	
	n+=1
	print("Three...!")
	yield n
	
	n+=1
	print("Four...!")
	yield n
	
g = generator()
next(g) 	# python remembers the previous value executed for generator.
next(g) # 	After Printing 4, We need to re generate the object of generator() for starting from one.


# 	Generators using for loop 

def create_odd_numbers(n): #	Program to create_odd_numbers
	for i in range(0,n,2):
		yield i
g = create_odd_numbers(10)
print(list(g)) 	# it will list all even numbers till 10. otherwise we need to invoke next(g) multiple times. and finally it may generate errors.
#**************************************
def squres_of_numbers(n): 	# 	Program to print the squres_of_numbers
	for i in range(n):
		yield (i**2)
		
g = squres_of_numbers(10)
print(list(g))
#**************************************
def powers_of_two():
	n = 0
	while True:
		n +=1
		yield 2 ** n

g = powers_of_two()
count = 0
for i in g:
	print(p)	
	count +=1
	if count >10:
		break

#**************************************