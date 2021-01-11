# A function nested within another function is called a closure.
# A closure can refer to outer variables defined within the outer function.
# Has access to these local variables when the main function is no longer present in the python memory.
# closer carry around local state along with the function definition
# closers are very important concepts used to implement decorators

# defining a closure inside a parent function, which returns the reference of the closure and assign the main function object
# into a variable and in the next step invoke the returned closure by using '()' along with the mentioned variable. 
import random

def greet_with_personal_Messages(name,message): 	# 	parent method.
	annotations = ['+','?','*','$','#','&','@']
	annotate = random.choice(annotations)
	
	def greeting(): 	# 	defining a closure
		print(annotate * 50)
		print(message, name)
		print(annotate * 50)
	return greeting # 	returns the object of closure greeting.  
	
greet_Aleena_fn = greet_with_personal_Messages('Aleena', 'Hai') # will return closure .
greet_Aleena_fn() # calling the closure fun returned by main function. 
del greet_with_personal_Messages
greet_Aleena_fn()	# A closure can maintain its information even though,the original parent function is deleted.

# 	Program of Student enrollment in college 
def college_enrollment(college):
	student_list = []
	def student_enrollment(student):
		student_list.append(student)
		print(student, college)
		print(student_list)
	return student_enrollment

enroll_marian = college_enrollment("Marian")
enroll_marSleeva = college_enrollment("MarSleeva")
enroll_marian('Aleena')
enroll_marian('karadi')
print("************************************")
enroll_marSleeva('Aleena')
enroll_marSleeva('Libi')

