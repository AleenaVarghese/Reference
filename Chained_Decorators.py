# here we are using chaining decorators 
def asterisk_highlighted_fn(func):
	def highlight():
		print('*' *50)
		func()
		print('*'*50)
	return highlight
	
def plus_highlight_fn(func):
	def highlight():
		print('+'*50)
		func()
		print('+'*50)
	return highlight
	
@plus_highlight_fn
@asterisk_highlighted_fn # chaining
def print_msg_one(): 	# the decorator which is closest to the function definition has high priority.it is taking first along with the other.
	print("Hi...! be cool")

print(print_msg_one())

