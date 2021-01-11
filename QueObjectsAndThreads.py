# Queues are thread safe. that is some elements can be added and some elements can be -
# removed from the queue simultaneously using 2 different threads.
# queue.LifeQueue() is used for implementing stack 
import queue
import time

# #********************************************* Implementing Queue ************************************************
q = Queue.Queue()
print(q.queue) 	# will print deque([])

for i in range(7):
	q.put(i)
print(q.queue)  # will print deque([0,1,2,3,4,5,6])

while not q.empty():
	print(q.get()) 	# will print 0,1,2,3,4,5,6. here the elements are returned from the queue in FIFO order.	
print(q.queue) 	# will print deque([])

#********************************************* Implementing Stack ************************************************
q = queue.LifeQueue()
for i in range(7):
	q.put(i)
while not q.empty():
	print(q.get())   # will print 6,5,4,3,2,1,0. here the elements are returned from the queue in LIFO order.
	
#********************************************* Implementing Priority Queue ************************************************

q = queue.PriorityQueue()  # Elements can be added to a Priority Queue in any order.
q.put(3)
q.put(5)
q.put(4)
q.put(1)   # highest priority element
q.put(6)   # lowest priority element
q.put(2)

while not q.empty():
	print(q.get())   # Elements can be removed from the Priority Queue in the order of their priority. Here 1 is removed first.
	
	
	
	