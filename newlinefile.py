
with open('D:\Study\data.txt', 'r') as file:
    data = file.read().replace('\n', '')
print(data)
for item in data:
	count=0
	if item =='|':
		count+=1
	if count>=3:
		data.replace('|', '\n')
		pass
print(data)
print(count)