import openPyxl
import json

workbook = openPyxl.loadworkbook('file_path')
workbook.sheetnames      # for getting all sheet names
sheet_object = workbook['sheetname']
sheet_object.title		# will print the sheet title
sheet_object['A1'].value 	# will return the value of cell A1
cell = sheet_object['B1']
print(dir(cell))	# will print all methods of cell
print(cell.row +" is the cell's row no and "+ cell.column +" is the cell's column no ")
sheet_object.cell(row = 1,column = 2).value
sheet_object.max_row 	# will print the full extend or full dimension of data that they contain
sheet_object.max_column 	# will print the full extend or full dimension of data that they contain

#iterate over all rows and columns in the spreadsheet
for i in range(1, max_row+1):
	for j in range(1, max_column+1):
		print(sheet_object.cell(row = i, column = j).value)

#iterate over rows and columns in the spreadsheet		
for rows in sheet_object['A1':'c2']:
	for cell in rows:
		print(cell.coordinate, cell.value)

#working with Json
revenues = {}		# json_object
for row in sheet_object.iter_rows(min_row = 1, max_row = max_row, min_col = 1, max_column = max_column, values_only = True): 	# itr_cols() is also there
	rep = row[0] 	# Assuming only 3 rows are there.
	rev_details ={
	"country" : row[1]
	"Revenue" : row[2]
	}
	revenues[rep] = rev_details
print(json.dumps(revenues, indent=4, sort_keys = True))

#*************************** IMPORTANT NOTES using new workbook ********************************

work_book = openPyxl.workbook() # new
work_book.active # to get currently active workbook
sheet = work_book['Sheet'] 	# will return object of workbook named sheet 
Sheet['A1']= "Hai"
Sheet['B1']= "Welcom To the World of Hope"
Sheet['C1']= "...!"
work_book.save("file_name")

# Overwriting

Sheet['A1']= "Hello"
Sheet['B1']= "Hope you have enjoyed the World of Hope"
Sheet['C1']= "...!"
work_book.save("file_name") # save using the same file name 

# Appending

sheet.calculate_dimension() 	# Will return the portion that contains data (like pressing control + shift + down keys)  
sheet.append(['one','two','three','four']) 		# Separate cell values to be inserted using commas 


# 	insert new empty row and column in specified locations 
sheet.insert_rows(idx = 2, amount = 3) 	# 	First parameter is starting row number. and second parameter is number of rows to be inserted
sheet.insert_cols(idx = 3) 	# 	the parameter is the location in which new column to be inserted.
sheet.calculate_dimension()
work_book.save(file_location)

# delete specified rows and column in specified locations 
sheet.delete_rows(idx = 2, amount = 3) 	# 	First parameter is starting row number. and second parameter is number of rows to be deleted
sheet.delete_cols(idx = 3) 	# 	the parameter is the location in which new column to be deleted.
sheet.calculate_dimension()
work_book.save(file_location)	

Sheet.title = "SheetName" # 	Rename Excel sheet
work_book.save(file_location)

# 	insert from a list of lists 
data = [['Sl.No','Name','Age'],[1,'Anu',25],[2,'Manu',30]]
for row in data:
	sheet.append(row)

# 	Important Columns and rows are making visible at all times Using Freeze Panes 
work_book = openpyxl.workbook()
sheet = work_book.active
sheet.freeze_panes = 'B1' 	# Freeze all columns above B1 and left of B1 
work_book.save(file_name) 	
sheet.freeze_panes = 'C2' 	# Freeze all columns above C2 and left of C2 

# Apply	filter and sort
#	***********************filtering***********************
work_sheet.auto_filter.ref = work_sheet.calculate_dimensin() 	# here specifying the range of data for applying filter
work_sheet.auto_filter.add_filter_column(0,['Brazil','Italy','India']) 	# First parameter specifies column 
work_book.save(file_location)
#   ***********************sorting***********************
range_str = work_sheet['B'][1].coordinate + ':' + work_sheet['B'][-1].coordinate
work_sheet.auto_filter.add_sort_condition(range_str, descending = True)
work_book.save(file_location)	

# ***********************   ***********************
work_book.create_sheet(title= 'newsheet', index= 0) 
newsheet =work_book.active
newsheet.merge_cells('A1:D3')
newsheet.merge_cells('B5:B6') 
newsheet['A1'].alignment = Alignment(horizontal='right', vertical='top')
newsheet['B5'].alignment = Alignment(horizontal='center') 	# Should use reapply in excel because of API buggs
work_book.save(file_location)	