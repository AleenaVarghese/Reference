import pandas as pd

dict_values = {
				"Name":["Aleena","John","Adam"],
				"Age":[21,25,24],
				"Job":["Data Analyst","Software Engineer","HR Manager"]
			}
df = pd.read_csv("file name.csv")
df = pd.DataFrame(dict_values)
df.shape() # will return a tuple contains number of rows and number of columns 
df[2:5] #will return rows 2, 3 and 4
df.columns # will return full column names from the dataframe
df.Name # will return Column Name from the data Frame.
df["Name"] # df["Name"] and df.Name are same 
df[["Name","Age"]] # need multiple parantheses to get multiple columns from the dataframe

