#https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html
import pandas as pd

dict_values = {
				"Name":["Aleena","John","Adam","Mohan"],
				"Age":[21,25,24,],
				"Job":["Data Analyst","Software Engineer","HR Manager",]
			}
df = pd.read_csv("UsaMedicalSurgical.csv")
df = pd.DataFrame(dict_values)

df.shape() # will return a tuple contains number of rows and number of columns 
df[2:5] #will return rows 2, 3 and 4
df.columns # will return full column names from the dataframe
df.Name # will return Column Name from the data Frame.
df["Name"] # df["Name"] and df.Name are same 
df[["Name","Age"]] # need multiple parantheses to get multiple columns from the dataframe
df["Age"].min() 
df["Age"].max()
df["Age"].mean()
df["Age"].std()
df.describe() # will rerurn the count, mean, std, min, 25%, 50%, 75%, max data all together
# ******************* Conditional formatting using Pandas *******************
df[df["Age"] == df.Age.max()] # will return full row which contains maxium age 
df["Name"][df["Age"] == df.Agemax()] # will return value of Name which contains maxium age 
df["Name","Age"][df["Age"] == df["Age"].max()] 
df.set_index("Name",inplace = True) # this will remove the automati numeric index and set index as Name.By seting 'inplace = True' making changesin the original dataframe.
df.loc['John'] # can be now use as index and this will return the curresponding row.
df.reset_index(inplace = True) # This will reset back the updated index into automatic numeric index 
