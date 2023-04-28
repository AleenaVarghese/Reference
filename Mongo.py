Structure : db->collections->documents 
Mongo Queries

insert 
 single insert
1. db.user.inserOne({name : "Mohan",age : 39, address :{streat :"123 main st",zip : "12345"}})
2. db.users.insertMany([{name : "Aleena",age: 26},{name: "Namitha"}]) #multiple insert

find 
1. db.users.find()
2. db.users.find().limit(2)

Sort 
1. db.user.find().sort({name:1}) #asc order
2. db.user.find().sort({name :-1}) #desc order
3. db.user.find().sort({age : 1, name : -1})

Get only required fields 
1. db.user.find({},{name : 1, age : 1}) will return name and age from all records.
2. db.user.find({name :"aleena"},{name : 0, age : 1}) will return age of Aleena.

Search using name 
1. db.users.find({name : "Aleena"})
2. db.users.find({name: {$eq : "Aleena"}})
3. db.user.find({name : {$ne : "Aleena"}})

Search using age > or <
1. db.users.find({age : {$gt : 20}}) # > 
2. db.users.find({age : {$gte : 20}}) # >=
3. db.users.find({age : {$lt : 30}}) # <
4. db.users.find({ age : {lte : 35}}) # <=

Serach using In operator and Not in operator
1. db.users.find({name : {$in : ["Aleena","Namitha"]}})
2. db.users.find({name : {$nin : ["Aleena","Namitha"]}})

To Return objects having a particular column/field/key only 
1. db.users.find({age : {$exists : true }}) # will return objects/ records which have age column/key. will return records even if that key contains even Null values. Because, this checks the existence of key only. 
2. db.users.find({age : {$exists : false }}) # will return objects which doesn't have age column/key. 

Search using multiple conditions Using And / Or operator 
1. db.users.find({age : {$gte : 20 , $lte: 30},name : "Aleena"}) # And operator 
2. db.users.find({$and : [{name :"Aleena"},{age : 20}]}) # and operator
3. db.users.find({$or : [{name : "Aleena"},{age : {$lte : 20}}]})

Search using Not equal to 
1. db.users.find({age : {$not : {$lte : 10 }}}) # will return all records who's age is not defind or set as Null.

Search using Expressions 
1. db.users.find({$expr : {$gt : ["$debt","$balance"]}}) # return record if debt column value is > balance column value

Search in nested Fields 
1. db.users.find({"Address.Streat" : "123 main st"})

Return only one record 
1. db.users.findOne({age : {$lte : 40}})

Get No of records 
1. db.users.countDocuments({age : {gte : 20}})

    UPDATE QUERIES 
Update one record 
1. db.users.updateOne({age:26},{$set : {age : 27}})
2. db.users.updateOne({_id : ObjectId('123adbhjkloiy678')},{$set : {$name : "New Name"}})

Update using increment - intiger values 
1. db.users.updateOne({_id : ObjectId('123adbhjkloiy678')},{$inc {age : 3}})

Rename column name 
1. db.users.updateOne({_id : ObjectId('123adbhjkloiy678')},{$rename : {name : "firstname"}})

Remove a key from a record 
1. db.users.updateOne({_id : ObjectId('123adbhjkloiy678')},{$unset : {age : ""}})

Add new keys to an existing record. 

Add new value to a list value of a key like {hobbies : ["swimming","painting"]}
1. db.users.updateOne({_id : ObjectId('123adbhjkloiy678')},{$push : {hobbies : "Reading" }})

Remove an existing value from a list value of a key {hobbies : ["swimming","painting","Reading"]
1. db.user.updateOne({_id : ObjectId('123adbhjkloiy678')},{$pull : {hobbies : "swimming"}})

Update Multiple records 
1. db.users.updateMany({address : {$exists : true}}, {$unset : {address : ""}} ) # Remove address key from all records. 

Replace keys of record 
1. db.users.replaceOne({name : "Aleena"},{age : 25}) # replaceOne will remove all existing keys and values available for that particular record and keep only the value that is passes as arguments in this list. in this case, after executing this statement, this record will only contain age key/column. 

Delete 
1. db.users.deleteOne({name : "Aleena"})
2. db.user.deleteMany({age :{$exists : false}}) # delete records which doesn't have age key on it.

