# CS5421-Project 
* Task: Design and implementation of a compiler for an XPath dialect for JSON and MongoDB.
* Team 12: Gu Yunxiang, Hou Shizheng, Liu Fengjiang, Zheng Ying, Zou Haoliang

## Brief
The project is a XPath compiler which take a XPath query as input, translate it into MongoDB query, then execute the MOngoDB query and return the result.

## Configuration
* Python 3 installed
* MongoDB (Import the "dataset/library.json" to the test database as the target collection)
* eXistDB (Optional, just to verify the result) (Import the "dataset/library.xml" to the test database as the target collection)

## Usage
### Option 1: install the package and use the handler
1. install our latest version of package with  
```pip install XPathMongoCompiler```;
2. import pymongo for underlying support for the  compiler:  
```import pymongo```;
3. import the compiler handler from the package:  
```from XPathMongoCompiler import XPathParser```;
4. The following codes serve as an example of processes from creating the compiler instance to conducting various kinds of queries:  
```
# create a compiler instance specifying the location of MongoDB and the database name
testHandler = XPathParser("mongodb://localhost:27017/", "test")

# sample simple query with only axes
for result in testHandler.query("/child::library/descendant::artist/ancestor", withID=False):
    pprint(result)

# sample query with predicate
for result in testHandler.query("/child::library/descendant::artists[child::artist/child::name="Wham!" or child::artist/child::name="Anang Ashanty"]", withID=False):
    pprint(result)

# sample query with predicate and aggregate functions
# please note that if the query contains aggregate function(s), please use result['result'] in the for loop as show below to get the result
for result in testHandler.query("max(/child::library/descendant::artists[count(child::artist)>0]/sum(child::artist/child::age))", withID=False):
    pprint(result['result'])
    
# sample simple query with shorthand syntax
for result in testHandler.query("/library//artist[name='Job Bunjob Pholin']/name", withID=False):
    pprint(result)


# other useful functions

# change the database manually
textHandler.setDatabase("test")
# update the document schema manually (this function would be called automatically for the first query on a collection or upon any change of collection)
textHandler.updateSchema("library")
```
### Option 2: run tests provide in source code
As an alternative, you can also run the "package/src/XPathMongoCompiler/compiler.py" script directly. We have provided several test sets that focus on different aspects of our design, and you can modify the code at the bottom of the file to run a whole test set or check a single query in a test set:
```
# test method 1: run a whole test set
for xpath in predicateTests:
    print("--------------------------------------------------\n")
    print("Input: ", xpath)
    for result in testHandler.query(xpath, withID=False):
        pprint(result)

# test method 2: run a single test in a test set
xpath = predicateTests[11]
print("--------------------------------------------------\n")
print("Input: ", xpath)
for result in testHandler.query(xpath, withID=True):
    pprint(result)
```
