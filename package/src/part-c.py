import pymongo
import re
from collections import deque
from pprint import pprint



class XPathParser():

    def __init__(self, uri, dbname):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[dbname]
        self.collection = ""
        self.schema = None


    # function to switch a database
    def setDatabase(self, dbname):
        self.db = self.client[dbname]


    # update schema of a collection as a dictionary
    def updateSchema(self, collection):
        sample = self.db[collection].find_one(projection={"_id" : 0})
        if sample == None:
            return {"success" : 0, "message" : "Collection %s is not in Database %s or is an empty collection."\
                 % (collection, self.db.name)}
        else:
            self.schema = self.buildSchema(sample)
            self.collection = collection
            return {"success" : 1, "message" : self.schema}


    # query entry
    # @params: s: input xpath as a String
    # @returns: query result from mongo
    def query(self, s, withID=True):
        result = self.generateSearch(s)
        if result["success"] == 0:
            return [result["message"]]

        searchContext = result["message"]
        if not withID:
            searchContext["projections"]["_id"] = 0

        result = None
        # TODO: case 1: xpath without aggregate functions
        if searchContext["aggregate"] != "":
            print("please implement logics for aggregation.")
        # case 2: xpath without aggregate functions
        else:
            # only considers "child" and "descendant" axes for now
            result = self.db[searchContext["collection"]].find(\
                filter=searchContext["filters"], 
                projection=searchContext["projections"])
        
        return result


    # generate a dictionary of the xpath equivalent
    # @params: s: input xpath as a String
    # @returns: {"aggregate" : aggregate function, 
    #           "collection" : collection name, 
    #           "filters" : predicates,
    #           "projections" : }
    def generateSearch(self, s):
        splittedPath = self.splitXPath(s)
        if splittedPath["collection"] != self.collection:
            result = self.updateSchema(splittedPath["collection"])
            if result["success"] == 0:
                return result

        searchContext = {"aggregate" : splittedPath["aggregate"],
                        "collection" : splittedPath["collection"],
                        "filters" : {},
                        "projections" : {}}
        self.queryHelperR(searchContext["filters"], 
                        searchContext["projections"], 
                        splittedPath["searchPath"], [])

        return {"success" : 1, "message" : searchContext}


    # recursively build up the search body
    # @params: filters: conditions generated from predicates; 
    #           projections: specified fields generated from paths;
    #           searchPath: partial path that has not been processed;
    #           acc: all the ancestors processed
    def queryHelperR(self, filters, projections, searchPath, acc):
        if searchPath == "":
            accPath = ".".join(acc)
            print(accPath)
            if accPath != "": 
                projections[accPath] = 1
            return

        # TODO: add support for predicates and other axes
        head, tail = searchPath.split("/", 1)
        axis, name = head.split("::")
        if axis == "child":
            acc.append(name)
            self.queryHelperR(filters, projections, tail, acc)
        elif axis == "descendant":
            ommittedPath = self.findPath(self.rootInSchema(acc), name)
            if ommittedPath == []:
                print("XPath from %s to %s is not shared by all objects." % (acc[-1], name))
            else:
                acc.extend(ommittedPath)
            self.queryHelperR(filters, projections, tail, acc)

    
    # ------------------------------helper functions-------------------------------------

    # build schema from "root" (dfs)
    def buildSchema(self, root):
        if root == None:
            return None

        if type(root) is dict:
            partialSchema = {}
            for k in root:
                partialSchema[k] = self.buildSchema(root[k])
            return partialSchema
        elif type(root) is list:
            return self.buildSchema(root[0])
        else:
            return type(root)


    # split a query into three parts: aggregate function, collection name and search path
    def splitXPath(self, s):
        # return value
        splitResult = {"aggregate" : "", "collection" : "", "searchPath" : []}

        # step 1: split out aggregation function keyword
        aggregateSplit = self.splitAggregateFunction(s)
        splitResult["aggregate"] = aggregateSplit["aggregate"]
        purePath = aggregateSplit["path"]

        # step 2: get the collection name from the root element of xpath (assuming the xml model is well-formed)
        collectionInfo, nodes = (purePath[1:] + "/").split("/", 1)
        splitResult["collection"] = collectionInfo.split("::")[1]

        # step 3: split out pure xpath with "/"
        splitResult["searchPath"] = nodes

        return splitResult


    # split the aggregate function name from the xpath (used for initial xpath splitting / predicate analysis)
    def splitAggregateFunction(self, s):
        splitResult = {"aggregate" : "", "path" : ""}
        aggregatePattern = re.compile("\((.+)\)")
        aggregateSplit = aggregatePattern.split(s)
        if len(aggregateSplit) > 1:
            splitResult["aggregate"] = aggregateSplit[0]
            splitResult["path"] = aggregateSplit[1]
        else:
            splitResult["path"] = aggregateSplit[0]
        return splitResult


    # find the root element in a sample document down the "path"
    def rootInSchema(self, path):
        sample = self.schema
        for p in path:
            if sample == None:
                break
            sample = sample[p]
        return sample


    # find a path to "name" starting from "root" (exclusive)
    def findPath(self, root, name):
        path = []    # bfs
        if root == None:
            return path

        queue = deque()
        queue.append((root, []))
        while queue:
            root, acc = queue.popleft()
            if type(root) is dict:
                matched = False
                for key in root.keys():
                    acc.append(key)
                    if key == name:
                        path = acc
                        matched = True
                        break
                    else:
                        queue.append((root[key], acc.copy()))
                        acc.pop(-1)
                if matched:
                    break
            elif type(root) is list:
                queue.append((root[0], acc))

        return path



if __name__ == "__main__":
    testHandler = XPathParser("mongodb://localhost:27017/", "test")

    testXPath = "/child::library/child::artists/child::artist/child::country"
    testXPath2 = "/child::library/descendant::artist/child::country"
    testXPath3 = "/child::library/descendant::country"
    testXPath4 = "/child::library/child::artists/descendant::country"
    for result in testHandler.query(testXPath2):
        pprint(result)
    for result in testHandler.query(testXPath3):
        pprint(result)
    for result in testHandler.query(testXPath4):
        pprint(result)

    print(testHandler.updateSchema("store"))