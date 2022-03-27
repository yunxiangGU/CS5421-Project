import pymongo
import re
from pprint import pprint



class XMLParser():

    def __init__(self, uri, dbname):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[dbname]


    # function to switch a database
    def setDatabase(self, dbname):
        self.db = self.client[dbname]


    # query entry
    # @params: s: input xpath as a String
    # @returns: query result from mongo
    def query(self, s, withID=True):
        searchContext = self.generateSearch(s)
        if not withID:
            searchContext["projections"]["_id"] = 0

        result = None
        # TODO: case 1: xpath without aggregate functions
        if searchContext["aggregate"] != "":
            print("please implement logics for aggregation.")
        # case 2: xpath without aggregate functions
        else:
            # only considers "child" axes for now
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
        searchContext = {"aggregate" : splittedPath["aggregate"],
                        "collection" : splittedPath["collection"],
                        "filters" : {},
                        "projections" : {}}
        self.queryHelperR(searchContext["filters"], 
                        searchContext["projections"], 
                        splittedPath["searchPath"], [])

        return searchContext


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

    
    # ------------------------------helper functions-------------------------------------

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



if __name__ == "__main__":
    testHandler = XMLParser("mongodb://localhost:27017/", "test")
    testXPath = "/child::library/child::artists/child::artist/child::country"
    for result in testHandler.query(testXPath, withID=True):
        pprint(result)