import pymongo
import re
from pprint import pprint


class XPathParser:

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
        sample = self.db[collection].find_one(projection={"_id": 0})
        if sample is None:
            return {"success": 0, "message": "Collection %s is not in Database %s or is an empty collection."
                                             % (collection, self.db.name)}
        else:
            self.schema = self.buildSchema(sample)
            self.collection = collection
            return {"success": 1, "message": self.schema}

    # query entry
    # @params: s: input xpath as a String
    # @returns: query result from mongo / error message
    def query(self, s, withID=True):
        if not self.check_is_full_syntax(s):
            s = self.translate_to_full_syntax(s)
        print("***query: ", s)
        generationResult = self.generateSearch(s)
        # return error message
        if generationResult["success"] == 0:
            return [generationResult]

        searchContext = generationResult["message"]
        if not withID:
            searchContext["projections"]["_id"] = 0

        # case 1: xpath with only outer aggregate functions
        if searchContext["aggregate"] != "" and searchContext["predicateAggregate"] == "" and searchContext["innerAggregate"] == {}:
            if searchContext["aggregate"] == "count":
                projection_value = list(searchContext.get("projections").keys())[0]
                filter_pipe = {"$match": searchContext.get("filters")}
                project_pipe = {"$project":{projection_value:1, "result":{"$cond": { "if": { "$isArray": '$'+projection_value }, "then": { "$size": '$'+projection_value }, "else": 1}}}}
                result_pipe = {'$group': {'_id': None, 'result': {'$sum': '$result'}}}
                queryResult = self.db[searchContext["collection"]].aggregate([filter_pipe, project_pipe, result_pipe])
            else:
                projection_value = list(searchContext.get("projections").keys())[0]
                filter_pipe = {"$match": searchContext.get("filters")}
                result_pipe = {'$group': {'_id': None, 'result': {'$' + searchContext["aggregate"]: '$' + projection_value}}}
                queryResult = self.db[searchContext["collection"]].aggregate([filter_pipe, result_pipe])
        
        # case 2: xpath with aggregate functions in predicate
        elif searchContext["predicateAggregate"] != "":
            projection_value = list(searchContext.get("projections").keys())[0]
            filter_key = list(searchContext.get("filters").keys())[0]
            filter_value = list(searchContext.get("filters").values())[0]
            filter_value_key = list(filter_value.keys())[0]
            try:
                filter_value_value = float(list(filter_value.values())[0])
            except ValueError:
                filter_value_value = list(filter_value.values())[0]
            if searchContext["predicateAggregate"] == "count":
                add_field_pipe = {"$addFields":{"addedField":{"$cond":{ "if": { "$isArray": "$"+filter_key }, "then": { "$size": "$"+filter_key }, "else": 1}}}}
            else:
                add_field_pipe = {"$addFields":{"addedField":{"$"+searchContext["predicateAggregate"]:"$"+filter_key}}}
            match_pipe = {"$match":{"addedField":{filter_value_key:filter_value_value}}}
            project_pipe = {"$project":{projection_value:1}}
            
            # xpath with aggregate functions in predicate and final
            if searchContext["innerAggregate"] != {}:
                if searchContext['innerAggregate']["innerAggregateFunction"] == "count":
                    pipe = {"$project":{searchContext['innerAggregate']["groupBy"]:1, "result":{"$cond": { "if": { "$isArray": '$'+projection_value }, "then": { "$size": '$'+projection_value }, "else": 1}}}}
                    # xpath with aggregate functions in outer
                    if searchContext["aggregate"] != "":
                        if searchContext["aggregate"] == "count":
                            result_pipe = {'$group': {'_id': None, 'result': {'$sum': 1}}}
                        else:
                            result_pipe = {'$group': {'_id': None, 'result': {'$' + searchContext["aggregate"]: '$result'}}}
                        queryResult = self.db[searchContext["collection"]].aggregate([add_field_pipe, match_pipe, project_pipe, pipe, result_pipe])
                    # xpath with aggregate functions not in outer
                    else:
                        queryResult = self.db[searchContext["collection"]].aggregate([add_field_pipe, match_pipe, project_pipe, pipe])
                else:
                    pipe = {"$project":{searchContext['innerAggregate']["groupBy"]:1, "result":{"$"+searchContext['innerAggregate']["innerAggregateFunction"]: '$'+projection_value}}}
                    # xpath with aggregate functions in outer
                    if searchContext["aggregate"] != "":
                        if searchContext["aggregate"] == "count":
                            result_pipe = {'$group': {'_id': None, 'result': {'$sum': 1}}}
                        else:
                            result_pipe = {'$group': {'_id': None, 'result': {'$' + searchContext["aggregate"]: '$result'}}}
                        queryResult = self.db[searchContext["collection"]].aggregate([add_field_pipe, match_pipe, project_pipe, pipe, result_pipe])
                    # xpath with aggregate functions not in outer
                    else:
                        queryResult = self.db[searchContext["collection"]].aggregate([add_field_pipe, match_pipe, project_pipe, pipe])
            
            # xpath with aggregate functions in predicate and not in final
            else:
                # xpath with aggregate functions in predicate and not in final and in outer
                if searchContext["aggregate"] != "":
                    if searchContext["aggregate"] == "count":
                        result_pipe = {'$group': {'_id': None, 'result': {'$sum': 1}}}
                    else:
                        result_pipe = {'$group': {'_id': None, 'result': {'$' + searchContext["aggregate"]: '$' + projection_value}}}
                    queryResult = self.db[searchContext["collection"]].aggregate([add_field_pipe, match_pipe, project_pipe, result_pipe])
                # xpath with aggregate functions in predicate and not in final and not in outer
                else:
                    queryResult = self.db[searchContext["collection"]].aggregate([add_field_pipe, match_pipe, project_pipe])
        
        # case 3: xpath without aggregate functions in predicate
        else:
            # only considers "child" and "descendant" axes for now
            # xpath without any aggregate functions
            if searchContext["innerAggregate"] == {}:
                queryResult = self.db[searchContext["collection"]].find(
                    filter=searchContext.get("filters"),
                    projection=searchContext.get("projections"))
            else:
                projection_value = list(searchContext.get("projections").keys())[0]
                filter_pipe = {"$match":searchContext.get("filters")}
                
                # xpath with aggregate functions in final
                if searchContext['innerAggregate']["innerAggregateFunction"] == "count":
                    pipe = {"$project":{searchContext['innerAggregate']["groupBy"]:1, "result":{"$cond": { "if": { "$isArray": '$'+projection_value }, "then": { "$size": '$'+projection_value }, "else": 1}}}}
                    if searchContext["aggregate"] != "":
                        if searchContext["aggregate"] == "count":
                            result_pipe = {'$group': {'_id': None, 'result': {'$sum': 1}}}
                            queryResult = self.db[searchContext["collection"]].aggregate([filter_pipe, pipe, result_pipe])
                        else:
                            result_pipe = {'$group': {'_id': None, 'result': {'$' + searchContext["aggregate"]: '$result'}}}
                            queryResult = self.db[searchContext["collection"]].aggregate([filter_pipe, pipe, result_pipe])
                    else:
                        queryResult = self.db[searchContext["collection"]].aggregate([filter_pipe, pipe])
                
                # xpath without aggregate functions in final
                else:
                    pipe = {"$project":{searchContext['innerAggregate']["groupBy"]:1, "result":{"$"+searchContext['innerAggregate']["innerAggregateFunction"]: '$'+projection_value}}}
                    # xpath with aggregate functions in outer
                    if searchContext["aggregate"] != "":
                        if searchContext["aggregate"] == "count":
                            result_pipe = {'$group': {'_id': None, 'result': {'$sum': 1}}}
                            queryResult = self.db[searchContext["collection"]].aggregate([filter_pipe, pipe, result_pipe])
                        else:
                            result_pipe = {'$group': {'_id': None, 'result': {'$' + searchContext["aggregate"]: '$result'}}}
                            queryResult = self.db[searchContext["collection"]].aggregate([filter_pipe, pipe, result_pipe])
                    
                    # xpath without aggregate functions in outer
                    else:
                        queryResult = self.db[searchContext["collection"]].aggregate([filter_pipe, pipe])

        return queryResult

    # generate a dictionary of the xpath equivalent
    # @params: s: input xpath as a String
    # @returns: {"aggregate" : aggregate function,
    #           "collection" : collection name,
    #           "filters" : predicates,
    #           "projections" : } or error message
    def generateSearch(self, s):
        splittedPath = self.splitXPath(s)
        if splittedPath["collection"] != self.collection:
            result = self.updateSchema(splittedPath["collection"])
            if result["success"] == 0:
                return result

        # Split filter conditions in advance and declare here, for delivering to queryHelper below
        predicate = {}
        if "filters" in splittedPath.keys():
            predicate = {"filters": splittedPath["filters"], "prevNode": splittedPath["prevNode"]}
        searchContext = {"aggregate": splittedPath["aggregate"],
                         "collection": splittedPath["collection"]}
        # Now variable 'predicate' as the last param, instead of an empty dictionary
        result = self.queryHelper(splittedPath["searchPath"], [], self.schema, predicate)
        if result["success"] == 0:
            return result
        else:
            for field, content in result["message"].items():
                searchContext[field] = content
            searchContext["predicateAggregate"] = splittedPath["predicateAggregate"]
            print("Search Context: ", searchContext)
            return {"success": 1, "message": searchContext}

    # recursively build up the search body
    # @params: searchPath: partial path that has not been processed;
    #           acc: all the ancestors processed;
    #           currentNode: current node in the schema
    # @returns: success message with filter, projection, ... or error message
    def queryHelper(self, searchPath, acc, currentNode, filters, innerAggregate={}):
        if searchPath == "":
            accPath = ".".join(acc)
            if accPath != "":
                # Call predicateHelper to parse the filter conditions, separating this part from queryHelper
                if "filters" in filters.keys():
                    filters = self.predicateHelper(filters["filters"], filters["prevNode"], accPath)
                return {"success": 1, "message": {"filters": filters, "projections": {accPath: 1}, "innerAggregate": innerAggregate}}
            return {"success": 1, "message": {"filters": filters, "innerAggregate": innerAggregate}}
        print("Search Path: ", searchPath)
        splittedPath = self.splitAggregateFunction(searchPath)
        if splittedPath["aggregate"] != "":
            innerAggregate = {"innerAggregateFunction": splittedPath["aggregate"], "groupBy": acc[-1]}
            return self.queryHelper(splittedPath["path"]+"/", acc, currentNode, filters, innerAggregate)
        head, tail = searchPath.split("/", 1)
        axis, name = head.split("::")

        # case 1: "child" axes (/child::para, /child::*)
        if axis == "child":
            if name == "*":
                integratedResult = {"success": 0, "message": "Cannot find child from %s"
                                                             % (acc[-1] if acc else "(root node)*")}
                for key in currentNode.keys():
                    branch = acc.copy()
                    branch.append(key)
                    integratedResult = self.integrateResults(integratedResult,
                                                             self.queryHelper(tail, branch, currentNode[key], filters, innerAggregate))
                return integratedResult
            elif currentNode.get(name) is not None:
                acc.append(name)
                return self.queryHelper(tail, acc, currentNode[name], filters, innerAggregate)
            else:
                return {"success": 0, "message": "Cannot find complete path %s"
                                                 % (" -> ".join(acc) + " -> " + name)}
        # case 2: "descendant" and "descendant-or-self" axes (/descendant::para, /descendant-or-self::para,
        # /descendant-or-self::node()/child::para)
        elif re.compile("descendant.*").match(axis) is not None:
            omittedPaths = []
            self.findPaths(self.nodeInSchema(acc), name, -1, [], omittedPaths)

            # default return value with no matching result
            integratedResult = {"success": 0, "message": "Cannot find indirect path %s ->> %s"
                                                         % (acc[-1] if acc != [] else "(root node)", name)}
            # case 1: special case for "descendant-or-self"
            if axis == "descendant-or-self" and acc != [] and acc[-1] == name:
                possibleResult = self.queryHelper(tail, acc, currentNode, filters, innerAggregate)
                if possibleResult["success"] == 1:
                    integratedResult = possibleResult
            # case 2: find some paths from current node to "name"
            for path in omittedPaths:
                # raw use of "node()" is not well-supported because of position collision in pymongo
                # "node()" here is specially adjusted for translation from "//" (abbreviated)
                if name == "node()" and tail != "":
                    path.pop(-1)
                branch = acc.copy()
                branch.extend(path)
                integratedResult = self.integrateResults(integratedResult,
                                                         self.queryHelper(tail, branch, self.nodeInSchema(branch),
                                                                          filters, innerAggregate))
            return integratedResult
        # case 3: "parent" axes (/parent::para, /parent::node())
        elif axis == "parent":
            currentNodeName = "(root node)"
            if acc:
                currentNodeName = acc.pop(-1)
                if name == "node()" or (acc != [] and acc[-1] == name):
                    return self.queryHelper(tail, acc, self.nodeInSchema(acc), filters, innerAggregate)
            return {"success": 0, "message": "Cannot find parent %s from %s" % (name, currentNodeName)}
        # case 4: "ancestor" and "ancestor-or-self" axes (/ancestor::div, /ancestor-or-self::div)
        # (only returns the first ancestor (or self) due to pymongo restriction on path collision)
        elif re.compile("ancestor.*").match(axis) is not None:
            if axis == "ancestor-or-self" and (acc != [] and acc[-1] == name):
                return self.queryHelper(tail, acc, currentNode.get(name), filters, innerAggregate)
            elif acc:
                acc.pop(-1)  # strip current node from acc
                if name in acc:
                    while acc != [] and acc[-1] != name:
                        acc.pop(-1)
                    return self.queryHelper(tail, acc, self.nodeInSchema(acc), filters, innerAggregate)
            # all failing cases are collected here
            return {"success": 0, "message": "Cannot find ancestor(%s) %s from %s"
                                             % ("exclusive" if axis == "ancestor" else "inclusive", name,
                                                acc[-1] if acc != [] else "(root node)")}
        # case 5: "self" axes (/self::para, /self::node())
        elif axis == "self":
            if name == "node()" or (acc == [] and name == self.collection) or (acc != [] and acc[-1] == name):
                return self.queryHelper(tail, acc, currentNode, filters, innerAggregate)
            else:
                return {"success": 0, "message": "current node %s cannot match with declared 'self' %s"
                                                 % (acc[-1] if acc != [] else "(root node)", name)}

    # ------------------------------helper functions-------------------------------------

    # build schema from "root" (dfs)
    def buildSchema(self, root):
        if root is None:
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
        splitResult = {"aggregate": "", "collection": "", "searchPath": []}

        # step 0: split out filter conditions
        filterSplit = self.splitFilterFunction(s)
        if "filters" in filterSplit.keys():
            splitResult["filters"] = filterSplit["filters"]
        splitResult["prevNode"] = filterSplit["prevNode"]

        # step 1: split out aggregation function keyword
        # Note the param now is the result of splitFilterFunction which doesn't contain predicates
        aggregateSplit = self.splitAggregateFunction(filterSplit["searchPath"])
        splitResult["aggregate"] = aggregateSplit["aggregate"]
        purePath = aggregateSplit["path"]

        # step 2: get the collection name from the root element of xpath (assuming the xml model is well-formed)
        collectionInfo, nodes = (purePath[1:] + "/").split("/", 1)
        # special process for "/child::collection_name[predicate]"
        if nodes == "" and re.match(".+\[.+\]", collectionInfo):
            outerPredicateSplit = re.compile("(\[.+\])").split(collectionInfo, 1)
            if len(outerPredicateSplit) > 1:
                collectionInfo = outerPredicateSplit[0]
                nodes = "self::node()" + outerPredicateSplit[1] + "/"
        splitResult["collection"] = collectionInfo.split("::")[1]

        # step 3: split out pure xpath with "/"
        splitResult["searchPath"] = nodes

        if filterSplit.get("predicateAggregate"):
            splitResult["predicateAggregate"] = filterSplit["predicateAggregate"]
        else:
            splitResult["predicateAggregate"] = ""

        return splitResult

    # split the aggregate function name from the xpath (used for initial xpath splitting / predicate analysis)
    def splitAggregateFunction(self, s):
        splitResult = {"aggregate": "", "path": ""}
        aggregatePattern = re.compile("\((.+)\)")
        aggregateSplit = aggregatePattern.split(s)
        if len(aggregateSplit) > 1 and aggregateSplit[0].isalpha():
            splitResult["aggregate"] = aggregateSplit[0]
            splitResult["path"] = aggregateSplit[1]
        elif len(aggregateSplit) > 1 and not aggregateSplit[0].isalpha():
            splitResult["path"] = s
        else:
            splitResult["path"] = aggregateSplit[0]
        return splitResult

    # split the filter conditions in '[]' at the very beginning of pipeline, keeping the aggregate split function intact
    def splitFilterFunction(self, s):
        print("***query: ", s)
        splitResult = {}

        idxOpeningBracket = -1
        idxClosingBracket = -1

        for i in range(len(s)):
            if s[i] == '[':
                idxOpeningBracket = i
                break
        if idxOpeningBracket != -1:
            for i in range(idxOpeningBracket + 1, len(s), 1):
                if s[i] == ']':
                    idxClosingBracket = i
                    break
        if idxOpeningBracket != -1 and idxClosingBracket != -1:
            prevPath = s[0: idxOpeningBracket]
            searchPath = s[0: idxOpeningBracket] + s[idxClosingBracket + 1:]
            predicate = s[idxOpeningBracket + 1: idxClosingBracket]

            splitResult["filters"] = predicate
            splitResult["prevNode"] = prevPath[1:].split("/", 1)[-1].split("::")[-1]
            splitResult["searchPath"] = searchPath
        else:
            splitResult["prevNode"] = ''
            splitResult["searchPath"] = s
        
        if splitResult.get("filters"):
            predicate = splitResult["filters"]
            if self.check_in_keyword_set(predicate, 0):
                aggregatePattern = re.compile("\((.+)\)")
                aggregateSplit = aggregatePattern.split(predicate)
                splitResult["predicateAggregate"] = aggregateSplit[0]
                splitResult["filters"] = "".join(aggregateSplit[1:])

        print("***splitResult: ", splitResult)
        return splitResult

    # parse the filter conditions including 'and', 'or', 'not()', and other logic operators, result in dictionary format
    def predicateHelper(self, predicate, prevNode, accPath):
        operatorSet = {}
        res = []
        notFlag = False

        if " and " in predicate:
            predicate = predicate.split(" and ")
            operatorSet["and"] = predicate
            predicateList = list(operatorSet.values())[0]
        elif " or " in predicate:
            predicate = predicate.split(" or ")
            operatorSet["or"] = predicate
            predicateList = list(operatorSet.values())[0]
        elif " | " in predicate:
            predicate = predicate.split(" | ")
            operatorSet["|"] = predicate
            predicateList = list(operatorSet.values())[0]
        else:
            operatorSet["0"] = predicate
            predicateList = operatorSet.values()

        for predicate in predicateList:
            if len(predicate) > 0:
                if "not(" in predicate:
                    notFlag = True
                    predicate = predicate[4:-1]
                prevPath = accPath[:(accPath.rfind(prevNode)+len(prevNode))]
                completePath = prevPath + '/' + predicate
                completePath = completePath.replace("child::", "")
                completePath = completePath.replace("/", ".")

                if ">=" in predicate:
                    operator = ">="
                elif "<=" in predicate:
                    operator = "<="
                elif "!=" in predicate:
                    operator = "!="
                elif ">" in predicate:
                    operator = ">"
                elif "<" in predicate:
                    operator = "<"
                elif "=" in predicate:
                    operator = "="
                else:
                    operator = ""

                if len(operator) > 0:
                    predicateKey = completePath.split(operator)[0]
                    predicateValue = predicate.split(operator)[1]
                    if '\'' in predicateValue or '\"' in predicateValue:
                        predicateValue = predicateValue[1: -1]
                    if operator == ">=":
                        if notFlag:
                            res.append({predicateKey: {'$not': {'$gte': predicateValue}}})
                            notFlag = False
                        else:
                            res.append({predicateKey: {'$gte': predicateValue}})
                    elif operator == "<=":
                        if notFlag:
                            res.append({predicateKey: {'$not': {'$lte': predicateValue}}})
                            notFlag = False
                        else:
                            res.append({predicateKey: {'$lte': predicateValue}})
                    elif operator == "!=":
                        if notFlag:
                            res.append({predicateKey: {'$not': {'$ne': predicateValue}}})
                            notFlag = False
                        else:
                            res.append({predicateKey: {'$ne': predicateValue}})
                    elif operator == ">":
                        if notFlag:
                            res.append({predicateKey: {'$not': {'$gt': predicateValue}}})
                            notFlag = False
                        else:
                            res.append({predicateKey: {'$gt': predicateValue}})
                    elif operator == "<":
                        if notFlag:
                            res.append({predicateKey: {'$not': {'$lt': predicateValue}}})
                            notFlag = False
                        else:
                            res.append({predicateKey: {'$lt': predicateValue}})
                    elif operator == "=":
                        if notFlag:
                            print("ERROR! not() function cannot be used with '='. Please use '!='")
                            return {'error': 'error'}
                        else:
                            res.append({predicateKey: predicateValue})

        filters = {}

        for key in operatorSet.keys():
            if key == "0":
                if len(res) != 0:
                    filters.update(res[0])
            elif key == "and":
                filters.update({'$and': res})
            else:
                filters.update({'$or': res})

        return filters

    # find the root element in a sample document down the "path"
    def nodeInSchema(self, path):
        sample = self.schema
        for p in path:
            if sample is None:
                break
            sample = sample[p]
        return sample

    # find a path to "name" starting from "root" (exclusive), picking the first "num" paths
    # if num == -1, record all the paths in "paths", otherwise record the "num"th path only.
    def findPaths(self, root, name, num, acc, paths):
        if root is None or num == 0:
            return

        if type(root) is dict:
            for key in root.keys():
                acc.append(key)
                if key == name or name == "node()":
                    if num > 0:
                        num -= 1
                    if num <= 0:
                        paths.append(acc.copy())  # only append once when num == 0, always append when num < 0
                self.findPaths(root[key], name, num, acc.copy(), paths)
                acc.pop(-1)

    # integrate correct results from all the successful branches (please set a default value for the integrated result)
    def integrateResults(self, integratedResult, branchResult):
        if integratedResult["success"] == 0:
            integratedResult = branchResult
        elif branchResult["success"] == 1:
            for field, content in branchResult["message"].items():
                if not integratedResult["message"].get(field):
                    integratedResult[field] = {}
                for key, val in content.items():
                    integratedResult["message"][field][key] = val
        return integratedResult

    def check_is_full_syntax(self, query):
        if query.find("::") < 0:
            return False
        else:
            return True

    def check_in_keyword_set(self, query, start):
        result = False
        keyword_list = ["count", "sum", "max", "min", "avg", "contains", "starts-with", "doc"]
        keyword_set = set(keyword_list)
        keyword_len_set = set(list(map(len, keyword_list)))
        for l in keyword_len_set:
            result = result or (query[start:start + l] in keyword_set and not query[start + l].isalpha())
        return result

    def translate_to_full_syntax(self, query):
        result = ""
        start = 0

        if query[start].isalpha() and self.check_in_keyword_set(query, start):
            result += query[start]
            start += 1
        elif query[start].isalpha():
            result += "child::" + query[start]
            start += 1

        while start < len(query):
            if query[start] == "/" and query[start + 1].isalpha():
                result += "/child::"
                start += 1
            elif query[start] == "/" and query[start + 1] == "/":
                result += "/descendant-or-self::node()/child::"
                start += 2
            elif query[start].isalpha():
                if not query[start - 1].isalpha() and self.check_in_keyword_set(query, start):
                    result += query[start]
                    start += 1
                elif query[start - 1] == "[":
                    result = result + "child::" + query[start]
                    start += 1
                elif query[start - 1] == " " and query[start - 4:start - 1] == "and":
                    result = result + "child::" + query[start]
                    start += 1
                elif query[start - 1] == " " and query[start - 3:start - 1] == "or":
                    result = result + "child::" + query[start]
                    start += 1
                else:
                    result += query[start]
                    start += 1

            elif query[start] == "@":
                result += "attribute::"
                start += 1
            elif query[start] == "." and query[start + 1] == "/":
                result += "self::node()"
                start += 1
            elif query[start:start + 2] == "..":
                result += "parent::node()"
                start += 2
            else:
                result += query[start]
                start += 1

        return result


if __name__ == "__main__":
    testHandler = XPathParser("mongodb://localhost:27017/", "test")

    testXPath1 = "/child::library/child::title/descendant-or-self::title"
    testXPath2 = "/child::library/descendant-or-self::node()/child::title"
    testXPath3 = "/child::library/descendant::artist/child::country"  # test 3, 4 and 5 are equivalent
    testXPath4 = "/child::library/descendant::country"
    testXPath5 = "/child::library/child::artists/descendant::country"

    for result in testHandler.query(testXPath1):
        pprint(result)
    print("--------------------------------------------------")
    for result in testHandler.query(testXPath2):
        pprint(result)
    print("--------------------------------------------------")
    for result in testHandler.query(testXPath3):
        pprint(result)
    print("--------------------------------------------------")
    for result in testHandler.query(testXPath4):
        pprint(result)
    print("--------------------------------------------------")
    for result in testHandler.query(testXPath5):
        pprint(result)
    print("--------------------------------------------------")

    predicateTest = "/child::library/child::artists[child::artist/child::name<\"Wham!\"]"
    for result in testHandler.query(predicateTest):
        pprint(result)
    print("--------------------------------------------------")

    predicateTest2 = "/child::library[child::year>1990]"
    for result in testHandler.query(predicateTest2):
        pprint(result)
    print("--------------------------------------------------")

    predicateTest3 = "/child::library/descendant::song/self::song[child::title=\"Payam Island\"]/child::duration"
    for result in testHandler.query(predicateTest3):
        pprint(result)
    print("--------------------------------------------------")

    #################### Test for aggregate ############################
    # testXPath7 = "count(/child::library/descendant::song/child::title)"
    # testXPath8 = "sum(/child::library/descendant::year)"
    # testXPath9 = "avg(/child::library/descendant::year)"
    # testXPath10 = "min(/child::library/descendant::year)"
    # testXPath11 = "max(/child::library/descendant::year)"

    # for result in testHandler.query(testXPath7):
    #     pprint(result['result'])
    # for result in testHandler.query(testXPath8):
    #     pprint(result['result'])
    # for result in testHandler.query(testXPath9):
    #     pprint(result['result'])
    # for result in testHandler.query(testXPath10):
    #     pprint(result['result'])
    # for result in testHandler.query(testXPath11):
    #     pprint(result['result'])

    # testXPath14 = "/child::library/child::songs/count(child::song)"
    # for result in testHandler.query(testXPath14):
    #     pprint(result['result'])
    # print("--------------------------------------------------")

    # testXPath14count = "count(/child::library/child::songs/count(child::song))"
    # for result in testHandler.query(testXPath14count):
    #     pprint(result['result'])
    # print("--------------------------------------------------")

    # testXPath14max = "max(/child::library/child::songs/count(child::song))"
    # for result in testHandler.query(testXPath14max):
    #     pprint(result['result'])
    # print("--------------------------------------------------")
    
    # testXPath16 = "/child::hou/child::artists/max(child::artist/child::age)"
    # for result in testHandler.query(testXPath16):
    #     pprint(result['result'])
    # print("--------------------------------------------------")

    # testXPath16count = "count(/child::hou/child::artists/max(child::artist/child::age))"
    # for result in testHandler.query(testXPath16count):
    #     pprint(result['result'])
    # print("--------------------------------------------------")

    # testXPath16max = "max(/child::hou/child::artists/max(child::artist/child::age))"
    # for result in testHandler.query(testXPath16max):
    #     pprint(result['result'])
    # print("--------------------------------------------------")

    # testXPath17 = "/child::hou/child::artists[max(child::artist/child::age)>24]/child::artist"
    # for result in testHandler.query(testXPath17):
    #     pprint(result)
    # print("--------------------------------------------------")

    # testXPath18 = "/child::hou/child::artists[count(child::artist)>0.5]/child::artist"
    # for result in testHandler.query(testXPath18):
    #     pprint(result)
    # print("--------------------------------------------------")
    
    # testXPath19 = "/child::hou/child::artists[count(child::artist)>0]/sum(child::artist/child::age)"
    # for result in testHandler.query(testXPath19):
    #     pprint(result['result'])

    # print("--------------------------------------------------")
    # testXPath19count = "count(/child::hou/child::artists[count(child::artist)>0]/sum(child::artist/child::age))"
    # for result in testHandler.query(testXPath19count):
    #     pprint(result['result'])

    # print("--------------------------------------------------")
    # testXPath19max = "max(/child::hou/child::artists[count(child::artist)>0]/sum(child::artist/child::age))"
    # for result in testHandler.query(testXPath19max):
    #     pprint(result['result'])
    # print("--------------------------------------------------")

    # testXPath20 = "/child::hou/child::artists[count(child::artist)>1]/count(child::artist/child::age)"
    # for result in testHandler.query(testXPath20):
    #     pprint(result['result'])

    # print("--------------------------------------------------")
    # testXPath20count = "count(/child::hou/child::artists[count(child::artist)>1]/count(child::artist/child::age))"
    # for result in testHandler.query(testXPath20count):
    #     pprint(result['result'])

    # print("--------------------------------------------------")
    # testXPath20max = "max(/child::hou/child::artists[count(child::artist)>1]/count(child::artist/child::age))"
    # for result in testHandler.query(testXPath20max):
    #     pprint(result['result'])
    # print("--------------------------------------------------")

    # testXPath21 = "/child::hou/child::artists[count(child::artist)>0]/child::artist/child::age"
    # for result in testHandler.query(testXPath21):
    #     pprint(result)

    # print("--------------------------------------------------")
    # testXPath21count = "count(/child::hou/child::artists[count(child::artist)>0]/child::artist/child::age)"
    # for result in testHandler.query(testXPath21count):
    #     pprint(result['result'])

    # print("--------------------------------------------------")
    # testXPath21max = "max(/child::hou/child::artists[count(child::artist)>0]/child::artist/child::age)"
    # for result in testHandler.query(testXPath21max):
    #     pprint(result['result'])
    # print("--------------------------------------------------")
    #################### Test for aggregate end ############################

    parentAndAncestorTests = ["/child::library/child::songs/descendant::title/parent::node()",
                              "/child::library/child::songs/descendant::title/parent::song",
                              "/child::library/descendant::country/ancestor::artists",
                              "/child::library/descendant::country/ancestor::country",
                              "/child::library/descendant::artist/ancestor-or-self::artist"]
    for xpath in parentAndAncestorTests:
        print("-----------------------------------------------------\n")
        for result in testHandler.query(xpath):
            pprint(result)
    print("--------------------------------------------------")

    shortHandTest = "/library//title"
    for result in testHandler.query(shortHandTest):
        pprint(result)
    print("--------------------------------------------------")

    print(testHandler.updateSchema("store"))    # wrong collection name
    print("--------------------------------------------------")

    rootTest = "/child::library"
    for result in testHandler.query(rootTest):
        pprint(result)
