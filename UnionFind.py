from collections import defaultdict

class UnionFind:

    #Attribution: https://algocoding.wordpress.com/2014/09/25/union-find-data-structure-disjoint-set-data-structure-part-2/
    """
    Class that implements the union-find structure with
    union by rank and find with path compression
    """
     
    def __init__(self):
        #self.parent = list(range(n))
        #self.rank = [0 for x in range(n)]
        self.parent = dict()
        self.rank = dict()

    def find(self, v):
        if v not in self.parent:
            self.parent[v] = v
            self.rank[v] = 0
            return self.parent[v]
        if not v == self.parent[v]:
            self.parent[v] = self.find(self.parent[v])
        return self.parent[v]
 
    def union(self, x, y):
        xRoot = self.find(x)
        yRoot = self.find(y)
        if xRoot == yRoot:
            return
        if self.rank[xRoot] > self.rank[yRoot]:
            self.parent[yRoot] = xRoot
        else:
            self.parent[xRoot] = yRoot
            if self.rank[xRoot] == self.rank[yRoot]:
                self.rank[yRoot] += 1
 
    def printParents(self):
        print("index: ",list(self.parent.keys()))
        print("parents: ",list(self.parent.values()))
        
    def printDisjointSets(self):
        myDict = defaultdict(set)
        for node in self.parent.keys():
            root = self.find(node)
            myDict[root].add(node)
        print("\nDisjoint sets: ")
        for mySet in myDict.values():
            print(mySet)
            
    def getDisjointSets(self):
        myDict = defaultdict(set)
        for node in self.parent.keys():
            root = self.find(node)
            myDict[root].add(node)
        #print("\nDisjoint sets: ")
        for mySet in myDict.values():
            yield mySet          
