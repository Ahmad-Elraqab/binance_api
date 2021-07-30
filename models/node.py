
class Node:
    def __init__(self, dataval=None):
        self.dataval = dataval
        self.nextval = None
        self.backval = None
        self.isTested = None
        self.isUpBroken = None
        self.isDownBroken = None
