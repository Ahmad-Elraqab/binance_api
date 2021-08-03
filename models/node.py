
class Node:
    def __init__(self, dataval=None):
        self.dataval = dataval
        self.nextval = None
        self.backval = None
        self.isTested = None
        self.isUpBroken = None
        self.isDownBroken = None

    def push(self, data):

        index = 0
        head = self
        new = Node(data)
        new.nextval = head

        new.nextval.nextval.nextval = None

        self = new
