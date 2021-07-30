
from models.node import Node


class Order:
    def __init__(self, symbol, interval, price, stopLose, sellPercent, ):
        self.price = price
        self.stopLose = Node(stopLose)
        self.sellPercent = sellPercent
        self.symbol = symbol
        self.interval = interval

    def sell():
        print("sell now")

    def buy():
        print("buy now")

    def AtFront(self, newdata):
        NewNode = Node(newdata)
        if self.stopLose is None:
            self.stopLose = NewNode
            return
        laste = self.stopLose
        while(laste.nextval):
            laste = laste.nextval
        laste.nextval = NewNode

    def pop_front(self):
        if(self.stopLose != None):

            temp = self.stopLose.nextval

            head = self.stopLose

            head.nextval = self.stopLose.nextval.nextval

            temp.nextval = None
            temp.isUpBroken = True

            return temp

    def pop_back(self):
       if(self.stopLose != None):

            temp = self.stopLose.backval

            head = self.stopLose

            head.backval = self.stopLose.backval.backval

            temp.backval = None
            temp.isDownBroken = True

            return temp

    def addBack(self, newdata):
        NewNode = Node(newdata)
        head = self.stopLose

        NewNode.backval = head.backval

        head.backval = NewNode

        self.stopLose = head

    def addFront(self, newdata):
        NewNode = Node(newdata)
        head = self.stopLose

        NewNode.nextval = head.nextval

        head.nextval = NewNode

        self.stopLose = head

        

    def AtEnd(self, newdata):
        NewNode = Node(newdata)
        if self.stopLose is None:
            self.stopLose = NewNode
            return
        laste = self.stopLose
        while(laste.backval):

            laste = laste.backval

        laste.backval = NewNode

    def printResistance(self):
        printval = self.stopLose
        while printval is not None:
            print(printval.dataval)
            printval = printval.nextval

    def printSupport(self):
        printval = self.stopLose
        while printval is not None:
            print(printval.dataval)
            printval = printval.backval
