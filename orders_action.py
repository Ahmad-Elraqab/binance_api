from models.order import Order


def setOrder(points_list):

    # print(points_list[order.symbol][order.interval])
    points_list[order.symbol][order.interval].sort()

    for point in points_list[order.symbol][order.interval]:
        if(float(point) > order.price):
            order.AtFront(float(point))
    points_list[order.symbol][order.interval].sort(reverse=True)

    for point in points_list[order.symbol][order.interval]:
        if(float(point) < order.price):
            order.AtEnd(float(point))

    # order.printResistance()
    # print('\t\t\t\t')
    # order.printSupport()


def analyzeOrder(current_price):

    element = None

    if current_price > order.stopLose.nextval.dataval:
        
        print("resistance changed")
        element = order.pop_front()

        order.addBack(element.dataval)
        
    elif current_price < order.stopLose.backval.dataval:

        print("support changed")
        element = order.pop_back()

        order.addFront(element.dataval)

    else:
        print(order.stopLose.nextval.dataval)
        print(order.stopLose.backval.dataval)


order = Order('ETHUSDT', '4hr', 2330.00, 2330.00, 0.05)
