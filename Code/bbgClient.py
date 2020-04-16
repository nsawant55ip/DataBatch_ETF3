import socket
import sys
import pickle
import datetime

HOST = "WS36"
PORT = 9999


def sendAndReceive(data):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket (SOCK_STREAM means a TCP socket)
    try:
        sock.connect((HOST, PORT))  # Connect to server and send data
    except Exception as e:
        print('Error : %s' % e)
    # sock.send(data + "\n")
    pickle.dump(data, sock.makefile('wb'), 2)
    # print "SENT: %s request" % data['name']
    # Receive data from the server and shut down
    # received = sock.recv(1024)
    received = sock.makefile('rb')
    #output = pickle.load(received)
    output = pickle.load(received,encoding = 'latin-1')
    sock.close()
    return output


def remoteBbgLatestPriceQuery(name, tickers, startDate, endDate=None, period='DAILY',
                              adjSplit=True, ret=False, periodAdjust='ACTUAL'):
    data = {}
    data['name'] = name
    data['tickers'] = tickers
    data['startDate'] = startDate
    if endDate is not None:
        data['endDate'] = endDate
    data['period'] = period
    data['adjSplit'] = adjSplit
    data['ret'] = ret
    data['periodAdjust'] = periodAdjust
    return sendAndReceive(data)


def remoteBbgHistoricalQuery(name, tickers, fields, startDate, endDate=None, period='DAILY',
                             adjSplit=True, ret=False, periodAdjust='ACTUAL'):
    data = {}
    data['name'] = name
    data['fields'] = fields
    data['tickers'] = tickers
    data['startDate'] = startDate
    if endDate is not None:
        data['endDate'] = endDate
    data['period'] = period
    data['adjSplit'] = adjSplit
    data['ret'] = ret
    data['periodAdjust'] = periodAdjust
    return sendAndReceive(data)


def remoteBbgReferenceData(name, tickers, fields):
    data = {}
    data['name'] = name
    data['tickers'] = tickers
    data['fields'] = fields
    return sendAndReceive(data)


def main():
    # This section is for testing the connect and receive
    print(repr(remoteBbgReferenceData('Corporate Actions',
                                         ['XLK US Equity'],
                                         ['etf_undl_index_ticker'])))


if __name__ == "__main__":
    main()
