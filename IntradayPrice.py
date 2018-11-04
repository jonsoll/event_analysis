# SimpleIntradayBarExample.py
from __future__ import print_function
from __future__ import absolute_import

import blpapi
import datetime
import pandas as pd
from optparse import OptionParser
from dateutil import tz


def parseCmdLine():

    parser = OptionParser(description="Retrieve reference data.")

    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")

    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)

    (options, args) = parser.parse_args()


    return options


def getPreviousTradingDate():
    tradedOn = datetime.date.today()

    while True:
        try:
            tradedOn -= datetime.timedelta(days=1)
        except OverflowError:
            return None

        if tradedOn.weekday() not in [5, 6]:
            return tradedOn

def getStartDate(eventDate, num_days):
    startDate = eventDate
    counter = 0

    while True:
        try:
            startDate -= datetime.timedelta(days=1)
        except OverflowError:
            return None

        if startDate.weekday() not in [5, 6]:
            counter += 1
        if counter == num_days:
            print('Start Date =', startDate)
            return startDate

def getEndDate(eventDate, num_days):
    endDate = eventDate
    counter = 0

    while True:
        try:
            endDate += datetime.timedelta(days=1)
        except OverflowError:
            return None

        if endDate.weekday() not in [5, 6]:
            counter += 1
        if counter == num_days:
            return endDate

def _utc_to_est(datetimes):
    return datetimes + datetime.timedelta(hours = -4)

def _to_dataframe(data):
    "Convert list of dicts of time-series price data to pandas dataframe"
    result = pd.DataFrame.from_dict(data)
    result['time'] = _utc_to_est(result['time'])
    return result.set_index('time')


def main(ticker, eventDateTime, daysPre=5, daysPost=5, interval=15):
    global options

    sessionOptions = blpapi.SessionOptions()

    try:
        options = parseCmdLine()
        # Fill SessionOptions
        host = options.host
        port = options.port
    except:
        host = 'localhost'
        port = 8194

    sessionOptions.setServerHost(host)
    sessionOptions.setServerPort(port)

    print("\n\nConnecting to %s:%d\n" % (host, port))

    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService("//blp/refdata"):
        print("Failed to open //blp/refdata")
        return

    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("IntradayBarRequest")
    request.set("security", ticker + " US Equity")
    request.set("eventType", "TRADE") # last price
    request.set("interval", interval)  # bar interval in minutes


    request.set("startDateTime", getStartDate(eventDateTime, daysPre))
    request.set("endDateTime"  , getEndDate(eventDateTime, daysPost))



    # print("Sending Request:", request)
    session.sendRequest(request)

    print('hello')
    try:

        # Process received events
        data = []
        while(True):
            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)

            flds = ['time', 'close', 'volume']

            for msg in ev:
                if msg.messageType() == 'IntradayBarResponse':
                    barTickData = msg.getElement('barData').getElement('barTickData')
                    for i in range(barTickData.numValues()):
                        data_bar = {}
                        for fld in flds:
                            dt = barTickData.getValue(i).getElement(0).getValue()
                            val = (barTickData.getValue(i).getElement(fld).getValue())
                            data_bar[fld] = val
                        data.append(data_bar)

            # Response completly received, so we could exit
            if ev.eventType() == blpapi.Event.RESPONSE:
                break

        return _to_dataframe(data)

    finally:
        # Stop the session
        session.stop()
        print('Session Closed')
