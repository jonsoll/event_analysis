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
            return endDate.date()

def _utc_to_est(dt):
    return dt + datetime.timedelta(hours = -4)

def _est_to_utc(dt):
    return dt + datetime.timedelta(hours = 4)

def _to_dataframe(data, ticker, eventDatetime):
    "Convert list of dicts of time-series price data to pandas dataframe"
    result = pd.DataFrame.from_dict(data)
    result['time']          = [_utc_to_est(x) for x in result['time']]
    result['ticker']        = ticker
    result['eventDatetime'] = eventDatetime
    return result.set_index('time')


def request_price_data(tickers, eventDatetimes):
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

    request.set("eventType", "TRADE") # last price
    request.set("interval", 1)  # bar interval in minutes

    final_result = []
    for ticker, eventDatetime in zip(tickers, eventDatetimes):
        print('Pulling data for event that happened at %s with %s' % (eventDatetime, ticker))
        request.set("startDateTime", eventDatetime)
        request.set("endDateTime"  , getEndDate(eventDatetime, 10))
        request.set("security", ticker + " Equity")
        session.sendRequest(request)

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
                                dt = barTickData.getValue(i).getElement(0).getValue()       # dt is in utc
                                val = (barTickData.getValue(i).getElement(fld).getValue())
                                data_bar[fld] = val
                            # print('dt: %s -- eventdt: %s' % (dt, _est_to_utc(eventDatetime)))
                            if dt >= _est_to_utc(eventDatetime):                            # to compare utc to utc
                                data.append(data_bar)

                # Response completly received, so we could exit
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break
            final_result.append(_to_dataframe(data, ticker, eventDatetime))

        except Exception as e:
            print('Error:', e)

    # Stop the session
    session.stop()
    print('Session Closed')
    return final_result
