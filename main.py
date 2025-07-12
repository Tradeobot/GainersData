from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import yfinance as yf
import redis
import argparse
import threading
import time

# For Debugging Purposes
DEBUG_QUERY_THREAD = True

# Other Constants
QUERY_TICK_RATE = 10.0    # Seconds

# Placeholder for Redis client if needed in the future
redis_client = None

def IsMarketOpen() -> bool:
    """
    Checks if the stock market is currently open
    """
    try:
        # Use yfinance to get market status
        market      = yf.Market("US", timeout=1)
        status: str = market.status.get("status")
        if status is None:
            return False

        # Check if the market status indicates it is closed
        if status.upper() == "CLOSED":
            return False

        return True

    except Exception as e:
        print(f"Error checking market status: {e}")
        return False

def InTradingHours() -> bool:
    """
    Checks if the current time is within trading hours
    """

    # Get the current time in the Eastern Time Zone
    now = datetime.now(ZoneInfo("America/New_York"))

    # Check if today is a weekend
    if now.weekday() >= 5:
        return False
    
    # Define market open and close times
    market_open  = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

    return market_open <= now <= market_close

def GetTopGainers(count: int, percent_change: float, intraday_price: float, volume: int,
                  intraday_market_cap: int = None, us_only: bool = True) -> list[dict]:
    """
    Fetches the top gainer stocks from Yahoo Finance

    :param count: Number of top gainers to fetch.
    :param percent_change: Minimum percentage change to filter gainers.
    :param intraday_price: Minimum intraday price to filter gainers.
    :param volume: Minimum trading volume to filter gainers.
    :param intraday_market_cap: Minimum intraday market cap to filter gainers.
    :param us_only: If True, only fetch US stocks.
    :return: A list of dictionaries containing ticker symbols and their acquisition timestamps.
    """
    # Create the query elements based on the provided parameters
    elements = [
        yf.EquityQuery("GT", ["percentchange", percent_change]),
        yf.EquityQuery("GTE", ["intradayprice", intraday_price]),
        yf.EquityQuery("GT", ["dayvolume", volume])
    ]

    # Add optional filters to the query
    if us_only:
        elements.append(yf.EquityQuery("EQ", ["region", "us"]))

    # If intraday_market_cap is provided, add it to the query
    if intraday_market_cap is not None:
        elements.append(yf.EquityQuery("GTE", ["intradaymarketcap", intraday_market_cap]))

    # Combine the elements into a single query
    query = yf.EquityQuery("AND", elements)

    try:
        # Fetch the top gainers using the query and count
        results: dict          = yf.screen(query, count=count, sortField="percentchange")
        collection: list[dict] = results.get("quotes", [])
        dt                     = datetime.now(ZoneInfo("America/New_York"))
        return [{
                "symbol"            : item.get("symbol"),
                "timestamp"         : int(time.time()),
                "day"               : dt.strftime("%A"),
                "datetime_iso"      : dt.isoformat(),
                "datetime_readable" : dt.strftime("%m-%d-%y %I:%M:%S.%f %p %Z")
        } for item in collection]

    except Exception as e:
        print(f"Error fetching top gainers: {e}")
        return []

def QueryThread() -> None:
    """
    Queries stock data and maintains a collection of top gainer stocks
    """

    # When starting the thread lets align the querying of the endpoints with 
    # the interval or tick rate of the query thread
    full_time  = time.time()
    seconds    = int(full_time)
    fractional = full_time - seconds
    remainder  = seconds % int(QUERY_TICK_RATE)

    # Sleep until it becomes the desired time
    time.sleep(QUERY_TICK_RATE - remainder - fractional)

    # Gainers Data
    todays_gainers: list[dict] = []

    while True:

        # Main Processing
        #-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
        # Only query for top gainers during trading hours
        if InTradingHours():

            # Only query for top gainers if the market is open
            if IsMarketOpen():

                # Query for the top gainers
                gainers = GetTopGainers(
                    count=10,
                    percent_change=10.0,
                    intraday_price=0.2,
                    volume=100000
                )

                # Update the gainers data
                todays_symbols = [g.get("symbol") for g in todays_gainers]
                for gainer in gainers:
                    if gainer.get("symbol") not in todays_symbols:
                        todays_gainers.append(gainer)

                # TODO: Update the todays_gainers entries stored in redis, with the new data

        # Outside of trading hours
        elif not InTradingHours() and len(todays_gainers) > 0:

            # TODO: Update the total gainers data stored in redis, with the new data
            
            # Clear the gainers data at the end of the trading day
            todays_gainers.clear()

        # Outside of trading hours and todays_gainers is empty
        else:
            
            # Now we can just wait until the next market opening
            current_time  = datetime.now(ZoneInfo("America/New_York"))
            next_opening  = (current_time + timedelta(days=1)).replace(hour=9, minute=25, second=0, microsecond=0)
            time_to_sleep = (next_opening - current_time).total_seconds()

            # Sleep until the right before the next market opening
            time.sleep(time_to_sleep)

        #-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
        # Worst case scenario for how long the code in the loop we're in to take is well below
        # the tick rate of the application, therefore we don't really need to time the duration
        # the processor took to execute this loop, which means that we can just sleep until the
        # the next moment in time that is divisible by the tick rate
        full_time  = time.time()
        seconds    = int(full_time)
        fractional = full_time - seconds
        remainder  = seconds % int(QUERY_TICK_RATE)

        # Sleep for the remainder amount of time till the next tick of the loop
        time.sleep(QUERY_TICK_RATE - remainder - fractional)

def main() -> None:
    """
    Main entry point of the application
    """

    parser = argparse.ArgumentParser(description="Maintains a collection of top gainer stocks")
    parser.add_argument("--ip", type=str, default="127.0.0.1", help="IP address to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind to")

    # Attempt to parse the input arguments of the application
    try:
        args = parser.parse_args()

    except argparse.ArgumentError as e:
        print(f"Argument Parsing Failed - {e}")
        return
    
    # Initialize Redis Client
    global redis_client
    redis_client = redis.Redis(host=args.ip, port=args.port, db=0)

    if DEBUG_QUERY_THREAD:
        QueryThread()
    else:
        # Create the query thread and start it
        query_thread = threading.Thread(target=QueryThread, daemon=True)
        query_thread.start()

    return

if __name__ == "__main__":
    main()
