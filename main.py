from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import yfinance as yf
import redis
import argparse
import time
import json

# Other Constants
QUERY_TICK_RATE = 10.0    # Seconds

# Placeholder for Redis client if needed in the future
redis_client: redis.Redis = None

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

                # For debugging
                if int(time.time()) % 3600 == 0:
                    print("Market is open, querying for top gainers...")

                # Query for the top gainers
                gainers = GetTopGainers(
                    count=10,
                    percent_change=10.0,
                    intraday_price=0.2,
                    volume=100000
                )

                # Update the gainers data locally with any new gainers
                todays_symbols = [g.get("symbol") for g in todays_gainers]
                for gainer in gainers:

                    # Only add the gainer if it is not already in the list
                    if gainer.get("symbol") not in todays_symbols:
                        todays_gainers.append(gainer)

                # Overwrite the todays_gainers entries stored in redis, with the new data
                redis_client.set("todays_gainers", json.dumps(todays_gainers))

        # Outside of trading hours
        elif not InTradingHours() and len(todays_gainers) > 0:

            # For debugging
            print("Trading day has ended, saving today's gainers to the gainers record.")

            gainers_record = redis_client.get("gainers_record")
            if gainers_record is None:

                # If we don't have a gainers record yet, set it to todays_gainers
                redis_client.set("gainers_record", json.dumps(todays_gainers))
            else:
                # If we already have a gainers record, we can append to it
                existing_gainers: list[dict] = json.loads(gainers_record)

                # Check if we have already recorded gainers for this day of this week
                day_of_the_week = datetime.now(ZoneInfo("America/New_York")).strftime("%A")
                if any(day_of_the_week in g for g in existing_gainers):

                    # If we have already recorded gainers for this day, remove them
                    existing_gainers = [g for g in existing_gainers if day_of_the_week not in g]

                # Append today's gainers to the existing gainers record
                existing_gainers += todays_gainers

                # Update the gainers record in redis
                redis_client.set("gainers_record", json.dumps(existing_gainers))

            # Clear the gainers data at the end of the trading day
            todays_gainers.clear()

        # Outside of trading hours and todays_gainers is empty, just report status
        else:

            # Tell redis that this applications is sleeping until the next market opening
            redis_client.set("status", json.dumps([
                "sleeping",
                {
                    "since_timestamp" : int(time.time()),
                    "since_iso"       : datetime.now(ZoneInfo("America/New_York")).isoformat(),
                    "since_readable"  : datetime.now(ZoneInfo("America/New_York")).strftime("%m-%d-%y %I:%M:%S.%f %p %Z")
                }
            ]))

            if datetime.now(ZoneInfo("America/New_York")).weekday() >= 5:
                # If it's the weekend, sleep until the next Monday at 9:25 AM
                current_time  = datetime.now(ZoneInfo("America/New_York"))
                days_ahead    = 7 - current_time.weekday()
                next_opening  = (current_time + timedelta(days=days_ahead)).replace(hour=9, minute=25, second=0, microsecond=0)
                time_to_sleep = (next_opening - current_time).total_seconds()
            else:
                # Now we can just wait until the next market opening
                current_time  = datetime.now(ZoneInfo("America/New_York"))
                next_opening  = (current_time + timedelta(days=1)).replace(hour=9, minute=25, second=0, microsecond=0)
                time_to_sleep = (next_opening - current_time).total_seconds()

            # For debugging
            print(f"Sleeping for {time_to_sleep} seconds until next market opening.")

            # Sleep until the right before the next market opening
            time.sleep(time_to_sleep)

        # If we made it here, then the application is alive and well
        redis_client.set("status", json.dumps([
            "alive",
            {
                "since_timestamp" : int(time.time()),
                "since_iso"       : datetime.now(ZoneInfo("America/New_York")).isoformat(),
                "since_readable"  : datetime.now(ZoneInfo("America/New_York")).strftime("%m-%d-%y %I:%M:%S.%f %p %Z")
            }
        ]))

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
    parser.add_argument("--password", type=str, required=True, help="Password for Redis")

    # Attempt to parse the input arguments of the application
    try:
        args = parser.parse_args()

    except argparse.ArgumentError as e:
        print(f"Argument Parsing Failed - {e}")
        return
    
    # Initialize Redis Client
    global redis_client
    redis_client = redis.Redis(host=args.ip, port=args.port, db=0, password=args.password)

    # For debugging
    print(f"Connected to Redis at {args.ip}:{args.port}")

    # Start the Query Thread
    QueryThread()

    return

if __name__ == "__main__":
    main()
