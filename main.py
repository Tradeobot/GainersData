from flask import Flask, Response, jsonify
import yfinance as yf
import argparse
import threading

# For Debugging Purposes
DEBUG_QUERY_THREAD = False

# Initialize Flask app
app = Flask(__name__)

def IsMarketOpen() -> bool:
    """
    Checks if the stock market is currently open
    """
    # Placeholder implementation
    return True

def QueryThread() -> None:
    """
    Queries stock data and maintains a collection of top gainer stocks
    """
    while True:
        # Query stock data and update the collection of top gainers
        pass

def main() -> None:
    """
    Main entry point of the application
    """

    parser = argparse.ArgumentParser(description="Maintains a collection of top gainer stocks")
    parser.add_argument("--ip", type=str, default="127.0.0.1", help="IP address to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind to")
    parser.add_argument("--o", type=str, required=True, help="Output folder path")

    # Attempt to parse the input arguments of the application
    try:
        args = parser.parse_args()

    except argparse.ArgumentError as e:
        print(f"Argument Parsing Failed - {e}")
        return
    
    if DEBUG_QUERY_THREAD:
        QueryThread()
    else:
        # Create the query thread and start it
        query_thread = threading.Thread(target=QueryThread, daemon=True)
        query_thread.start()

        # Start the Flask server if the query thread is alive
        if query_thread.is_alive():
            app.run(host=args.ip, port=args.port)

    return

if __name__ == "__main__":
    main()
