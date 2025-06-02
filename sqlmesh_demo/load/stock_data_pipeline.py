import dlt
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Iterator, Dict, Any
import os

def read_symbols() -> Iterator[str]:
    """Generator that reads and yields stock symbols from a file."""
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'symbols.txt')
    try:
        with open(file_path, 'r') as file:
            for symbol in file:
                symbol = symbol.strip()
                if symbol:  # Skip empty lines
                    yield symbol
    except FileNotFoundError:
        print(f"File {file_path} not found.")

def validate_symbol(symbol: str) -> bool:
    """Validates a symbol using yfinance."""
    try:
        stock = yf.Ticker(symbol)
        return not stock.history(period='1d').empty
    except Exception as e:
        print(f"Error validating symbol {symbol}: {e}")
        return False

@dlt.resource(
    primary_key="Symbol",
    write_disposition="replace",
    table_name="stock_info"
)
def stock_info_resource() -> Iterator[Dict[str, Any]]:
    """Resource that fetches stock information for each symbol."""
    for symbol in read_symbols():
        try:
            stock = yf.Ticker(symbol)
            # Validate symbol by checking if we can get history
            if not stock.history(period='1d').empty:
                info = stock.info
                info['Symbol'] = symbol  # Add symbol column
                yield info
            else:
                print(f"Invalid symbol: {symbol}")
        except Exception as e:
            print(f"Error processing symbol {symbol}: {e}")

@dlt.resource(
    primary_key=["Symbol", "Expiration", "strike", "Type", "contractSymbol"],
    write_disposition="replace",
    table_name="stock_options"
)
def stock_options_resource() -> Iterator[Dict[str, Any]]:
    """Resource that fetches option chain data for each symbol."""
    for symbol in read_symbols():
        try:
            stock = yf.Ticker(symbol)
            exp_dates = stock.options
            
            if not exp_dates:
                print(f"No option data available for {symbol}")
                continue

            for exp in exp_dates:
                print(f"Fetching options for {symbol} expiring on {exp}")
                try:
                    options = stock.option_chain(exp)
                    
                    # Process calls
                    for _, row in options.calls.iterrows():
                        option_data = row.to_dict()
                        option_data['Type'] = 'Call'
                        option_data['Expiration'] = exp
                        option_data['Symbol'] = symbol
                        yield option_data
                    
                    # Process puts
                    for _, row in options.puts.iterrows():
                        option_data = row.to_dict()
                        option_data['Type'] = 'Put'
                        option_data['Expiration'] = exp
                        option_data['Symbol'] = symbol
                        yield option_data
                
                except Exception as e:
                    print(f"Error fetching options for {symbol} expiring on {exp}: {e}")
                    continue
        
        except Exception as e:
            print(f"Error processing symbol {symbol}: {e}")

@dlt.resource(
    primary_key=["Symbol", "Date"],
    write_disposition="replace",
    table_name="stock_history"
)
def stock_history_resource() -> Iterator[Dict[str, Any]]:
    """Resource that fetches historical stock data for each symbol."""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=360)).strftime('%Y-%m-%d')

    for symbol in read_symbols():
        if validate_symbol(symbol):
            print(f"Fetching data for: {symbol}")
            try:
                # Fetch and process the data
                stock_data = yf.download(symbol, start=start_date, end=end_date)
                
                # Convert DataFrame to records with date handling
                records = []
                for date, row in stock_data.iterrows():
                    record = {
                        'Date': date.strftime('%Y-%m-%d'),
                        'Open': float(row['Open'].iloc[0]) if isinstance(row['Open'], pd.Series) else float(row['Open']),
                        'High': float(row['High'].iloc[0]) if isinstance(row['High'], pd.Series) else float(row['High']),
                        'Low': float(row['Low'].iloc[0]) if isinstance(row['Low'], pd.Series) else float(row['Low']),
                        'Close': float(row['Close'].iloc[0]) if isinstance(row['Close'], pd.Series) else float(row['Close']),
                        'Volume': int(row['Volume'].iloc[0]) if isinstance(row['Volume'], pd.Series) else int(row['Volume']),
                        'Symbol': symbol
                    }
                    records.append(record)
                
                # Yield each record
                for record in records:
                    yield record
            
            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")
        else:
            print(f"Invalid symbol: {symbol}")

def load_all_stock_data():
    """Creates and runs the combined stock data pipeline."""
    pipeline = dlt.pipeline(
        pipeline_name='stock_data',
        destination='motherduck',
        dataset_name='stock_data'
    )

    # Load all three types of data
    info_load = pipeline.run(stock_info_resource())
    print("Stock Info Load Results:")
    print(info_load)

    options_load = pipeline.run(stock_options_resource())
    print("\nStock Options Load Results:")
    print(options_load)

    history_load = pipeline.run(stock_history_resource())
    print("\nStock History Load Results:")
    print(history_load)

if __name__ == "__main__":
    load_all_stock_data()
