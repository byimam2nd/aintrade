
import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
import os

# Add the root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.base_symbols_models import Symbol

def run_strategy():
    """
    A simple strategy that reads all symbols from the database and prints them.
    """
    # Load config
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    db_url = config.get("database", "url")

    # Database setup
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Query all symbols
        symbols = session.query(Symbol).all()

        print("--- Running Simple Strategy ---")
        for symbol in symbols:
            print(f"Symbol: {symbol.symbol}, Last Price: {symbol.last_price}")
        print("--- Strategy Finished ---")

    except Exception as e:
        print(f"Error running strategy: {e}")
    finally:
        session.close()

if __name__ == '__main__':
    run_strategy()
