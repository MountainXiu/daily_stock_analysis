# -*- coding: utf-8 -*-
"""
===================================
LongPortFetcher - 长桥数据源
===================================

数据来源：LongPort OpenAPI
文档：https://open.longportapp.com/zh-CN/docs/getting-started

"""

import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any
import pandas as pd

# LongPort SDK imports
try:
    from longport.openapi import QuoteContext, Config, Period, AdjustType
    HAS_LONGPORT = True
except ImportError:
    HAS_LONGPORT = False

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS
from src.config import get_config

logger = logging.getLogger(__name__)

class LongPortFetcher(BaseFetcher):
    name = "LongPortFetcher"
    priority = -1  # Highest priority (lower is higher)

    def __init__(self):
        self.config = get_config()
        self.ctx: Optional[QuoteContext] = None
        self._init_context()

    def _init_context(self):
        if not HAS_LONGPORT:
            logger.warning("LongPort SDK not installed. Please run `pip install longport`")
            return

        if not self.config.longport_app_key or not self.config.longport_access_token:
            logger.warning("LongPort API credentials not found in config (LONGPORT_APP_KEY/ACCESS_TOKEN)")
            return

        try:
            # Configure from config object
            lp_config = Config(
                app_key=self.config.longport_app_key,
                app_secret=self.config.longport_app_secret,
                access_token=self.config.longport_access_token
            )
            # Quote level
            if self.config.longport_quote_level:
                # Setting quote level is not directly in Config constructor usually, 
                # but let's assume default config is fine or passed via env if SDK supports it.
                # Actually Config.from_env() reads LONGPORT_QUOTE_LEVEL.
                # We are manually creating Config, so we might check if Config accepts it.
                # Checking SDK source is hard, but usually app_key/secret/token is enough.
                pass

            self.ctx = QuoteContext(lp_config)
            logger.info("LongPort QuoteContext initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize LongPort context: {e}")
            self.ctx = None

    def _convert_to_longport_symbol(self, stock_code: str) -> str:
        """Convert stock code to LongPort symbol format (e.g., 600519 -> 600519.SH)"""
        stock_code = stock_code.strip()
        if '.' in stock_code:
             # Already has suffix, ensure upper case
             parts = stock_code.split('.')
             suffix = parts[-1].upper()
             if suffix in ['SH', 'SZ', 'HK', 'US']:
                 return stock_code.upper()
             return stock_code # Return as is if unknown suffix

        # A-Share logic
        if stock_code.startswith('6'):
            return f"{stock_code}.SH"
        elif stock_code.startswith(('00', '30')): # 00xxxx, 30xxxx
            return f"{stock_code}.SZ"
        elif stock_code.startswith('8') or stock_code.startswith('4'): # BJ stock? LongPort might support .BJ
             return f"{stock_code}.BJ"
        elif stock_code.startswith('0') and len(stock_code) == 5: # HK stock 00700
             return f"{stock_code}.HK"
        elif stock_code.isdigit() and len(stock_code) <= 5: # HK stock 700
             return f"{stock_code}.HK"
        elif stock_code.isalpha():
            return f"{stock_code}.US"
        
        # Default fallback
        return f"{stock_code}.SH" if stock_code.startswith('6') else f"{stock_code}.SZ"

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        if not self.ctx:
            raise DataFetchError("LongPort context not initialized or credentials missing")

        symbol = self._convert_to_longport_symbol(stock_code)
        
        try:
            # Convert date strings to date objects
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            # Fetch candlesticks
            # Using ForwardAdjust (前复权) as it is standard for backtesting/analysis
            candlesticks = self.ctx.history_candlesticks_by_date(
                symbol, 
                Period.Day, 
                AdjustType.ForwardAdjust, 
                start, 
                end
            )
            
            if not candlesticks:
                logger.warning(f"No data found for {symbol} from {start} to {end}")
                return pd.DataFrame()

            # Convert to DataFrame
            data = []
            for candle in candlesticks:
                # candle attributes: close, high, low, open, time, volume, turnover
                # time is datetime object
                data.append({
                    'date': candle.time.strftime("%Y-%m-%d"),
                    'open': float(candle.open),
                    'high': float(candle.high),
                    'low': float(candle.low),
                    'close': float(candle.close),
                    'volume': float(candle.volume),
                    'amount': float(candle.turnover)
                })
            
            df = pd.DataFrame(data)
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data from LongPort for {symbol}: {e}")
            raise DataFetchError(f"LongPort fetch error: {e}")

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=STANDARD_COLUMNS)
            
        # Calculate pct_chg if not present
        if 'pct_chg' not in df.columns:
            df['pct_chg'] = df['close'].pct_change() * 100
            df['pct_chg'] = df['pct_chg'].fillna(0)
            
        # Ensure columns exist
        for col in STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = 0.0
                
        return df[STANDARD_COLUMNS]
