from dotenv import load_dotenv
import os
import sys
import polars as pl
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
import time
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from utils.logger import get_logger
from utils.utils import paths

load_dotenv()
api_key = os.getenv("API_KEY")
ayer = datetime.now(timezone.utc).date() - timedelta(days=1)

logger = get_logger("Actualiza-Data")

logger.info(f"este es {api_key}")

urls_twelve = {
    "BTCUSD": f"https://api.twelvedata.com/time_series?symbol=BTC/USD&interval=1day&outputsize=7&apikey={api_key}",
    "EURUSD": f"https://api.twelvedata.com/time_series?symbol=EUR/USD&interval=1day&outputsize=7&apikey={api_key}",
    "XAUUSD": f"https://api.twelvedata.com/time_series?symbol=XAU/USD&interval=1day&outputsize=7&apikey={api_key}",
}

urls_investing = {
    "BTCUSD": "https://es.investing.com/crypto/bitcoin/historical-data",
    "EURUSD": "https://es.investing.com/currencies/eur-usd-historical-data",
    "SPX": "https://es.investing.com/indices/us-spx-500-historical-data",
    "US10Y": "https://es.investing.com/rates-bonds/u.s.-10-year-bond-yield-historical-data",
    "USDX": "https://es.investing.com/indices/usdollar-historical-data",
    "XAUUSD": "https://es.investing.com/currencies/xau-usd-historical-data"
}

urls_marketwatch = {
    "USDX": "https://www.marketwatch.com/investing/index/DXY/download-data"
}

symbols_yfinance = {
    "EURUSD": "EURUSD=X",
    "SPX": "^GSPC",
    "US10Y": "^TNX",
    "XAUUSD": "GC=F"
}


def redondear_ohlc(df: pl.DataFrame, decimales: int):
    columns = [x for x in df.columns if x not in ["date", "symbol"]]
    for column in columns:
        df = df.with_columns(pl.col(column).round(decimals=decimales))
    return df


def extraer_yfinance(symbol: str) -> pl.DataFrame:
    logger.info(f"Llamado a la Api de Yfinance para el Symbol {symbol}")
    time.sleep(10)
    symbol_api = symbols_yfinance[symbol]
    try:
        ticker = yf.Ticker(symbol_api)
        data = ticker.history(period="5d", interval="1d")
        # data.index = data.index.tz_convert("UTC")
        df = pl.DataFrame(data.reset_index())
        df = df.with_columns(pl.lit(symbol).alias("symbol"))
        return df
    except Exception as e:
        logger.error(f"Error comunicando con yfinance {e}")
    df = pl.DataFrame()
    return df


def extraer_twelve(symbol: str) -> pl.DataFrame:
    # ayer = datetime.now(timezone.utc).date() - timedelta(days=2)
    logger.info(f"Llamado a la Api de TwelveData para el Symbol {symbol}")
    time.sleep(10)
    ayer_str = ayer.strftime("%Y-%m-%d")
    try:
        response = requests.get(urls_twelve[symbol])
        response.raise_for_status()  # Lanza excepción si el código no es 200
        diccionarios = response.json()["values"]
        for dicionario in diccionarios:
            if dicionario["datetime"] == ayer_str:
                ohlc = {
                    "date": ayer,
                    "open": dicionario["open"],
                    "high": dicionario["high"],
                    "low": dicionario["low"],
                    "close": dicionario["close"],
                    "symbol": symbol
                }
                return pl.DataFrame(ohlc)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error en la Comunicacion con Api TwelveData{e}")
    df = pl.DataFrame()
    return df


def extraer_marketwatch(symbol: str) -> pl.DataFrame:
    logger.info(f"Webscraping a Marketwatch para el Symbol {symbol}")
    url = urls_marketwatch[symbol]
    ayer = datetime.now(timezone.utc).date() - timedelta(days=1)
    ayer_str = ayer.strftime("%m/%d/%Y")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "es-ES,es;q=0.9",
        "Referer": "https://www.marketwatch.com/",
    }
    try:
        response = requests.get(url, headers=headers)
        time.sleep(5)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # $x("//div[@class="column column--full j-downloaddata"]/div[1]/mw-tabs/div[2]/div[1]/mw-downloaddata/div//tbody")
        tabla = soup.select_one(
            "div.column.column--full.j-downloaddata > div > mw-tabs > div:nth-of-type(2) > div > mw-downloaddata > div tbody"
        )
        if not tabla:
            logger.error("No se encontró la tabla en el HTML")
            return pl.DataFrame()

        filas = tabla.find_all("tr")
        for fila in filas:
            columnas = fila.find_all("td")
            if len(columnas) < 5:
                continue

            fecha_str = columnas[0].find("div").text

            if fecha_str == ayer_str:
                open_ = columnas[1].find("div").text
                high = columnas[2].find("div").text
                low = columnas[3].find("div").text
                close = columnas[4].find("div").text

                df = pl.DataFrame([{
                    "date": ayer,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "symbol": symbol
                }])
                return df
        logger.warning(f"No se encontró la fila para la fecha {ayer}")
        return pl.DataFrame()

    except requests.exceptions.RequestException as e:
        logger.error(f"Error en respuesta del Servidor de Investing {e}")

    return pl.DataFrame()


def extraer(symbol: str) -> pl.DataFrame:
    if symbol in ['BTCUSD', 'EURUSD', 'XAUUSD']:
        return extraer_twelve(symbol)

    if symbol == 'USDX':
        return extraer_marketwatch(symbol=symbol)
        # return extraer_investing(symbol)

    return extraer_yfinance(symbol)


def transformar(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    if df.shape[0] == 0:
        return df
    columns = [x for x in df.columns if x not in ["date", "symbol"]]
    if symbol in ["BTCUSD", "EURUSD", "XAUUSD"]:
        for column in columns:
            df = df.with_columns(pl.col(column).cast(pl.Float64))
            if symbol == "EURUSD":
                df = df.with_columns(pl.col(column).round(decimals=4))
        return df

    if symbol in ["SPX", "US10Y"]:
        df = df.with_columns(pl.col("Date").dt.date().alias("date"))
        df = df.select(["date", "Open", "High", "Low", "Close", "symbol"])
        df.columns = ["date", "open", "high", "low", "close", "symbol"]
        mask = df["date"] == ayer
        if symbol == "SPX":
            df = redondear_ohlc(df=df, decimales=2)
        return df.filter(mask)

    for column in columns:
        df = df.with_columns(pl.col(column).cast(pl.Float64))
    return df


def persistir(df: pl.DataFrame, symbol: str):
    if df.shape[0] == 0:
        return
    historico = pl.read_parquet(paths[symbol])
    final = pl.concat([historico, df], how="vertical")
    clave = "date"
    final = final.unique(subset=clave, keep="last").sort("date")
    final.write_parquet(paths[symbol])
    return


def main():
    symbols = ["BTCUSD", "EURUSD", "XAUUSD", "SPX", "US10Y", "USDX"]
    if datetime.now(timezone.utc).date().weekday() in [0, 6]:
        symbols = ["BTCUSD"]

    for symbol in symbols:
        (
            extraer(symbol=symbol)
            .pipe(lambda df: transformar(df=df, symbol=symbol))
            .pipe(lambda df: persistir(df=df, symbol=symbol))
        )
    sys.exit(0)


if __name__ == "__main__":
    main()
