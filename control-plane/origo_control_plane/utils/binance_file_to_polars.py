import io
import zipfile

import polars as pl
import requests

REQUEST_TIMEOUT_SECONDS = 60


def binance_file_to_polars(url: str, has_header: bool) -> pl.DataFrame:
    """
    Create Polars DataFrame from Binance data file download.

    Args:
        url (str): URL referring to the Binance data file
        has_header (bool): Whether the file contains a header row

    Returns:
        pl.DataFrame: DataFrame with downloaded and parsed Binance data
    """

    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    zip_buf = io.BytesIO(response.content)
    z = zipfile.ZipFile(zip_buf)

    csv_filename = next(name for name in z.namelist() if name.lower().endswith('.csv'))

    with z.open(csv_filename) as csv_file:
        df = pl.read_csv(csv_file, has_header=has_header)

    return df
