import requests
from driftpy.constants import (
    QUOTE_PRECISION,
    BASE_PRECISION,
    PRICE_PRECISION,
)

def snake_to_camel_df(df):
    """Convert DataFrame column names from snake_case to camelCase"""

    def camel_case(word):
        components = word.split("_")
        return components[0] + "".join(x.title() for x in components[1:])

    new_columns = {col: camel_case(col) for col in df.columns}
    return df.rename(columns=new_columns)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
