from typing import AnyStr

import pandas as pd
import yaml
import pyarrow.parquet as pq
import pyarrow as pa

from b3download import download_al_files


def read_columns() -> [str]:
    with open('header.yaml') as headers:
        return yaml.load(headers, Loader=yaml.FullLoader).keys()


def read_dtype() -> {}:
    with open('header.yaml') as headers:
        return yaml.load(headers, Loader=yaml.FullLoader)


def read_file(file: AnyStr) -> pd.DataFrame:
    file_data = []
    for line in file:
        row = line.split(';')
        if len(row) == 18:
            file_data.append(row)
    return pd.DataFrame(file_data, columns=read_columns()).astype(dtype=read_dtype())


def load_all_files_and_store_parquet():
    for loaded_file in download_al_files():
        df = read_file(loaded_file.decode("utf-8").split('\n'))
        df['dt'] = df['date'].dt.date
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        pq.write_to_dataset(pa.Table.from_pandas(df), './data/parsed', partition_cols=['dt', 'mdSymbol'],
                            compression='gzip')


load_all_files_and_store_parquet()