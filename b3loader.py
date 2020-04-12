import pyarrow.parquet as pq
import time

start = time.time()
df = pq.ParquetDataset('./data/parsed', filters=[('date', '=', '2019-11-01'), ('mdSymbol', '=', 'ABEV3')]).read().to_pandas()
# df = pq.ParquetDataset('./data/parsed').read().to_pandas()
delta = time.time() - start
print("Done in : {}s".format(delta))
