from ftplib import FTP
from datetime import datetime
from typing import AnyStr
import urllib.request as request
from contextlib import closing
from datetime import datetime
import gzip


def download_b3_file(filename:str) -> str:
    file_path = 'ftp://ftp.bmf.com.br/MarketData/Bovespa-Vista/{}'.format(filename)
    print('Downloading: {}'.format(file_path))
    with closing(request.urlopen(file_path)) as r:
        with gzip.open(r) as zip_file:
            return zip_file.read()


def download_al_files():
    b3_ftp = FTP('ftp.bmf.com.br')
    b3_ftp.login()
    b3_ftp.cwd('MarketData/Bovespa-Vista/')
    for file in b3_ftp.nlst():
        yield download_b3_file(file)


