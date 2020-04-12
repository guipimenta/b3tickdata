from io import BytesIO
from ftplib import FTP
import urllib.request as request
from contextlib import closing
import py7zlib
import gzip
from zipfile import ZipFile


def decode(loaded_bytes: bytes) -> [str]:
    return loaded_bytes.decode("utf-8").split('\n')


def download_b3_file(filename: str) -> [str]:
    file_path = 'ftp://ftp.bmf.com.br/MarketData/Bovespa-Vista/{}'.format(filename)
    print('Downloading: {}'.format(file_path))
    with closing(request.urlopen(file_path)) as r:
        if '7z' in filename:
            archive = BytesIO(r.read())
            arquive_7z = py7zlib.Archive7z(archive)
            return decode(arquive_7z.getmember(0).read())
        elif 'gz' in filename:
            with gzip.open(r) as zip_file:
                return decode(zip_file.read())
        else:
            return decode(ZipFile(BytesIO(r.read())).read(filename.replace('.zip', '.TXT')))


def download_al_files():
    b3_ftp = FTP('ftp.bmf.com.br')
    b3_ftp.login()
    b3_ftp.cwd('MarketData/Bovespa-Vista/')
    for file in b3_ftp.nlst():
        yield download_b3_file(file)
