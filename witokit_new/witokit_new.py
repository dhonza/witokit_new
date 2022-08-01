"""Welcome to WiToKit.

This is the entry point of the application.
"""

import multiprocessing
from sys import stderr
import urllib.request
import re
import bz2
import requests

from tqdm import tqdm, tqdm_notebook
from bs4 import BeautifulSoup

import utils.files as futils
import utils.urls as uutils

__all__ = ('download', 'extract')


def _download_href(output_dirpath, wiki_dump_url, href):
    url = uutils.get_wiki_arxiv_url(wiki_dump_url, href)
    output_filepath = futils.get_download_output_filepath(output_dirpath,
                                                          href)
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024

    progress_bar = tqdm_notebook(total=total_size, unit="B", unit_scale=True)

    with open(output_filepath, "wb") as output:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            output.write(data)
    
    progress_bar.close()


def _parallel_download(wiki_arxiv_hrefs, wiki_dump_url, num_threads,
                       output_dirpath):

    with multiprocessing.Pool(num_threads) as pool:

        for href in wiki_arxiv_hrefs:
            _download_href(output_dirpath, wiki_dump_url, href)


def _collect_wiki_arxiv_hrefs(wiki_dump_url: str, lang: str, date: str) -> list:
    wiki_arxiv_hrefs = []
    try:
        response = urllib.request.urlopen(wiki_dump_url)
        html_doc = response.read()
        soup = BeautifulSoup(html_doc, 'html.parser')
        for link in soup.find_all('a'):
            pattern = uutils.get_wikipedia_multi_pattern(lang, date)
            href = link.get('href')
            if re.match(pattern, href):
                wiki_arxiv_hrefs.append(href)
        if not wiki_arxiv_hrefs:
            for link in soup.find_all('a'):
                pattern = uutils.get_wikipedia_single_pattern(lang, date)
                href = link.get('href')
                if re.match(pattern, href):
                    wiki_arxiv_hrefs.append(href)
        if not wiki_arxiv_hrefs:
            print('ERROR: No wikipedia arxiv found!', file=stderr)
    except urllib.error.HTTPError as error:
        raise error
    return wiki_arxiv_hrefs


def download(lang: str = "en", output_dirpath: str = "./data", num_threads: int = 1, date: str = "latest") -> None:
    """
    Download function. 
    args: 
    lang -- language of wiki (cs, en etc.)
    output_dirpath -- path of directory for downloaded wikipedia dump
    num_threads -- number of threads to use (default 1)
    date -- date of dump (default: latest)
    """

    wiki_dump_url = uutils.get_wikipedia_dump_url(lang, date)

    wiki_arxiv_hrefs = _collect_wiki_arxiv_hrefs(wiki_dump_url, lang, date)

    _parallel_download(wiki_arxiv_hrefs, wiki_dump_url,
                       num_threads, output_dirpath)


def _decompress_arxiv(arxiv):
    inc_decompressor = bz2.BZ2Decompressor()

    output_arxiv_filepath = arxiv.rsplit('.bz2')[0]
    with open(arxiv, 'rb') as arxiv_byte_stream:
        with open(output_arxiv_filepath, 'wb') as out_stream:
            for data in iter(lambda: arxiv_byte_stream.read(100 * 1024), b''):
                out_stream.write(inc_decompressor.decompress(data))


def extract(bz2_input_dirpath: str, num_threads: int = 1) -> None:
    """
    Function for extracting bz2 file
    """
    bz2_arxivs = futils.get_bz2_arxivs(bz2_input_dirpath)
    total_arxivs = len(bz2_arxivs)
    with multiprocessing.Pool(num_threads) as pool:
        for _ in tqdm(pool.imap_unordered(_decompress_arxiv, bz2_arxivs),
                      total=total_arxivs):
            continue

# Only for test purposes!
if __name__ == "__main__":
    lang = input("Enter desired language: ")
    date = input("Enter date: ")
    output_dirpath = input("Enter directory for output: ")
    num_threads = int(input("Enter number of threads: "))

    download(lang, output_dirpath, num_threads, date)
