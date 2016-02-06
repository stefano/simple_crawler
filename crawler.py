import csv
import os
import queue
import sys

from urllib.parse import urlparse, urljoin, urldefrag
from urllib.robotparser import RobotFileParser
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup


class CrawlerURL:
    """
    A URL for the crawler. It contains the actual URL and other useful
    meta-data (at the moment, only the depth from the root URL).
    """

    def __init__(self, url, depth):
        # remove the fragment to avoid duplicates
        self.url = urldefrag(url)[0]
        self.depth = depth

    def make_child_url(self, url):
        return CrawlerURL(urljoin(self.url, url), self.depth + 1)


class PrintLogger:
    """
    A simple logger for the crawler that outputs the status to the terminal.
    """

    def log_visiting(self, crawler_url):
        print('Visiting {}'.format(crawler_url.url))

    def log_processing(self, crawler_url):
        print('Processing {}'.format(crawler_url.url))

    def log_error(self, error):
        sys.stderr.write('{}\n'.format(error))


class SeenURLs:
    """
    Keeps track of URLs seen by the crawler. It keeps everything in
    memory, which works fine for small sites. A different approach
    would be required to be able to crawl bigger sites (e.g. using
    persistent storage).
    """

    def __init__(self):
        self._urls = set()

    def mark_seen(self, crawler_url):
        self._urls.add(crawler_url.url)

    def seen(self, crawler_url):
        return crawler_url.url in self._urls


class URLStore:
    """
    Stores URLS and their contents to disk.

    Mapping url paths to directory paths can lead to some problems,
    e.g. http://www.example.com/home/a.html would be saved under
    "www.example.com/home/a.html", but then we wouldn't be able to
    save "http://www.example.com/home/" as "www.example.com/home.

    To avoid name mangling issues, we keep an index mapping URLs to
    file names, and store the contents of the URLs in a flat
    directory.

    We use a CSV file for the index and keep all files in the same
    directory. For bigger sites, a different approach would be
    required. For example, we could split the files into multiple
    directories to avoid running into OS limitations on the number of
    files in a directory, and the index could be kept in a structure
    easier and faster to query (e.g. a SQL database or a key-value
    store).
    """

    def __init__(self, domain):
        self._base_dir = domain
        self._files_dir = os.path.join(self._base_dir, 'files')
        self._index_path = os.path.join(self._base_dir, 'index.csv')

        # make sure directories exist
        for dir_path in (self._base_dir, self._files_dir):
            try:
                os.makedirs(dir_path)
            except FileExistsError:
                # directory already exist
                pass

        # create index file
        self._index_file = open(self._index_path, 'w+')
        self._index_csv = csv.writer(self._index_file)

    def close(self):
        self._index_file.close()

    def save_file(self, crawler_url, page_content):
        path = self._url_to_path(crawler_url)

        self._add_to_index(crawler_url, path)

        with open(path, 'wb+') as url_file:
            url_file.write(page_content)

    def _url_to_path(self, crawler_url):
        return os.path.join(
            self._files_dir,
            # filename can't contain '/'
            crawler_url.url.replace('_', '__').replace('/', '_'),
        )

    def _add_to_index(self, crawler_url, file_path):
        self._index_csv.writerow([crawler_url.url, file_path])


class Crawler:
    """
    The crawler. It takes a custom logger and an initial URL to start
    crawling from.

    It only crawls inside a single sub-domain.

    It assumes it's starting from scratch. It cannot resume previously
    interrupted crawls.

    It fetches one URL at a time, blocking. A production crawler would
    fetch URLs using async requests, and have multiple workers to
    fetch and process URLs.
    """

    USER_AGENT = 'simple-crawler/1.0'
    # a server could generate an infinite loop of unique URLs. To
    # avoid looping, set a (generous) max depth from the root URL.
    MAX_DEPTH = 100

    def __init__(self, logger, root_url):
        self._logger = logger
        self._domain = urlparse(root_url).netloc

        if not self._domain:
            raise Exception('Crawler requires an absolute URL')

        self._store = URLStore(self._domain)
        self._seen_urls = SeenURLs()
        # the queue of URLs the crawler needs to visit.  We follow
        # links breadth-first by using a LIFO queue.
        self._urls_to_visit = queue.LifoQueue()

        # we need to follow robots.txt restrictions
        self._robots = self._init_robots(root_url)

        # add the root_url to the list of URLs to visit
        self._queue(CrawlerURL(root_url, depth=0))

    def _init_robots(self, root_url):
        url = urlparse(root_url)

        robots_url = url.scheme + '://' + url.netloc + '/robots.txt'
        parser = RobotFileParser(robots_url)
        parser.read()

        return parser

    def crawl(self):
        """
        Starts the actual crawl. It won't stop until it has no further
        links to follow.
        """

        while not self._urls_to_visit.empty():
            self._fetch_page(self._urls_to_visit.get())

    def close(self):
        self._store.close()

    def _fetch_page(self, crawler_url):
        """
        Fetches a page and processes it.
        """

        self._logger.log_visiting(crawler_url)

        req = Request(crawler_url.url, headers={'User-Agent': self.USER_AGENT})

        try:
            response = urlopen(req)
            content = response.read()
        except Exception as error:
            # log the error and ignore the page. In a production
            # setting, we'd check the error type and retry after a
            # while for temporary errors.
            self._logger.log_error(error)
            return

        self._logger.log_processing(crawler_url)

        # only html pages can contain further links to follow
        if 'text/html' in response.info()['Content-Type']:
            self._extract_links(crawler_url, content)

        self._store.save_file(crawler_url, content)

    def _extract_links(self, crawler_url, page_content):
        document = BeautifulSoup(page_content, 'html.parser')

        # html elements that can contain a link to follow
        link_elements = (
            # css files
            ('link', 'href'),
            # images
            ('img', 'src'),
            # links to other pages
            ('a', 'href'),
        )

        for element_name, link_attribute in link_elements:
            for element in document.find_all(element_name):
                self._queue(
                    crawler_url.make_child_url(element.get(link_attribute)),
                )

    def _can_queue(self, crawler_url):
        """
        Tells if the URL should be visited or not.
        """

        # limit max depth from root to avoid infinite sequences of
        # unique URLs
        if crawler_url.depth > self.MAX_DEPTH:
            return False

        try:
            parsed_url = urlparse(crawler_url.url)
        except (ValueError, TypeError):
            # not a valid URL, ignore it
            return False

        # respect the robots
        if not self._robots.can_fetch(self.USER_AGENT, crawler_url.url):
            return False

        # we're only interested in crawling a specific sub-domain
        if self._domain != parsed_url.netloc:
            return False

        # don't fetch the same page multiple times
        return not self._seen_urls.seen(crawler_url)

    def _queue(self, crawler_url):
        """
        Queues an URL to be crawled.
        """

        if not self._can_queue(crawler_url):
            return

        # mark it as seen before fetching it to avoid having
        # duplicates in the queue
        self._seen_urls.mark_seen(crawler_url)
        self._urls_to_visit.put(crawler_url)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python crawler.py <root url>')
        sys.exit(1)

    crawler = Crawler(PrintLogger(), sys.argv[1])
    crawler.crawl()
    crawler.close()
