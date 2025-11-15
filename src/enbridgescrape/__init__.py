# from enbridgescrape.scrapeAll import run
from .Scraper import metaDump, runNN_Scrape, runEnbridgeScrape
# from .enbridgeScrape import runIterScrape

from .Runner import scrapeToday, scrapeHistoric

# from .Munge.OAMunge import formatOA
from .utils import paths

__all__ = ["metaDump", "runNN_Scrape", "runEnbridgeScrape",
           "scrapeToday", "scrapeHistoric",  "paths"]
