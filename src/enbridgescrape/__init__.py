# from enbridgescrape.scrapeAll import run
from .metaScrape import metaDump
# from .enbridgeScrape import runIterScrape
from .NoNotice import runNN_Scrape
from .enbridgeMain import runEnbridgeScrape
from .enbridgeToday import scrapeToday
from .enbridgeHistoric import scrapeHistoric


__all__ = ["metaDump", "runNN_Scrape",
           "runEnbridgeScrape", "scrapeToday", "scrapeHistoric"]
