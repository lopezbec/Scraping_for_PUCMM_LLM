import json
import datetime as dt
from collections import Counter
from scrapy import signals


class RealTimeStats:
    def __init__(self) -> None:
        self.pages = 0
        self.raw_bytes = 0
        self.chars = 0
        self.words = 0
        self.languages: Counter[str] = Counter()
        self.started_at = dt.datetime.utcnow()


    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext


    def item_scraped(self, item, spider):
        text = item.get("text", "")
        self.pages += 1
        self.raw_bytes += len(text.encode())
        self.chars += len(text)
        self.words += len(text.split())

        lang = item.get("language")
        if lang:
            self.languages[lang] += 1

    def spider_closed(self, spider, reason):
        elapsed = (dt.datetime.utcnow() - self.started_at).total_seconds()
        summary = {
            "pages_total": self.pages,
            "crawl_secs": elapsed,
            "bytes_total": self.raw_bytes,
            "chars_total": self.chars,
            "words_total": self.words,
            "avg_words_pg": round(self.words / self.pages, 2) if self.pages else 0,
            "langs": list(self.languages.keys()),
            "reason": reason,
        }
        out_path = spider.out_dir / "crawl_summary.json"
        out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
        spider.logger.info("📊 summary saved → %s", out_path)
