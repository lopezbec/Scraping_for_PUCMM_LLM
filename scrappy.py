import scrapy, tempfile, os, io, random, hashlib, json, datetime, re
from urllib.parse import urlparse, urldefrag, urljoin
from pathlib import Path

from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException
from pdfminer.high_level import extract_text
import pdfplumber, pytesseract
from PIL import Image

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/126.0",
]


class FullSiteSpider(scrapy.Spider):
    name = "fullsite"

    def __init__(self, domain: str | None = None, start_url: str | None = None, **kw):
        if not (domain and start_url):
            raise ValueError("pass -a domain=<domain> -a start_url=<url>")
        self.allowed_domains = [domain.lower()]
        self.start_urls = [start_url]
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.out_dir = Path("data") / f"{self.allowed_domains[0]}_{ts}"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.page_count = 0
        self.logger.info("▶ Saving pages to %s", self.out_dir)
        super().__init__(**kw)

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
    }

    def parse(self, response: scrapy.http.Response):
        url = response.url
        ctype = response.headers.get("Content-Type", b"").decode()

        if "text/html" in ctype:
            soup = BeautifulSoup(response.text, "lxml")
            text = soup.get_text(" ", strip=True)
            self._save_item(
                response,
                {"type": "html", "raw_html": response.text, "text": text},
            )
            for a in soup.find_all("a", href=True):
                nxt, _ = urldefrag(urljoin(url, a["href"]))
                if urlparse(nxt).scheme not in {"http", "https"}:
                    continue
                yield scrapy.Request(
                    nxt,
                    headers={"User-Agent": random.choice(USER_AGENTS)},
                    callback=self.parse,
                    meta={
                        "playwright": not nxt.lower().endswith(
                            (".pdf", ".xls", ".xlsx", ".csv", ".txt")
                        )
                    },
                )

        elif url.lower().endswith(".pdf"):
            self._save_item(response, {"type": "pdf", "text": self._pdf_to_text(response.body)})

        elif url.lower().endswith((".txt", ".csv")) or "text/plain" in ctype:
            self._save_item(response, {"type": "txt", "text": response.text})

    def _save_item(self, response: scrapy.http.Response, payload: dict):
        meta = self._extract_metadata(response)
        meta.update(payload)
        fname = hashlib.md5(meta["url"].encode()).hexdigest() + ".json"
        with open(self.out_dir / fname, "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False, indent=2)
        self.page_count += 1
        self.logger.info("[%d] %s", self.page_count, meta["url"])

    def _extract_metadata(self, response: scrapy.http.Response) -> dict:
        headers = response.headers
        lang = None
        license_ = "unknown"
        meta_robots = None

        if b"text/html" in headers.get(b"Content-Type", b""):
            soup = BeautifulSoup(response.text, "lxml")
            lang = soup.html.attrs.get("lang") if soup.html else None
            tag = soup.find("meta", attrs={"name": re.compile("^robots$", re.I)})
            if tag and tag.get("content"):
                meta_robots = tag["content"]
            lic_tag = (
                soup.find("meta", attrs={"name": re.compile("license", re.I)})
                or soup.find("link", attrs={"rel": re.compile("license", re.I)})
            )
            if lic_tag:
                license_ = lic_tag.get("content") or lic_tag.get("href") or "unknown"
            if license_ == "unknown" and soup.find(string=re.compile("creativecommons", re.I)):
                license_ = "creative_commons"

        if not lang and headers.get(b"Content-Language"):
            lang = headers[b"Content-Language"].decode(errors="ignore").split(",")[0]

        if not lang:
            try:
                lang = detect(response.text[:1000])
            except (LangDetectException, UnicodeDecodeError):
                lang = "und"

        return {
            "url": response.url,
            "timestamp": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "status": response.status,
            "content_type": headers.get("Content-Type", b"").decode(),
            "content_length": int(headers.get("Content-Length", 0) or 0),
            "language": lang.lower() if lang else "und",
            "server_license": license_,
            "meta_robots": meta_robots,
            "robots_txt_allowed": response.meta.get("robots", {}).get("allowed", True),
        }

    @staticmethod
    def _pdf_to_text(binary: bytes) -> str:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as fh:
            fh.write(binary)
            path = fh.name
        try:
            txt = extract_text(path) or ""
            if txt.strip():
                return txt
            text_pages = []
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    img = page.to_image(resolution=300)
                    buf = io.BytesIO()
                    img.save(buf, format="PNG")
                    text_pages.append(pytesseract.image_to_string(Image.open(buf)))
            return "\n".join(text_pages)
        finally:
            os.remove(path)

    def closed(self, reason):
        self.logger.info("✅ Finished: %d pages saved (%s)", self.page_count, reason)
