import argparse, pathlib, json, sys, hashlib
from langdetect import detect, LangDetectException
from tqdm import tqdm

def iter_json_files(folder: pathlib.Path):
    for p in folder.rglob("*.json"):
        if p.is_file():
            yield p

def main() -> None:
    ap = argparse.ArgumentParser(description="deduplicate + language-filter crawl output")
    ap.add_argument("crawl_dir", type=pathlib.Path, help="folder with per-page *.json files")
    ap.add_argument("--out", required=True, help="output jsonlines filename")
    ap.add_argument("--lang", default="en", help="keep only this ISO-639-1 language (default: en)")
    args = ap.parse_args()

    if not args.crawl_dir.is_dir():
        sys.exit(f"❌ not a directory: {args.crawl_dir}")

    url_seen    = set()
    text_hashes = set()

    kept = 0
    dup_url = dup_txt = lang_drop = 0

    with open(args.out, "w", encoding="utf-8") as out_f:
        for fp in tqdm(list(iter_json_files(args.crawl_dir)), desc="🔄 reading"):
            data = json.loads(fp.read_text(encoding="utf-8", errors="ignore"))

            url = data.get("url")
            if url in url_seen:
                dup_url += 1
                continue
            url_seen.add(url)

            text = data.get("text", "")
            h = hashlib.sha1(text.encode()).hexdigest()
            if h in text_hashes:
                dup_txt += 1
                continue
            text_hashes.add(h)

            lang = data.get("language")
            if not lang:
                try:
                    lang = detect(text[:1000])
                except (LangDetectException, UnicodeDecodeError):
                    lang = "und"
                data["language"] = lang

            if lang.lower() != args.lang.lower():
                lang_drop += 1
                continue

            out_f.write(json.dumps(data, ensure_ascii=False) + "\n")
            kept += 1

    print(f"\n✅ Total read          : {kept + dup_url + dup_txt + lang_drop}")
    print(f"🚫 duplicate URLs      : {dup_url}")
    print(f"🚫 duplicate texts     : {dup_txt}")
    print(f"🚫 language filtered   : {lang_drop}")
    print(f"💾 kept / written      : {kept}  →  {args.out}")

if __name__ == "__main__":
    main()







