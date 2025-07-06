import argparse, json, hashlib
from pathlib import Path
from langdetect import detect, LangDetectException
from tqdm import tqdm


def _hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _iter_json_files(folder: Path):
    for fp in sorted(folder.glob("*.json")):
        yield json.loads(fp.read_text(encoding="utf-8"))


def preprocess(in_dir: Path, out_path: Path, lang: str | None):
    seen_urls, seen_texts = set(), set()
    dup_urls = dup_texts = lang_filtered = kept = 0

    with out_path.open("w", encoding="utf-8") as out:
        for obj in tqdm(list(_iter_json_files(in_dir)), desc="reading"):
            url = obj.get("url", "")
            if url in seen_urls:
                dup_urls += 1
                continue
            seen_urls.add(url)

            text_hash = _hash(obj.get("text", ""))
            if text_hash in seen_texts:
                dup_texts += 1
                continue
            seen_texts.add(text_hash)

            if lang:
                try:
                    if detect(obj.get("text", "")) != lang:
                        lang_filtered += 1
                        continue
                except (LangDetectException, UnicodeDecodeError):
                    lang_filtered += 1
                    continue

            out.write(json.dumps(obj, ensure_ascii=False) + "\n")
            kept += 1

    total = len(seen_urls) + dup_urls
    print(f"\n✅ Total read          : {total}")
    print(f"🚫 duplicate URLs      : {dup_urls}")
    print(f"🚫 duplicate texts     : {dup_texts}")
    print(f"🚫 language filtered   : {lang_filtered}")
    print(f"💾 kept / written      : {kept}  →  {out_path}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("input_dir", type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--lang", default=None)
    args = p.parse_args()
    preprocess(args.input_dir, args.out, args.lang)


if __name__ == "__main__":
    main()
