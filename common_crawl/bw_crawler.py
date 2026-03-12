import argparse
from base_crawler import BaseCommonCrawler

class BWCrawler(BaseCommonCrawler):
    def __init__(self, output_dir="data/bw_raw", headless=True, limit=None, overwrite=False, skip_threshold=50, stealth=False):
        super().__init__(
            output_dir=output_dir,
            headers_selector=".documentHeader, .docheader",
            base_url="https://www.landesrecht-bw.de/bsbw/search",
            headless=headless,
            limit=limit,
            overwrite=overwrite,
            skip_threshold=skip_threshold,
            stealth=stealth,
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BW Landesrecht Crawler")
    parser.add_argument("--limit", type=int, help="Limit number of decisions to crawl", default=None)
    parser.add_argument("--no-headless", action="store_true", help="Run with visible browser window")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--stealth", action="store_true", help="Use browser-like User-Agent instead of crawler UA")
    parser.add_argument("--skip-threshold", type=int, default=50, help="Stop after N consecutive existing files (0=disable)")
    parser.add_argument("--output", type=str, default="data/bw_raw", help="Output directory")

    args = parser.parse_args()

    crawler = BWCrawler(output_dir=args.output, headless=not args.no_headless, limit=args.limit, overwrite=args.overwrite, skip_threshold=args.skip_threshold, stealth=args.stealth)
    crawler.crawl()
