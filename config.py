# -*- coding: utf-8 -*-

import datetime as dt
from pathlib import Path

# ====== INPUT ======
# default to the package-local data directory so running from project root works
DATA_DIR = Path(__file__).resolve().parent.joinpath("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
JSON_INPUT = DATA_DIR.joinpath("products.json")

# ====== MYSQL ======
MYSQL_HOST = "127.0.0.1"
MYSQL_USER = "root"
MYSQL_PASS = "12345678"
# ĐỔI TÊN DB để test mới hoàn toàn (thêm hậu tố _1)
DB_RD     = "rd_tiki_data_comment_1"              # Rạng Đông
DB_OTHER  = "other_brand_tiki_data_comment_1"     # Hãng khác

# ====== CRAWL LIMITS ======
# Cho phép cấu hình tổng số review mục tiêu theo từng nhóm
RD_TOTAL_REVIEWS    = 100
OTHER_TOTAL_REVIEWS = 500

def _split_per_star(total: int) -> int:
    return max(1, total // 5)

RD_PER_STAR    = _split_per_star(RD_TOTAL_REVIEWS)
OTHER_PER_STAR = _split_per_star(OTHER_TOTAL_REVIEWS)

# ====== EXCEL FILES ======
PARTIAL_XLSX = Path("tiki_reviews_partial.xlsx")
FINAL_XLSX   = Path(f"tiki_reviews_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")

# ====== API CONFIG ======
# Có thể cần update theo thực tế. Để linh hoạt, cho phép override bằng .env sau (nếu muốn).
TIKI_BASE_URL = "https://tiki.vn"
# Endpoint review phổ biến (cần xác nhận):
#   /api/v2/reviews?product_id=<id>&page=<page>&limit=<n>&sort=created_at,desc&include=comments,attribute_vote_summary,thank
REVIEWS_ENDPOINT = "/api/v2/reviews"
# Endpoint product chi tiết (để lấy product_name/brand nếu cần):
#   /api/v2/products/<id>
PRODUCT_ENDPOINT = "/api/v2/products/{product_id}"

# Tham số mặc định khi gọi review
DEFAULT_REVIEW_LIMIT = 20
DEFAULT_REVIEW_SORT = "created_at,desc"  # hoặc "score,desc" – cần xác nhận

# Map filter sao → tên tham số (cần xác nhận). Một số triển khai dùng ratings=5 hoặc stars=5.
STAR_FILTER_PARAM = "stars"  # "stars" | "rating" | "ratings"

# Rate limit & retry
RATE_LIMIT_RPS = 2.0        # request per second tối đa
RETRY_MAX = 5               # số lần retry khi 429/5xx
RETRY_BACKOFF_BASE = 1.5    # hệ số backoff exponential

# Proxy (nếu cần) – dạng "http://user:pass@host:port" hoặc None
HTTP_PROXY = None
HTTPS_PROXY = None

# Optional static headers. Có thể cần Cookie hoặc header đặc thù để nhận đủ dữ liệu.
DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": TIKI_BASE_URL,
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Danh sách User-Agent để xoay vòng mỗi request, giúp tránh bị chặn đơn giản.
USER_AGENTS = [
    DEFAULT_HEADERS["User-Agent"],
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

# Pool proxy để xoay IP. Mỗi phần tử có thể là chuỗi (dùng chung cho HTTP/HTTPS) hoặc dict.
# Ví dụ: PROXY_POOL = ["http://user:pass@host1:port", {"http": "http://...", "https": "http://..."}]
PROXY_POOL = []
