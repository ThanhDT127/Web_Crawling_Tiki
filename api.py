# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import time
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import (
    TIKI_BASE_URL,
    REVIEWS_ENDPOINT,
    PRODUCT_ENDPOINT,
    DEFAULT_REVIEW_LIMIT,
    DEFAULT_REVIEW_SORT,
    STAR_FILTER_PARAM,
    RATE_LIMIT_RPS,
    RETRY_MAX,
    RETRY_BACKOFF_BASE,
    DEFAULT_HEADERS,
    HTTP_PROXY,
    HTTPS_PROXY,
    USER_AGENTS,
    PROXY_POOL,
)


class ApiError(Exception):
    pass


def _rate_limiter_factory(rps: float):
    if rps <= 0:
        return lambda: None
    interval = 1.0 / rps
    _last = {"t": 0.0}

    def _wait():
        now = time.time()
        dt = now - _last["t"]
        if dt < interval:
            time.sleep(interval - dt)
        _last["t"] = time.time()

    return _wait


_rl = _rate_limiter_factory(RATE_LIMIT_RPS)


def _extract_product_id(url: str) -> Optional[str]:
    if not url:
        return None
    # Phổ biến: https://tiki.vn/<slug>-p12345678.html
    m = re.search(r"-p(\d+)(?:\.html|$)", url)
    if m:
        return m.group(1)
    # Fallback: query param product_id=123
    m = re.search(r"[?&]product_id=(\d+)", url)
    if m:
        return m.group(1)
    return None


@dataclass
class Review:
    reviewer: str
    review_date: str
    rating: Optional[int]
    review_text: str
    image_urls: List[str]
    video_urls: List[str]


class _Rotator:
    def __init__(self, values: Iterable[Any]):
        self._values: List[Any] = [v for v in values if v]
        self._idx = 0

    def next(self) -> Optional[Any]:
        if not self._values:
            return None
        value = self._values[self._idx]
        self._idx = (self._idx + 1) % len(self._values)
        return value


def _normalize_proxy_entry(entry: Any) -> Optional[Dict[str, str]]:
    if not entry:
        return None
    if isinstance(entry, str):
        entry = entry.strip()
        if not entry:
            return None
        return {"http": entry, "https": entry}
    if isinstance(entry, dict):
        normalized: Dict[str, str] = {}
        for key in ("http", "https"):
            val = entry.get(key)
            if isinstance(val, str) and val.strip():
                normalized[key] = val.strip()
        return normalized or None
    return None


class TikiApi:
    def __init__(self, headers: Optional[Dict[str, str]] = None):
        self.base_headers = dict(headers or DEFAULT_HEADERS)

        ua_pool = USER_AGENTS if isinstance(USER_AGENTS, list) else []
        if self.base_headers.get("User-Agent") and self.base_headers["User-Agent"] not in ua_pool:
            ua_pool = [self.base_headers["User-Agent"], *(ua_pool or [])]
        self._ua_rotator = _Rotator(ua_pool or [self.base_headers.get("User-Agent")])

        proxy_entries: List[Dict[str, str]] = []
        for raw in (PROXY_POOL or []):
            norm = _normalize_proxy_entry(raw)
            if norm:
                proxy_entries.append(norm)
        default_proxy = _normalize_proxy_entry({"http": HTTP_PROXY, "https": HTTPS_PROXY})
        if default_proxy and default_proxy not in proxy_entries:
            proxy_entries.append(default_proxy)
        self._proxy_rotator = _Rotator(proxy_entries)

        self._clients: Dict[Optional[Tuple[Tuple[str, str], ...]], httpx.Client] = {}

    def _client_for_proxy(self, proxy: Optional[Dict[str, str]]) -> httpx.Client:
        key = None
        if proxy:
            key = tuple(sorted(proxy.items()))
        if key not in self._clients:
            self._clients[key] = httpx.Client(
                base_url=TIKI_BASE_URL,
                headers=self.base_headers,
                timeout=30.0,
                proxies=proxy,
                follow_redirects=True,
            )
        return self._clients[key]

    def _choose_headers(self) -> Dict[str, str]:
        headers = deepcopy(self.base_headers)
        ua = self._ua_rotator.next()
        if isinstance(ua, str) and ua.strip():
            headers["User-Agent"] = ua.strip()
        return headers

    def close(self):
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                pass

    @retry(reraise=True,
           stop=stop_after_attempt(RETRY_MAX),
           wait=wait_exponential(multiplier=RETRY_BACKOFF_BASE, min=1, max=20),
           retry=retry_if_exception_type((httpx.HTTPError, ApiError)))
    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        _rl()
        proxy = self._proxy_rotator.next()
        client = self._client_for_proxy(proxy)
        headers = self._choose_headers()
        resp = client.get(url, params=params, headers=headers)
        if resp.status_code == 429:
            raise ApiError("Too Many Requests (429)")
        resp.raise_for_status()
        return resp

    def get_product_info(self, product_id: str) -> Dict[str, Any]:
        url = PRODUCT_ENDPOINT.format(product_id=product_id)
        r = self._get(url)
        return r.json()

    def get_reviews_page(
        self,
        product_id: str,
        page: int = 1,
        limit: int = DEFAULT_REVIEW_LIMIT,
        star: Optional[int] = None,
        sort: str = DEFAULT_REVIEW_SORT,
    ) -> Tuple[List[Review], Dict[str, Any]]:
        params: Dict[str, Any] = {
            "product_id": product_id,
            "page": page,
            "limit": limit,
            "sort": sort,
            # NOTE: API thực tế cần xác nhận tham số filter sao
        }
        if star in [1, 2, 3, 4, 5]:
            params[STAR_FILTER_PARAM] = star

        r = self._get(REVIEWS_ENDPOINT, params=params)
        data = r.json()

        # Map dữ liệu → List[Review]. Tuỳ cấu trúc Tiki API (cần xác nhận tên trường chính xác)
        items = []
        raw_items = data.get("data") or data.get("reviews") or []
        if isinstance(raw_items, dict):
            raw_items = list(raw_items.values())

        for e in raw_items:
            try:
                reviewer = (e.get("created_by") or {}).get("name") or e.get("created_by_name") or ""
                review_date = e.get("created_at") or e.get("time") or ""
                rating = e.get("rating") or e.get("stars") or e.get("score")
                if isinstance(rating, str) and rating.isdigit():
                    rating = int(rating)
                if not isinstance(rating, int):
                    rating = None
                content = e.get("content") or e.get("title") or e.get("comment") or ""

                image_urls: List[str] = []
                vids: List[str] = []
                imgs = e.get("images") or e.get("attachments") or []
                if isinstance(imgs, dict):
                    imgs = list(imgs.values())
                if isinstance(imgs, list):
                    for im in imgs:
                        if isinstance(im, str) and im:
                            image_urls.append(im)
                        elif isinstance(im, dict):
                            u = im.get("full_path") or im.get("url") or im.get("origin")
                            if isinstance(u, str) and u:
                                image_urls.append(u)
                vlist = e.get("videos") or []
                if isinstance(vlist, list):
                    for v in vlist:
                        if isinstance(v, str) and v:
                            vids.append(v)
                        elif isinstance(v, dict):
                            u = v.get("url") or v.get("source")
                            if isinstance(u, str) and u:
                                vids.append(u)

                items.append(Review(
                    reviewer=str(reviewer).strip(),
                    review_date=str(review_date),
                    rating=rating,
                    review_text=str(content).strip(),
                    image_urls=image_urls,
                    video_urls=vids,
                ))
            except Exception:
                continue

        # Extract pagination info if available (total pages, etc.)
        meta = {
            "current_page": data.get("current_page") or data.get("page") or page,
            "total_pages": data.get("last_page") or data.get("total_pages") or None,
            "total_items": data.get("total") or None,
        }
        return items, meta

    @staticmethod
    def parse_product_id(url: str) -> Optional[str]:
        return _extract_product_id(url)
