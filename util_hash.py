# -*- coding: utf-8 -*-

import hashlib


def md5(s: str) -> str:
    return hashlib.md5((s or "").encode("utf-8")).hexdigest()


def md5_prefix64(url: str, reviewer: str, review_date: str, review_text: str) -> str:
    prefix = (review_text or "")[:64]
    base = f"{url}|{reviewer}|{review_date}|{prefix}"
    return md5(base)
