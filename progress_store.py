# -*- coding: utf-8 -*-

import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, List

PROGRESS_DIR = Path(__file__).resolve().parent.joinpath("progress")
PROGRESS_DIR.mkdir(parents=True, exist_ok=True)


def _hash_url(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def progress_path(url: str) -> Path:
    return PROGRESS_DIR / f"{_hash_url(url)}.json"


class LinkProgress:
    def __init__(self, url: str, total_target: int = 250, per_star_target: int = 50):
        self.url = url
        self.path = progress_path(url)
        self._default_total = int(total_target)
        self._default_per_star = int(per_star_target)
        self.data = self._load_or_init()

    def _ensure_structures(self, d: Dict[str, Any]) -> bool:
        changed = False
        counts = d.get("counts")
        if not isinstance(counts, dict):
            counts = {}
            d["counts"] = counts
            changed = True
        for star in ["1", "2", "3", "4", "5"]:
            if star not in counts or not isinstance(counts.get(star), int):
                counts[star] = int(counts.get(star, 0) or 0)
                changed = True
        if "total" not in counts or not isinstance(counts.get("total"), int):
            counts["total"] = int(counts.get("total", 0) or 0)
            changed = True

        pages = d.get("pages_done")
        if not isinstance(pages, dict):
            pages = {}
            d["pages_done"] = pages
            changed = True
        for star in ["1", "2", "3", "4", "5"]:
            if star not in pages or not isinstance(pages.get(star), int):
                pages[star] = int(pages.get(star, 0) or 0)
                changed = True

        exhausted = d.get("exhausted")
        if not isinstance(exhausted, dict):
            exhausted = {}
            d["exhausted"] = exhausted
            changed = True
        for star in ["1", "2", "3", "4", "5"]:
            if star not in exhausted or not isinstance(exhausted.get(star), bool):
                exhausted[star] = bool(exhausted.get(star, False))
                changed = True

        seen = d.get("seen_hashes")
        if not isinstance(seen, dict):
            seen = {}
            changed = True
        for star in ["1", "2", "3", "4", "5"]:
            bucket = seen.get(star)
            if not isinstance(bucket, list):
                seen[star] = []
                changed = True
        d["seen_hashes"] = seen
        return changed

    def _load_or_init(self) -> Dict[str, Any]:
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    d = json.load(f)
                    t = d.get("targets") or {}
                    expected = {"1": self._default_per_star, "2": self._default_per_star, "3": self._default_per_star, "4": self._default_per_star, "5": self._default_per_star, "total": self._default_total}
                    changed = self._ensure_structures(d)
                    if (not t) or any(str(k) not in t for k in ["1","2","3","4","5","total"]) or (
                        int(t.get("total", 0)) != self._default_total
                    ) or any(int(t.get(str(k), 0)) != self._default_per_star for k in [1,2,3,4,5]):
                        d["targets"] = expected
                        self._save(d)
                    elif changed:
                        self._save(d)
                    return d
            except Exception:
                pass
        base = {
            "url": self.url,
            "completed": False,
            "targets": {"1": self._default_per_star, "2": self._default_per_star, "3": self._default_per_star, "4": self._default_per_star, "5": self._default_per_star, "total": self._default_total},
            "counts":  {"1": 0,  "2": 0,  "3": 0,  "4": 0,  "5": 0,  "total": 0 },
            "pages_done": {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0},
            "exhausted":  {"1": False, "2": False, "3": False, "4": False, "5": False},
            "seen_hashes": {"1": [], "2": [], "3": [], "4": [], "5": []},
            "last_update": time.time(),
        }
        self._save(base)
        return base

    def _save(self, d: Dict[str, Any]) -> None:
        d["last_update"] = time.time()
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)

    def get(self) -> Dict[str, Any]:
        return self.data

    def ensure_targets(self, total_target: int, per_star_target: int) -> None:
        t = self.data.get("targets") or {}
        expected = {"1": int(per_star_target), "2": int(per_star_target), "3": int(per_star_target), "4": int(per_star_target), "5": int(per_star_target), "total": int(total_target)}
        previous_total = int(t.get("total", 0) or 0) if t else 0
        if (not t) or any(str(k) not in t for k in ["1","2","3","4","5","total"]) or (
            int(t.get("total", 0)) != int(total_target)
        ) or any(int(t.get(str(k), 0)) != int(per_star_target) for k in [1,2,3,4,5]):
            self.data["targets"] = expected
            try:
                cnt_total = int(self.data.get("counts", {}).get("total", 0) or 0)
            except Exception:
                cnt_total = 0
            if self.data.get("completed") and cnt_total < int(total_target):
                self.data["completed"] = False
            if previous_total < int(total_target):
                self.data["exhausted"] = {"1": False, "2": False, "3": False, "4": False, "5": False}
                self.data["pages_done"] = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
            self._save(self.data)
        else:
            try:
                cnt_total = int(self.data.get("counts", {}).get("total", 0) or 0)
            except Exception:
                cnt_total = 0
            if self.data.get("completed") and cnt_total < int(total_target):
                self.data["completed"] = False
                self._save(self.data)

    def record_hashes_for_star(self, star: int, hashes: List[str]) -> int:
        if star not in [1, 2, 3, 4, 5] or not hashes:
            return 0
        k = str(star)
        seen = self.data.setdefault("seen_hashes", {})
        bucket = seen.get(k)
        if not isinstance(bucket, list):
            bucket = []
        known = set(bucket)
        new_hashes: List[str] = []
        for h in hashes:
            if not h:
                continue
            if h not in known:
                known.add(h)
                new_hashes.append(h)
        if not new_hashes:
            seen[k] = bucket
            return 0
        bucket.extend(new_hashes)
        seen[k] = bucket
        self.data["seen_hashes"] = seen
        self.data["counts"][k] = self.data["counts"].get(k, 0) + len(new_hashes)
        self.data["counts"]["total"] = self.data["counts"].get("total", 0) + len(new_hashes)
        self._save(self.data)
        return len(new_hashes)

    def inc_page_done(self, star: int):
        k = str(star)
        self.data["pages_done"][k] = self.data["pages_done"].get(k, 0) + 1
        self._save(self.data)

    def mark_exhausted(self, star: int):
        k = str(star)
        self.data["exhausted"][k] = True
        self._save(self.data)

    def mark_completed(self):
        self.data["completed"] = True
        self._save(self.data)

    def want_more_for_star(self, star: int) -> bool:
        k = str(star)
        return (not self.data["exhausted"].get(k, False)) and (
            self.data["counts"].get(k, 0) < self.data["targets"].get(k, 50)
        )

    def total_reached(self) -> bool:
        return self.data["counts"].get("total", 0) >= self.data["targets"].get("total", 250)

    @staticmethod
    def list_completed_urls() -> set:
        done = set()
        for p in PROGRESS_DIR.glob("*.json"):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    d = json.load(f)
                if d.get("completed"):
                    done.add(d.get("url"))
            except Exception:
                continue
        return done
