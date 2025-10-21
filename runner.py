# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import re
from typing import Dict, Any, List, Optional

import pandas as pd

from .config import (
    JSON_INPUT, DB_RD, DB_OTHER, FINAL_XLSX,
    RD_TOTAL_REVIEWS, RD_PER_STAR, OTHER_TOTAL_REVIEWS, OTHER_PER_STAR,
)
from .db import init_databases, save_reviews
from .excel_store import ExcelStore
from .progress_store import LinkProgress, progress_path
from .api import TikiApi
from .util_hash import md5_prefix64


class Runner:
    def __init__(self, json_path=JSON_INPUT):
        self.json_path = str(json_path)
        self.excel_store = ExcelStore()
        self.api = TikiApi()

    def close(self):
        try:
            self.api.close()
        except Exception:
            pass

    def _normalize_model_label(self, lbl: str) -> str:
        if not lbl:
            return ""
        lbl = str(lbl).strip().replace('_', ' ').strip()
        parts = [p for p in re.split(r"\s+", lbl) if p]
        return parts[0] if parts else lbl

    def _crawl_one(self, url: str, is_rd: bool, product_model: Optional[str], category: Optional[str] = None) -> List[Dict[str, Any]]:
        product_id = TikiApi.parse_product_id(url)
        if not product_id:
            print(f"❌ Không trích xuất được product_id từ URL: {url}")
            return []

        # Lấy thông tin product (tên/brand) nếu cần
        product_info = {}
        try:
            product_info = self.api.get_product_info(product_id)
        except Exception as e:
            print("⚠️ Không lấy được product info:", e)

        product_name = (product_info.get("name") if isinstance(product_info, dict) else None) or None
        brand_val = None
        if isinstance(product_info, dict):
            bobj = product_info.get("brand")
            if isinstance(bobj, dict):
                brand_val = bobj.get("name")
            elif isinstance(bobj, str):
                brand_val = bobj

        # Quota theo nhóm
        if is_rd:
            total_cap = int(RD_TOTAL_REVIEWS)
            per_star_cap = int(RD_PER_STAR)
        else:
            total_cap = int(OTHER_TOTAL_REVIEWS)
            per_star_cap = int(OTHER_PER_STAR)

        target_per_star = {s: per_star_cap for s in range(1, 6)}
        remainder = max(0, total_cap - per_star_cap * 5)
        if remainder > 0:
            target_per_star[5] += remainder

        collected: List[Dict[str, Any]] = []
        taken_per_star = {s: 0 for s in range(1, 6)}
        seen_hashes = set()

        def make_row(r: Dict[str, Any], rating_val: int) -> Dict[str, Any]:
            rid = md5_prefix64(url, r.get("reviewer", ""), r.get("review_date", ""), r.get("review_text", ""))
            row = {
                "category": category,
                "brand": brand_val if not is_rd else None,
                "product_model": product_model,
                "product_name": product_name,
                "rating": rating_val,
                "reviewer": r.get("reviewer"),
                "review_date": r.get("review_date"),
                "review_text": r.get("review_text"),
                "image_urls": r.get("image_urls", []),
                "video_urls": r.get("video_urls", []),
                "product_link": url,
                "review_id_hash": rid,
                "from": "Tiki",
            }
            return row

        ck = LinkProgress(url, total_target=total_cap, per_star_target=per_star_cap)
        try:
            ck.ensure_targets(total_cap, per_star_cap)
        except Exception:
            pass

        # Đồng bộ số lượng từ checkpoint nếu có
        try:
            ck_counts = ck.get().get("counts", {})
            for s in range(1, 6):
                taken_per_star[s] = int(ck_counts.get(str(s), 0) or 0)
            existing_seen = ck.get().get("seen_hashes", {}) or {}
            for hashes in existing_seen.values():
                if isinstance(hashes, list):
                    seen_hashes.update(hashes)
        except Exception:
            pass

        def push_with_quota(batch: List[Dict[str, Any]], target_star: int) -> List[Dict[str, Any]]:
            kept = []
            for r in batch:
                if len(collected) >= total_cap:
                    break
                rating = r.get("rating")
                if isinstance(rating, str) and rating.isdigit():
                    rating = int(rating)
                if rating not in [1, 2, 3, 4, 5]:
                    rating = target_star
                row = make_row(r, rating)
                rid = row["review_id_hash"]
                if rid in seen_hashes:
                    continue
                if rating == target_star and taken_per_star[rating] < target_per_star[rating]:
                    seen_hashes.add(rid)
                    taken_per_star[rating] += 1
                    collected.append(row)
                    kept.append(row)
            return kept

        # Phase 1: theo thứ tự sao 1→5 (giống Lazada)
        for s in [1, 2, 3, 4, 5]:
            if ck.get()["exhausted"].get(str(s), False):
                continue
            while ck.want_more_for_star(s) and (not ck.total_reached()):
                try:
                    reviews, meta = self.api.get_reviews_page(product_id, page=ck.get().get("pages_done", {}).get(str(s), 0) + 1, star=s)
                except Exception as e:
                    print(f"⚠️ Lỗi gọi API reviews sao {s}: {e}")
                    break
                batch = [
                    {
                        "reviewer": rv.reviewer,
                        "review_date": rv.review_date,
                        "rating": rv.rating,
                        "review_text": rv.review_text,
                        "image_urls": rv.image_urls,
                        "video_urls": rv.video_urls,
                    }
                    for rv in reviews
                ]
                kept = push_with_quota(batch, s)

                if kept:
                    dbname = DB_RD if is_rd else DB_OTHER
                    try:
                        ins = save_reviews(dbname, kept) or 0
                        dup = max(0, len(kept) - ins)
                        print(f"✅ [MySQL:{'RD' if is_rd else 'OTHER'}] inserted={ins}, duplicate={dup}, total_collected={len(collected)}")
                        ck.record_hashes_for_star(s, [row["review_id_hash"] for row in kept])
                    except Exception as e:
                        print(f"❌ [MySQL ERROR] {e}")

                # Inc page
                try:
                    ck.inc_page_done(s)
                except Exception:
                    pass

                # Hết trang?
                total_pages = meta.get("total_pages")
                cur_page = meta.get("current_page")
                if total_pages is not None and cur_page is not None and int(cur_page) >= int(total_pages):
                    ck.mark_exhausted(s)
                    break

        # Phase 2: nếu chưa đủ tổng, duyệt ưu tiên sao 5→4→3→2→1, bỏ per-star chỉ giữ tổng
        if not ck.total_reached():
            for s in [5, 4, 3, 2, 1]:
                if ck.total_reached():
                    break
                # reset page để fill-up
                while not ck.total_reached():
                    try:
                        reviews, meta = self.api.get_reviews_page(product_id, page=ck.get().get("pages_done", {}).get(str(s), 0) + 1, star=s)
                    except Exception as e:
                        print(f"⚠️ Lỗi fill-up API reviews sao {s}: {e}")
                        break
                    batch = [
                        {
                            "reviewer": rv.reviewer,
                            "review_date": rv.review_date,
                            "rating": rv.rating,
                            "review_text": rv.review_text,
                            "image_urls": rv.image_urls,
                            "video_urls": rv.video_urls,
                        }
                        for rv in reviews
                    ]
                    kept_extra: List[Dict[str, Any]] = []
                    for r in batch:
                        if len(collected) >= total_cap:
                            break
                        rating = r.get("rating")
                        if isinstance(rating, str) and rating.isdigit():
                            rating = int(rating)
                        if rating not in [1, 2, 3, 4, 5]:
                            rating = s
                        row = make_row(r, rating)
                        rid = row["review_id_hash"]
                        if rid in seen_hashes:
                            continue
                        if rating == s:
                            seen_hashes.add(rid)
                            collected.append(row)
                            kept_extra.append(row)

                    if kept_extra:
                        dbname = DB_RD if is_rd else DB_OTHER
                        try:
                            ins = save_reviews(dbname, kept_extra) or 0
                            dup = max(0, len(kept_extra) - ins)
                            print(f"✅ [MySQL:{'RD' if is_rd else 'OTHER'}] inserted={ins}, duplicate={dup}, total_collected={len(collected)}")
                            ck.record_hashes_for_star(s, [row["review_id_hash"] for row in kept_extra])
                        except Exception as e:
                            print(f"❌ [MySQL ERROR] {e}")

                    try:
                        ck.inc_page_done(s)
                    except Exception:
                        pass

                    total_pages = meta.get("total_pages")
                    cur_page = meta.get("current_page")
                    if total_pages is not None and cur_page is not None and int(cur_page) >= int(total_pages):
                        break

        # Hoàn tất nếu đạt chỉ tiêu hoặc hết trang mọi sao
        exhausted = ck.get().get("exhausted", {})
        all_exhausted = all(bool(exhausted.get(str(s), False)) for s in [1, 2, 3, 4, 5])
        if ck.total_reached() or all_exhausted:
            try:
                ck.mark_completed()
            except Exception:
                pass
        return collected[:total_cap]

    def _process_group(self, category: str, group: Dict[str, Any], all_rd: List[dict], all_ot: List[dict]):
        rd_block = group.get("rangdong", {})
        if isinstance(rd_block, dict):
            for model, lst in rd_block.items():
                if not isinstance(lst, list):
                    continue
                for item in lst:
                    for pdp in item.get("tiki", []):
                        ck = LinkProgress(pdp)
                        ck.ensure_targets(int(RD_TOTAL_REVIEWS), int(RD_PER_STAR))
                        st = ck.get(); cnt = st.get('counts',{}).get('total',0); tgt = st.get('targets',{}).get('total',0)
                        if st.get('completed') and cnt >= tgt:
                            print(f"⏭️  Bỏ qua (đã hoàn tất): {pdp} | đã lấy: {cnt}/{tgt}")
                            continue
                        print(f"▶️  Xử lý: {pdp}\n   Checkpoint: {str(progress_path(pdp))}")
                        rows = self._crawl_one(pdp, is_rd=True, product_model=model, category=category)
                        if rows:
                            all_rd.extend(rows)
                            print(f"📦 Tổng review thu được (RD): {len(rows)}")

        for brand_key, value in group.items():
            if brand_key == "rangdong":
                continue
            if isinstance(value, dict):
                for model_label, lst in value.items():
                    model_candidate = self._normalize_model_label(model_label)
                    if not isinstance(lst, list):
                        continue
                    for item in lst:
                        pdp_links = [u for u in (item.get("tiki", []) or []) if u]
                        for pl in pdp_links:
                            ck = LinkProgress(pl)
                            ck.ensure_targets(int(OTHER_TOTAL_REVIEWS), int(OTHER_PER_STAR))
                            st = ck.get(); cnt = st.get('counts',{}).get('total',0); tgt = st.get('targets',{}).get('total',0)
                            if st.get('completed') and cnt >= tgt:
                                print(f"⏭️  Bỏ qua (đã hoàn tất): {pl} | đã lấy: {cnt}/{tgt}")
                                continue
                            print(f"▶️  Xử lý: {pl}\n   Checkpoint: {str(progress_path(pl))}")
                            rows = self._crawl_one(pl, is_rd=False, product_model=model_candidate or None, category=category)
                            if rows:
                                all_ot.extend(rows)
                                print(f"📦 Tổng review thu được (OTHER): {len(rows)}")
            elif isinstance(value, list):
                model_candidate = self._normalize_model_label(brand_key)
                for item in value:
                    pdp_links = [u for u in (item.get("tiki", []) or []) if u]
                    for pl in pdp_links:
                        ck = LinkProgress(pl)
                        ck.ensure_targets(int(OTHER_TOTAL_REVIEWS), int(OTHER_PER_STAR))
                        st = ck.get(); cnt = st.get('counts',{}).get('total',0); tgt = st.get('targets',{}).get('total',0)
                        if st.get('completed') and cnt >= tgt:
                            print(f"⏭️  Bỏ qua (đã hoàn tất): {pl} | đã lấy: {cnt}/{tgt}")
                            continue
                        print(f"▶️  Xử lý: {pl}\n   Checkpoint: {str(progress_path(pl))}")
                        rows = self._crawl_one(pl, is_rd=False, product_model=model_candidate or None, category=category)
                        if rows:
                            all_ot.extend(rows)
                            print(f"📦 Tổng review thu được (OTHER): {len(rows)}")

    def run(self):
        init_databases()
        with open(self.json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        normalized = {(k.replace(" ", "_")): v for k, v in data.items()}

        all_rd: List[dict] = []
        all_ot: List[dict] = []

        for raw_cat, grp in normalized.items():
            category = raw_cat.replace('_', ' ')
            if not isinstance(grp, dict):
                continue
            self._process_group(category, grp, all_rd, all_ot)

        try:
            with pd.ExcelWriter(str(FINAL_XLSX), engine="openpyxl") as w:
                if all_rd:
                    pd.DataFrame(all_rd).drop_duplicates(subset=["review_id_hash"]).to_excel(w, sheet_name="RD", index=False)
                if all_ot:
                    pd.DataFrame(all_ot).drop_duplicates(subset=["review_id_hash"]).to_excel(w, sheet_name="OTHER", index=False)
        except Exception as e:
            print("[Excel final] Error:", e)

        print("\n🎯 DONE")
        print(f"   RD unique rows:    {len(set([r['review_id_hash'] for r in all_rd])) if all_rd else 0}")
        print(f"   OTHER unique rows: {len(set([r['review_id_hash'] for r in all_ot])) if all_ot else 0}")
        print(f"   Final Excel: {FINAL_XLSX}")
