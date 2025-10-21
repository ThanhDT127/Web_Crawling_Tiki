# -*- coding: utf-8 -*-

import os
import pandas as pd
from .config import PARTIAL_XLSX


class ExcelStore:
    def __init__(self, partial_path=PARTIAL_XLSX):
        self.partial_path = str(partial_path)

    def append_partial(self, all_rows, is_rd: bool):
        if not all_rows:
            return
        sheet = "RD" if is_rd else "OTHER"
        try:
            new_df = pd.DataFrame(all_rows).drop_duplicates(subset=["review_id_hash"]) if all_rows else None
            if new_df is None or new_df.empty:
                return
            if os.path.exists(self.partial_path):
                try:
                    old_df = pd.read_excel(self.partial_path, sheet_name=sheet)
                except Exception:
                    old_df = pd.DataFrame()
                cur = pd.concat([old_df, new_df], ignore_index=True) if not old_df.empty else new_df.copy()
                cur = cur.drop_duplicates(subset=["review_id_hash"]) if not cur.empty else cur
                with pd.ExcelWriter(self.partial_path, engine="openpyxl", mode="a", if_sheet_exists="overlay") as w:
                    if sheet in w.book.sheetnames:
                        w.book.remove(w.book[sheet])
                    cur.to_excel(w, sheet_name=sheet, index=False)
            else:
                with pd.ExcelWriter(self.partial_path, engine="openpyxl") as w:
                    new_df.to_excel(w, sheet_name=sheet, index=False)
        except Exception as e:
            print("[Excel partial] Error:", e)
