# -*- coding: utf-8 -*-

import pymysql
from typing import List, Dict, Any
from .config import MYSQL_HOST, MYSQL_USER, MYSQL_PASS, DB_RD, DB_OTHER


CREATE_TABLE_SQL_RD = """
CREATE TABLE IF NOT EXISTS reviews (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  category VARCHAR(128) NULL,
  product_model VARCHAR(64) NULL,
  product_name VARCHAR(512) NULL,
  rating TINYINT NULL,
  reviewer VARCHAR(255) NULL,
  review_date VARCHAR(64) NULL,
  review_text MEDIUMTEXT NULL,
  image_urls MEDIUMTEXT NULL,
  video_urls MEDIUMTEXT NULL,
  product_link TEXT NULL,
  review_id_hash CHAR(32) NOT NULL,
  `from` VARCHAR(32) NOT NULL DEFAULT 'Tiki',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_review (review_id_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_TABLE_SQL_OTHER = """
CREATE TABLE IF NOT EXISTS reviews (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  category VARCHAR(128) NULL,
  brand VARCHAR(128) NULL,
  product_model VARCHAR(64) NULL,
  product_name VARCHAR(512) NULL,
  rating TINYINT NULL,
  reviewer VARCHAR(255) NULL,
  review_date VARCHAR(64) NULL,
  review_text MEDIUMTEXT NULL,
  image_urls MEDIUMTEXT NULL,
  video_urls MEDIUMTEXT NULL,
  product_link TEXT NULL,
  review_id_hash CHAR(32) NOT NULL,
  `from` VARCHAR(32) NOT NULL DEFAULT 'Tiki',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_review (review_id_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _raw_conn():
    return pymysql.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS,
        charset="utf8mb4", autocommit=True
    )


def _conn(dbname: str):
    return pymysql.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS,
        database=dbname, charset="utf8mb4", autocommit=False
    )


def init_databases():
    base = _raw_conn()
    cur = base.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_RD} DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_OTHER} DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    cur.close(); base.close()

    con = _conn(DB_RD); c = con.cursor()
    c.execute(CREATE_TABLE_SQL_RD); con.commit(); c.close(); con.close()

    con = _conn(DB_OTHER); c = con.cursor()
    c.execute(CREATE_TABLE_SQL_OTHER); con.commit(); c.close(); con.close()


def save_reviews(dbname: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    con = _conn(dbname); cur = con.cursor()

    if dbname == DB_OTHER:
        sql = """
        INSERT IGNORE INTO reviews (
            category, brand, product_model, product_name, rating, reviewer, review_date,
            review_text, image_urls, video_urls, product_link, review_id_hash, `from`
        ) VALUES (
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s
        )
        """
        payload = []
        for r in rows:
            payload.append((
                r.get("category"),
                r.get("brand", ""),
                r.get("product_model"),
                r.get("product_name"),
                r.get("rating"),
                r.get("reviewer"),
                r.get("review_date"),
                r.get("review_text"),
                ",".join(r.get("image_urls", [])) if isinstance(r.get("image_urls"), list) else "",
                ",".join(r.get("video_urls", [])) if isinstance(r.get("video_urls"), list) else "",
                r.get("product_link"),
                r.get("review_id_hash"),
                r.get("from", "Tiki"),
            ))
    else:
        sql = """
        INSERT IGNORE INTO reviews (
            category, product_model, product_name, rating, reviewer, review_date, review_text,
            image_urls, video_urls, product_link, review_id_hash, `from`
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s
        )
        """
        payload = []
        for r in rows:
            payload.append((
                r.get("category"),
                r.get("product_model"),
                r.get("product_name"),
                r.get("rating"),
                r.get("reviewer"),
                r.get("review_date"),
                r.get("review_text"),
                ",".join(r.get("image_urls", [])) if isinstance(r.get("image_urls"), list) else "",
                ",".join(r.get("video_urls", [])) if isinstance(r.get("video_urls", []), list) else "",
                r.get("product_link"),
                r.get("review_id_hash"),
                r.get("from", "Tiki"),
            ))

    inserted = 0
    try:
        cur.executemany(sql, payload)
        con.commit()
        inserted = cur.rowcount
    except Exception as e:
        print("[MySQL] Error:", e)
        con.rollback()
    finally:
        cur.close(); con.close()
    return inserted
