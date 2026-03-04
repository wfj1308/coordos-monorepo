#!/usr/bin/env python3
"""
iCRM -> CoordOS migration utility.

Typical usage:
  python scripts/migrate.py --phase company
  python scripts/migrate.py --phase employee
  python scripts/migrate.py --phase contract
  python scripts/migrate.py --phase finance
  python scripts/migrate.py --phase drawing
  python scripts/migrate.py --phase verify
  python scripts/migrate.py --phase raw_full
  python scripts/migrate.py --phase verify_raw_full
  python scripts/migrate.py --phase all
"""
import argparse
import base64
import csv
import hashlib
import json
import logging
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import mysql.connector
import psycopg2
import psycopg2.extras

# 閳光偓閳光偓 闁板秶鐤?閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "YOUR_MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE", "icrm"),
    "charset": "utf8mb4",
}

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "user": os.getenv("PG_USER", "coordos"),
    "password": os.getenv("PG_PASSWORD", "YOUR_PG_PASSWORD"),
    "database": os.getenv("PG_DATABASE", "coordos"),
}

TENANT_ID = int(os.getenv("TENANT_ID", "10000"))
BATCH_SIZE = int(os.getenv("MIGRATE_BATCH_SIZE", "500"))
RAW_SOURCE = os.getenv("RAW_SOURCE", "mysql").strip().lower()
RAW_PG_SOURCE_SCHEMA = os.getenv("RAW_PG_SOURCE_SCHEMA", "public").strip()
REGULATION_SOURCE_CSV = os.getenv("REGULATION_SOURCE_CSV", "").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
log = logging.getLogger(__name__)


# 閳光偓閳光偓 鏉╃偞甯?閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓
def get_mysql():
    return mysql.connector.connect(**MYSQL_CONFIG)

def get_pg():
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    return conn


# 閳光偓閳光偓 鏉╀胶些閺冦儱绻?閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓閳光偓
def log_migration(pg_cur, table: str, legacy_id: int, new_id: Optional[int],
                  status: str, error: str = None):
    pg_cur.execute("""
        INSERT INTO migration_log (table_name, legacy_id, new_id, status, error_msg)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (table_name, legacy_id) DO UPDATE
        SET status=EXCLUDED.status, new_id=EXCLUDED.new_id,
            error_msg=EXCLUDED.error_msg, migrated_at=NOW()
    """, (table, legacy_id, new_id, status, error))


# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
#  PHASE 1: 閸掑棗鍙曢崣姝岀讣缁変紮绱欓幍鈧張澶庛€冮惃鍕唨绾偓閿?# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
def migrate_companies():
    log.info("=== PHASE 1: 鏉╀胶些閸掑棗鍙曢崣?(company 閳?companies) ===")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("""
        SELECT id, name, companyType, company_id, code, licenseNum,
               charger, chargerPhone, financeCharger, bankCard,
               address, area_id, zone_id, note,
               deleted, addDate, create_time, update_time, tenant_id
        FROM company
        WHERE deleted = 0
        ORDER BY id
    """)
    rows = mysql_cur.fetchall()
    log.info(f"  鐠囪褰?{len(rows)} 閺夆€冲瀻閸忣剙寰冪拋鏉跨秿")

    success = 0
    for row in rows:
        try:
            pg_cur.execute("""
                INSERT INTO companies (
                    legacy_id, name, company_type, code, license_num,
                    charger, charger_phone, finance_charger, bank_account,
                    address, area_id, zone_id, note,
                    deleted, tenant_id, created_at, updated_at,
                    migrate_status
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,'LEGACY'
                )
                ON CONFLICT (legacy_id) DO NOTHING
                RETURNING id
            """, (
                row["id"], row["name"], row["companyType"] or 2,
                row["code"], row["licenseNum"],
                row["charger"], row["chargerPhone"], row["financeCharger"],
                row["bankCard"], row["address"], row["area_id"], row["zone_id"],
                row["note"], bool(row["deleted"]), row["tenant_id"] or TENANT_ID,
                row["create_time"] or row["addDate"],
                row["update_time"] or row["addDate"],
            ))
            result = pg_cur.fetchone()
            new_id = result[0] if result else None
            log_migration(pg_cur, "company", row["id"], new_id, "SUCCESS")
            success += 1
        except Exception as e:
            log.error(f"  company id={row['id']} 婢惰精瑙? {e}")
            log_migration(pg_cur, "company", row["id"], None, "FAILED", str(e))

    # fill parent_id from legacy parent relation
    mysql_cur.execute("SELECT id, company_id FROM company WHERE company_id IS NOT NULL AND deleted=0")
    for row in mysql_cur.fetchall():
        pg_cur.execute("""
            UPDATE companies c
            SET parent_id = (SELECT id FROM companies WHERE legacy_id = %s)
            WHERE legacy_id = %s
        """, (row["company_id"], row["id"]))

    pg_conn.commit()
    log.info(f"  閴?閸掑棗鍙曢崣姝岀讣缁夎鐣幋? {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
#  PHASE 2: 閸涙ê浼愭潻浣盒?# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
def migrate_employees():
    log.info("=== PHASE 2: 鏉╀胶些閸涙ê浼?(employee 閳?employees) ===")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("""
        SELECT id, name, phone, account, company_id, department_id,
               user_id, postion, startDate, endDate, addDate
        FROM employee
        ORDER BY id
    """)
    rows = mysql_cur.fetchall()
    log.info("  loaded %s employee rows", len(rows))

    success = 0
    for row in rows:
        try:
            # 閺屻儲澹樼€电懓绨查惃?PG company id
            pg_cur.execute("SELECT id FROM companies WHERE legacy_id = %s", (row["company_id"],))
            company_row = pg_cur.fetchone()
            pg_company_id = company_row[0] if company_row else None

            pg_cur.execute("""
                INSERT INTO employees (
                    legacy_id, name, phone, account, company_id,
                    department_id, user_id, position,
                    start_date, end_date, tenant_id, created_at, migrate_status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'LEGACY')
                ON CONFLICT (legacy_id) DO NOTHING
                RETURNING id
            """, (
                row["id"], row["name"], row["phone"], row["account"],
                pg_company_id, row["department_id"], row["user_id"],
                row["postion"], row["startDate"], row["endDate"],
                TENANT_ID, row["addDate"],
            ))
            result = pg_cur.fetchone()
            log_migration(pg_cur, "employee", row["id"],
                          result[0] if result else None, "SUCCESS")
            success += 1
        except Exception as e:
            log.error(f"  employee id={row['id']} 婢惰精瑙? {e}")
            log_migration(pg_cur, "employee", row["id"], None, "FAILED", str(e))

    pg_conn.commit()
    log.info(f"  閴?閸涙ê浼愭潻浣盒╃€瑰本鍨? {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
#  PHASE 3: 閸氬牆鎮撴潻浣盒╅敍鍫熸付婢跺秵娼呴敍灞芥儓婵梹澧柧楣冨櫢瀵ょ尨绱?# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
def migrate_contracts():
    log.info("=== PHASE 3: 鏉╀胶些閸氬牆鎮?(contract 閳?contracts) ===")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    # 濞夈劍鍓伴敍姝盿rent 鐎涙顔岄弰顖氼潤閹垫﹢鎽奸敍灞界箑妞よ鍘涢幓鎺戝弳閻栬泛鎮庨崥灞藉晙閹绘帒鐡欓崥鍫濇倱
    # topo-order contracts so parent rows land first
    mysql_cur.execute("""
        SELECT id, num, contractName, contractBalance, manageRatio,
               signing_subject, signing_time, contractDate, payType,
               type, state, storeState, company_id, customer_id,
               employee_id, parent, owner_id, catalog,
               totleBalance, totleGathering, totleInvoice,
               note, deleted, draft, tenant_id,
               addDate, lastDate
        FROM contract
        WHERE deleted = 0
        ORDER BY id
    """)
    rows = mysql_cur.fetchall()
    log.info("  loaded %s contract rows", len(rows))

    # 閹锋挻澧ら幒鎺戠碍閿涙碍鐥呴張?parent 閻ㄥ嫬鍘涢幓?    rows_dict = {r["id"]: r for r in rows}
    ordered = _topo_sort_contracts(rows)
    log.info("  topo sort done: %s contracts", len(ordered))

    success = 0
    for row in ordered:
        try:
            # 閺?company
            pg_cur.execute("SELECT id FROM companies WHERE legacy_id=%s",
                           (row["company_id"],))
            r = pg_cur.fetchone()
            pg_company_id = r[0] if r else None

            # 閺?employee
            pg_cur.execute("SELECT id FROM employees WHERE legacy_id=%s",
                           (row["employee_id"],))
            r = pg_cur.fetchone()
            pg_employee_id = r[0] if r else None

            # 閺屻儳鍩楅崥鍫濇倱閿涘牆鍑￠崷銊ュ闂堛垺褰冮崗銉礆
            pg_parent_id = None
            if row["parent"]:
                pg_cur.execute("SELECT id FROM contracts WHERE legacy_id=%s",
                               (row["parent"],))
                r = pg_cur.fetchone()
                pg_parent_id = r[0] if r else None

            pg_cur.execute("""
                INSERT INTO contracts (
                    legacy_id, num, contract_name, contract_balance,
                    manage_ratio, signing_subject, signing_time,
                    contract_date, pay_type, contract_type, state,
                    store_state, company_id, customer_id, employee_id,
                    parent_id, owner_id, catalog,
                    totle_balance, totle_gathering, totle_invoice,
                    note, deleted, draft, tenant_id,
                    created_at, updated_at, migrate_status
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'LEGACY'
                )
                ON CONFLICT (legacy_id) DO UPDATE SET
                    company_id = EXCLUDED.company_id,
                    employee_id = EXCLUDED.employee_id,
                    parent_id = EXCLUDED.parent_id,
                    updated_at = EXCLUDED.updated_at,
                    migrate_status = 'LEGACY'
                RETURNING id
            """, (
                row["id"], row["num"], row["contractName"],
                row["contractBalance"], row["manageRatio"],
                row["signing_subject"], row["signing_time"],
                row["contractDate"], row["payType"], row["type"],
                row["state"], row["storeState"] or 2,
                pg_company_id, row["customer_id"], pg_employee_id,
                pg_parent_id, row["owner_id"], row["catalog"] or 1,
                row["totleBalance"], row["totleGathering"], row["totleInvoice"],
                row["note"], bool(row["deleted"]), row["draft"] or 0,
                row["tenant_id"] or TENANT_ID,
                row["addDate"] or row["lastDate"] or datetime.now(),
                row["lastDate"] or row["addDate"] or datetime.now(),
            ))
            result = pg_cur.fetchone()
            log_migration(pg_cur, "contract", row["id"],
                          result[0] if result else None, "SUCCESS")
            success += 1
        except Exception as e:
            log.error(f"  contract id={row['id']} 婢惰精瑙? {e}")
            pg_conn.rollback()
            log_migration(pg_cur, "contract", row["id"], None, "FAILED", str(e))
            pg_conn.commit()
            continue

        if success % BATCH_SIZE == 0:
            pg_conn.commit()
            log.info(f"    瀹稿弶褰佹禍?{success} 閺?..")

    pg_conn.commit()
    log.info(f"  閴?閸氬牆鎮撴潻浣盒╃€瑰本鍨? {success}/{len(ordered)}")

    # delegation-chain depth stats
    pg_cur.execute("""
        WITH RECURSIVE chain AS (
            SELECT id, parent_id, 1 AS depth FROM contracts WHERE parent_id IS NULL
            UNION ALL
            SELECT c.id, c.parent_id, ch.depth+1
            FROM contracts c JOIN chain ch ON c.parent_id = ch.id
        )
        SELECT depth, COUNT(*) FROM chain GROUP BY depth ORDER BY depth
    """)
    log.info("  婵梹澧柧鐐箒鎼达箑鍨庣敮?")
    for r in pg_cur.fetchall():
        log.info("    depth %s: %s contracts", r[0], r[1])

    mysql_conn.close(); pg_conn.close()


def _topo_sort_contracts(rows):
    """閹锋挻澧ら幒鎺戠碍閿涙氨鍩楅崥鍫濇倱閸︺劌澧犻敍灞界摍閸氬牆鎮撻崷銊ユ倵"""
    id_map = {r["id"]: r for r in rows}
    visited = set()
    result = []

    def visit(row):
        if row["id"] in visited:
            return
        if row["parent"] and row["parent"] in id_map:
            visit(id_map[row["parent"]])
        visited.add(row["id"])
        result.append(row)

    for row in rows:
        visit(row)
    return result


# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
#  PHASE 4: 鐠愩垹濮熼弫鐗堝祦鏉╀胶些閿涘牊鏁瑰▎?缂佹挾鐣?閸欐垹銈ㄩ敍?# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
def migrate_finance():
    log.info("=== PHASE 4: 鏉╀胶些鐠愩垹濮熼弫鐗堝祦 ===")
    _migrate_gatherings()
    _migrate_balances()
    _migrate_invoices()


def _migrate_gatherings():
    log.info("  4a. 閺€鑸殿儥閸?(gathering 閳?gatherings)")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("""
        SELECT id, gatheringNumber, gatheringMoney, gatheringdate,
               gatheringState, gatheringType, gatheringperson,
               contract_id, company_id, employee_id, balance_id,
               bankInfo_id, state, beforeMoney, afterMoney,
               manageRatio, needManageFee, note, draft,
               addDate, lastDate
        FROM gathering
        ORDER BY id
    """)
    rows = mysql_cur.fetchall()
    success = 0
    for row in rows:
        try:
            pg_cur.execute("SELECT id FROM contracts WHERE legacy_id=%s", (row["contract_id"],))
            r = pg_cur.fetchone(); pg_contract_id = r[0] if r else None
            pg_cur.execute("SELECT id FROM companies WHERE legacy_id=%s", (row["company_id"],))
            r = pg_cur.fetchone(); pg_company_id = r[0] if r else None
            pg_cur.execute("SELECT id FROM employees WHERE legacy_id=%s", (row["employee_id"],))
            r = pg_cur.fetchone(); pg_employee_id = r[0] if r else None

            pg_cur.execute("""
                INSERT INTO gatherings (
                    legacy_id, gathering_number, gathering_money,
                    gathering_date, gathering_state, gathering_type,
                    gathering_person, contract_id, company_id,
                    employee_id, bank_info_id, state,
                    before_money, after_money, manage_ratio,
                    need_manage_fee, note, draft,
                    tenant_id, created_at, updated_at, migrate_status
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,'LEGACY'
                ) ON CONFLICT (legacy_id) DO UPDATE SET
                    contract_id = EXCLUDED.contract_id,
                    company_id = EXCLUDED.company_id,
                    employee_id = EXCLUDED.employee_id,
                    updated_at = EXCLUDED.updated_at,
                    migrate_status = 'LEGACY'
                RETURNING id
            """, (
                row["id"], row["gatheringNumber"], row["gatheringMoney"],
                row["gatheringdate"], row["gatheringState"], row["gatheringType"],
                row["gatheringperson"], pg_contract_id, pg_company_id,
                pg_employee_id, row["bankInfo_id"], row["state"],
                row["beforeMoney"], row["afterMoney"], row["manageRatio"],
                row["needManageFee"], row["note"], row["draft"] or 0,
                TENANT_ID,
                row["addDate"] or row["lastDate"] or datetime.now(),
                row["lastDate"] or row["addDate"] or datetime.now(),
            ))
            result = pg_cur.fetchone()
            log_migration(pg_cur, "gathering", row["id"],
                          result[0] if result else None, "SUCCESS")
            success += 1
        except Exception as e:
            log.error(f"  gathering id={row['id']}: {e}")
            pg_conn.rollback()
            log_migration(pg_cur, "gathering", row["id"], None, "FAILED", str(e))
            pg_conn.commit()
            continue
        if success % BATCH_SIZE == 0:
            pg_conn.commit()
    pg_conn.commit()
    log.info(f"    閴?閺€鑸殿儥閸? {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


def _migrate_balances():
    log.info("  4b. 缂佹挾鐣婚崡?(balance 閳?balances)")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("""
        SELECT id, balanceNumber, contractName, customerName,
               gatheringMoney, bankMoney, cashMoney,
               bankSettlement, cashSettlement,
               VATRate, VATSum, deductRate, deductSum,
               managementCostSum, costTicketSum, totalInvoice,
               balanceType, state, payDate,
               gathering_id, employee_id, bank_id, payEmployee_id,
               note, draft, addDate, lastDate
        FROM balance
        ORDER BY id
    """)
    rows = mysql_cur.fetchall()
    success = 0
    for row in rows:
        try:
            pg_cur.execute("SELECT id FROM gatherings WHERE legacy_id=%s", (row["gathering_id"],))
            r = pg_cur.fetchone(); pg_gathering_id = r[0] if r else None
            pg_cur.execute("SELECT id FROM employees WHERE legacy_id=%s", (row["employee_id"],))
            r = pg_cur.fetchone(); pg_employee_id = r[0] if r else None

            pg_cur.execute("""
                INSERT INTO balances (
                    legacy_id, balance_number, contract_name, customer_name,
                    gathering_money, bank_money, cash_money,
                    bank_settlement, cash_settlement,
                    vat_rate, vat_sum, deduct_rate, deduct_sum,
                    management_cost_sum, cost_ticket_sum, total_invoice,
                    balance_type, state, pay_date,
                    gathering_id, employee_id, bank_id, pay_employee_id,
                    note, draft, tenant_id,
                    created_at, updated_at, migrate_status
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'LEGACY'
                ) ON CONFLICT (legacy_id) DO UPDATE SET
                    gathering_id = EXCLUDED.gathering_id,
                    employee_id = EXCLUDED.employee_id,
                    updated_at = EXCLUDED.updated_at,
                    migrate_status = 'LEGACY'
                RETURNING id
            """, (
                row["id"], row["balanceNumber"], row["contractName"],
                row["customerName"], row["gatheringMoney"],
                row["bankMoney"], row["cashMoney"],
                row["bankSettlement"], row["cashSettlement"],
                row["VATRate"], row["VATSum"], row["deductRate"], row["deductSum"],
                row["managementCostSum"], row["costTicketSum"], row["totalInvoice"],
                row["balanceType"], row["state"], row["payDate"],
                pg_gathering_id, pg_employee_id, row["bank_id"],
                row["payEmployee_id"], row["note"], row["draft"] or 0,
                TENANT_ID,
                row["addDate"] or row["lastDate"] or datetime.now(),
                row["lastDate"] or row["addDate"] or datetime.now(),
            ))
            result = pg_cur.fetchone()
            log_migration(pg_cur, "balance", row["id"],
                          result[0] if result else None, "SUCCESS")
            success += 1
        except Exception as e:
            log.error(f"  balance id={row['id']}: {e}")
            pg_conn.rollback()
            log_migration(pg_cur, "balance", row["id"], None, "FAILED", str(e))
            pg_conn.commit()
            continue
        if success % BATCH_SIZE == 0:
            pg_conn.commit()
    pg_conn.commit()
    log.info(f"    閴?缂佹挾鐣婚崡? {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


def _migrate_invoices():
    log.info("  4c. 閸欐垹銈?(invoice 閳?invoices)")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("""
        SELECT id, invoiceCode, invoiceNumber, invoiceType, invoiceState,
               invoicedate, invoiceContent, invoiceperson,
               curAmount, money, usedMoney,
               contract_id, customer_id, employee_id,
               state, draft, note, addDate, lastDate
        FROM invoice
        ORDER BY id
    """)
    rows = mysql_cur.fetchall()
    success = 0
    for row in rows:
        try:
            pg_cur.execute("SELECT id FROM contracts WHERE legacy_id=%s", (row["contract_id"],))
            r = pg_cur.fetchone(); pg_contract_id = r[0] if r else None
            pg_cur.execute("SELECT id FROM employees WHERE legacy_id=%s", (row["employee_id"],))
            r = pg_cur.fetchone(); pg_employee_id = r[0] if r else None

            pg_cur.execute("""
                INSERT INTO invoices (
                    legacy_id, invoice_code, invoice_number,
                    invoice_type, invoice_state, invoice_date,
                    invoice_content, invoice_person,
                    cur_amount, money, used_money,
                    contract_id, customer_id, employee_id,
                    state, draft, note, tenant_id,
                    created_at, updated_at, migrate_status
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,'LEGACY'
                ) ON CONFLICT (legacy_id) DO UPDATE SET
                    contract_id = EXCLUDED.contract_id,
                    employee_id = EXCLUDED.employee_id,
                    updated_at = EXCLUDED.updated_at,
                    migrate_status = 'LEGACY'
                RETURNING id
            """, (
                row["id"], row["invoiceCode"], row["invoiceNumber"],
                row["invoiceType"], row["invoiceState"], row["invoicedate"],
                row["invoiceContent"], row["invoiceperson"],
                row["curAmount"], row["money"], row["usedMoney"],
                pg_contract_id, row["customer_id"], pg_employee_id,
                row["state"], row["draft"] or 0, row["note"],
                TENANT_ID,
                row["addDate"] or row["lastDate"] or datetime.now(),
                row["lastDate"] or row["addDate"] or datetime.now(),
            ))
            result = pg_cur.fetchone()
            log_migration(pg_cur, "invoice", row["id"],
                          result[0] if result else None, "SUCCESS")
            success += 1
        except Exception as e:
            log.error(f"  invoice id={row['id']}: {e}")
            pg_conn.rollback()
            log_migration(pg_cur, "invoice", row["id"], None, "FAILED", str(e))
            pg_conn.commit()
            continue
        if success % BATCH_SIZE == 0:
            pg_conn.commit()
    pg_conn.commit()
    log.info(f"    閴?閸欐垹銈? {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
#  PHASE 5: 閸ュ墽鐒婃潻浣盒?# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
def migrate_drawings():
    log.info("=== PHASE 5: 鏉╀胶些閸ュ墽鐒?(drawing 閳?drawings) ===")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("""
        SELECT id, num, major, state, handleStatus, resultStatus,
               contract_id, company_id, employee_id, reviewer,
               remarks, draft, addDate, lastDate
        FROM drawing
        ORDER BY id
    """)
    rows = mysql_cur.fetchall()
    success = 0
    for row in rows:
        try:
            pg_cur.execute("SELECT id FROM contracts WHERE legacy_id=%s", (row["contract_id"],))
            r = pg_cur.fetchone(); pg_contract_id = r[0] if r else None
            pg_cur.execute("SELECT id FROM companies WHERE legacy_id=%s", (row["company_id"],))
            r = pg_cur.fetchone(); pg_company_id = r[0] if r else None
            pg_cur.execute("SELECT id FROM employees WHERE legacy_id=%s", (row["employee_id"],))
            r = pg_cur.fetchone(); pg_employee_id = r[0] if r else None

            pg_cur.execute("""
                INSERT INTO drawings (
                    legacy_id, num, major, state,
                    handle_status, result_status,
                    contract_id, company_id, employee_id,
                    reviewer, remarks, draft, tenant_id,
                    created_at, updated_at, migrate_status
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'LEGACY'
                ) ON CONFLICT (legacy_id) DO UPDATE SET
                    contract_id = EXCLUDED.contract_id,
                    company_id = EXCLUDED.company_id,
                    employee_id = EXCLUDED.employee_id,
                    updated_at = EXCLUDED.updated_at,
                    migrate_status = 'LEGACY'
                RETURNING id
            """, (
                row["id"], row["num"], row["major"], row["state"],
                row["handleStatus"], row["resultStatus"],
                pg_contract_id, pg_company_id, pg_employee_id,
                row["reviewer"], row["remarks"], row["draft"] or 0,
                TENANT_ID,
                row["addDate"] or row["lastDate"] or datetime.now(),
                row["lastDate"] or row["addDate"] or datetime.now(),
            ))
            result = pg_cur.fetchone()
            log_migration(pg_cur, "drawing", row["id"],
                          result[0] if result else None, "SUCCESS")
            success += 1
        except Exception as e:
            log.error(f"  drawing id={row['id']}: {e}")
            pg_conn.rollback()
            log_migration(pg_cur, "drawing", row["id"], None, "FAILED", str(e))
            pg_conn.commit()
            continue
        if success % BATCH_SIZE == 0:
            pg_conn.commit()
    pg_conn.commit()
    log.info(f"  閴?閸ュ墽鐒? {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
#  PHASE 6: 閺嶏繝鐛?# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
def verify():
    log.info("=== PHASE 6: 閺佺増宓佺€瑰本鏆ｉ幀褎鐗庢?===")
    pg_conn = get_pg()
    pg_cur = pg_conn.cursor()

    checks = [
        ("companies",  "SELECT COUNT(*) FROM companies"),
        ("employees",  "SELECT COUNT(*) FROM employees"),
        ("qualifications", "SELECT COUNT(*) FROM qualifications"),
        ("regulation_documents", "SELECT COUNT(*) FROM regulation_documents"),
        ("regulation_versions", "SELECT COUNT(*) FROM regulation_versions"),
        ("contracts",  "SELECT COUNT(*) FROM contracts"),
        ("gatherings", "SELECT COUNT(*) FROM gatherings"),
        ("balances",   "SELECT COUNT(*) FROM balances"),
        ("invoices",   "SELECT COUNT(*) FROM invoices"),
        ("drawings",   "SELECT COUNT(*) FROM drawings"),
        ("approve_flows", "SELECT COUNT(*) FROM approve_flows"),
        ("approve_tasks", "SELECT COUNT(*) FROM approve_tasks"),
        ("approve_records", "SELECT COUNT(*) FROM approve_records"),
        ("approve_flow_approvals", "SELECT COUNT(*) FROM approve_flow_approvals"),
        ("costtickets", "SELECT COUNT(*) FROM costtickets"),
        ("costticket_items", "SELECT COUNT(*) FROM costticket_items"),
        ("payments", "SELECT COUNT(*) FROM payments"),
        ("payment_items", "SELECT COUNT(*) FROM payment_items"),
        ("payment_attachments", "SELECT COUNT(*) FROM payment_attachments"),
        ("balance_invoices", "SELECT COUNT(*) FROM balance_invoices"),
        ("gathering_items", "SELECT COUNT(*) FROM gathering_items"),
        ("customers", "SELECT COUNT(*) FROM customers"),
        ("balance_records", "SELECT COUNT(*) FROM balance_records"),
        ("gathering_records", "SELECT COUNT(*) FROM gathering_records"),
        ("invoice_records", "SELECT COUNT(*) FROM invoice_records"),
        ("contract_creations", "SELECT COUNT(*) FROM contract_creations"),
        ("contract_creation_attachments", "SELECT COUNT(*) FROM contract_creation_attachments"),
        ("contract_extras", "SELECT COUNT(*) FROM contract_extras"),
        ("contract_extra_attachments", "SELECT COUNT(*) FROM contract_extra_attachments"),
        ("bid_assures", "SELECT COUNT(*) FROM bid_assures"),
        ("bid_assure_flows", "SELECT COUNT(*) FROM bid_assure_flows"),
        ("contract_details", "SELECT COUNT(*) FROM contract_details"),
        ("contract_attributes", "SELECT COUNT(*) FROM contract_attributes"),
        ("contract_attachments", "SELECT COUNT(*) FROM contract_attachments"),
        ("invoice_items", "SELECT COUNT(*) FROM invoice_items"),
        ("drawing_attachments", "SELECT COUNT(*) FROM drawing_attachments"),
        ("bankflow_entries", "SELECT COUNT(*) FROM bankflow_entries"),
        ("contract_archives", "SELECT COUNT(*) FROM contract_archives"),
        ("gathering_attachments", "SELECT COUNT(*) FROM gathering_attachments"),
        ("filebonds", "SELECT COUNT(*) FROM filebonds"),
        ("project_file_uploads", "SELECT COUNT(*) FROM project_file_uploads"),
        ("project_files", "SELECT COUNT(*) FROM project_files"),
        ("contract_cancels", "SELECT COUNT(*) FROM contract_cancels"),
        ("project_partners", "SELECT COUNT(*) FROM project_partners"),
        ("company_contracts", "SELECT COUNT(*) FROM company_contracts"),
        ("balance_print_records", "SELECT COUNT(*) FROM balance_print_records"),
        ("balance_fast_settlements", "SELECT COUNT(*) FROM balance_fast_settlements"),
        ("balance_fast_settlement_quotas", "SELECT COUNT(*) FROM balance_fast_settlement_quotas"),
        ("balance_fast_settlement_quota_files", "SELECT COUNT(*) FROM balance_fast_settlement_quota_files"),
        ("settlement_manage_fees", "SELECT COUNT(*) FROM settlement_manage_fees"),
        ("settlement_manage_fee_gatherings", "SELECT COUNT(*) FROM settlement_manage_fee_gatherings"),
        ("invoice_scraps", "SELECT COUNT(*) FROM invoice_scraps"),
        ("audit_gatherings", "SELECT COUNT(*) FROM audit_gatherings"),
        ("audit_gathering_files", "SELECT COUNT(*) FROM audit_gathering_files"),
        ("audit_invoices", "SELECT COUNT(*) FROM audit_invoices"),
        ("audit_receipt_invoiced_links", "SELECT COUNT(*) FROM audit_receipt_invoiced_links"),
        ("bank_infos", "SELECT COUNT(*) FROM bank_infos"),
        ("company_manage_moneys", "SELECT COUNT(*) FROM company_manage_moneys"),
        ("company_year_manage_moneys", "SELECT COUNT(*) FROM company_year_manage_moneys"),
        ("company_auth_areas", "SELECT COUNT(*) FROM company_auth_areas"),
        ("contract_nos", "SELECT COUNT(*) FROM contract_nos"),
        ("project_reports", "SELECT COUNT(*) FROM project_reports"),
        ("project_report_files", "SELECT COUNT(*) FROM project_report_files"),
        ("drawing_file_types", "SELECT COUNT(*) FROM drawing_file_types"),
        ("approve_flow_definitions_legacy", "SELECT COUNT(*) FROM approve_flow_definitions_legacy"),
        ("approve_flow_definition_items_legacy", "SELECT COUNT(*) FROM approve_flow_definition_items_legacy"),
        ("approve_flow_by_areas_legacy", "SELECT COUNT(*) FROM approve_flow_by_areas_legacy"),
        ("approve_flow_by_zones_legacy", "SELECT COUNT(*) FROM approve_flow_by_zones_legacy"),
        ("payrolls_legacy", "SELECT COUNT(*) FROM payrolls_legacy"),
        ("balance_records_legacy", "SELECT COUNT(*) FROM balance_records_legacy"),
        ("workers_legacy", "SELECT COUNT(*) FROM workers_legacy"),
        ("log_safety_records", "SELECT COUNT(*) FROM log_safety_records"),
        ("legacy_source_rows", "SELECT COUNT(*) FROM legacy_source_rows"),
    ]
    for name, sql in checks:
        try:
            pg_cur.execute(sql)
            count = pg_cur.fetchone()[0]
            log.info("  %s: %s rows", name, count)
        except Exception as e:
            pg_conn.rollback()
            log.warning("  %s: skipped (%s)", name, e)

    # 婢惰精瑙︾拋鏉跨秿
    pg_cur.execute("""
        SELECT table_name, COUNT(*) as failed
        FROM migration_log WHERE status='FAILED'
        GROUP BY table_name
    """)
    failed = pg_cur.fetchall()
    if failed:
        log.warning("  migration failures:")
        for r in failed:
            log.warning("    %s: %s failed rows", r[0], r[1])
    else:
        log.info("  no failed rows in migration_log")

    # contract chain integrity
    pg_cur.execute("""
        SELECT COUNT(*) FROM contracts
        WHERE parent_id IS NOT NULL
          AND parent_id NOT IN (SELECT id FROM contracts)
    """)
    orphans = pg_cur.fetchone()[0]
    if orphans:
        log.warning("  orphan contracts (parent missing): %s", orphans)
    else:
        log.info("  contract parent chain is complete")

    # amount spot-check
    pg_cur.execute("""
        SELECT SUM(contract_balance) FROM contracts
        WHERE migrate_status='LEGACY' AND deleted=FALSE
    """)
    total = pg_cur.fetchone()[0]
    log.info(f"  contract total amount: {total:,.2f}" if total else "  contract total amount: N/A")

    pg_conn.close()


# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
#  娑撹鍙嗛崣?# 閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜閳烘劏鏅查埡鎰ㄦ櫜
def migrate_qualifications():
    log.info("=== PHASE 5: migrate qualifications (worker/profession -> qualifications) ===")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    mysql_cur.execute(
        """
        SELECT
            w.id,
            w.name,
            w.no,
            w.register,
            w.regprofession_id,
            w.profession,
            w.protitle,
            w.title,
            w.company_id,
            w.user_id,
            w.certificateFile,
            w.addDate,
            w.lastDate,
            p.name AS regprofession_name,
            p.code AS regprofession_code,
            pp.name AS regprofession_parent_name,
            pp.code AS regprofession_parent_code
        FROM worker w
        LEFT JOIN profession p ON p.id = w.regprofession_id
        LEFT JOIN profession pp ON pp.id = p.pid
        WHERE w.regprofession_id IS NOT NULL
          AND w.regprofession_id <> 99
        ORDER BY w.id
        """
    )
    rows = mysql_cur.fetchall()
    log.info("  loaded %s worker qualification rows", len(rows))

    inserted = 0
    skipped = 0

    for row in rows:
        qual_type = map_worker_qual_type(row)
        if not qual_type:
            skipped += 1
            continue

        pg_cur.execute(
            """
            SELECT id, COALESCE(name,''), COALESCE(executor_ref,'')
            FROM employees
            WHERE tenant_id=%s
              AND deleted=FALSE
              AND user_id=%s
            ORDER BY id
            LIMIT 1
            """,
            (TENANT_ID, row["user_id"]),
        )
        employee = pg_cur.fetchone()
        if not employee:
            skipped += 1
            continue

        employee_id, employee_name, executor_ref = employee
        if not executor_ref:
            executor_ref = f"v://cn.zhongbei/executor/person/legacy-{employee_id}@v1"

        cert_no = normalize_worker_cert_no(row.get("register"), row.get("id"), qual_type)
        specialty = first_non_empty(
            row.get("profession"),
            row.get("protitle"),
            row.get("regprofession_name"),
        )
        level = extract_worker_level(row.get("regprofession_name"), row.get("title"))
        scope = first_non_empty(
            row.get("regprofession_parent_name"),
            row.get("regprofession_name"),
        )
        attachment_url = normalize_worker_attachment(row.get("certificateFile"))
        issued_at = row.get("addDate")
        valid_from = row.get("addDate")
        created_at = row.get("addDate") or row.get("lastDate") or datetime.now()
        updated_at = row.get("lastDate") or row.get("addDate") or datetime.now()
        note = f"legacy worker id={row.get('id')}, regprofession_id={row.get('regprofession_id')}"
        holder_name = first_non_empty(employee_name, row.get("name"), f"legacy-worker-{row.get('id')}")

        try:
            pg_cur.execute(
                """
                INSERT INTO qualifications (
                    holder_type, holder_id, holder_name, executor_ref,
                    qual_type, cert_no, issued_by, issued_at, valid_from, valid_until,
                    status, specialty, level, scope, attachment_url, note,
                    deleted, tenant_id, created_at, updated_at, max_concurrent_projects
                ) VALUES (
                    'PERSON', %s, %s, %s,
                    %s, %s, %s, %s, %s, NULL,
                    'VALID', %s, %s, %s, %s, %s,
                    FALSE, %s, %s, %s, %s
                )
                ON CONFLICT (cert_no) DO UPDATE SET
                    holder_type=EXCLUDED.holder_type,
                    holder_id=EXCLUDED.holder_id,
                    holder_name=EXCLUDED.holder_name,
                    executor_ref=EXCLUDED.executor_ref,
                    qual_type=EXCLUDED.qual_type,
                    issued_by=EXCLUDED.issued_by,
                    issued_at=EXCLUDED.issued_at,
                    valid_from=EXCLUDED.valid_from,
                    valid_until=EXCLUDED.valid_until,
                    status=EXCLUDED.status,
                    specialty=EXCLUDED.specialty,
                    level=EXCLUDED.level,
                    scope=EXCLUDED.scope,
                    attachment_url=EXCLUDED.attachment_url,
                    note=EXCLUDED.note,
                    deleted=FALSE,
                    updated_at=EXCLUDED.updated_at,
                    max_concurrent_projects=EXCLUDED.max_concurrent_projects
                """,
                (
                    employee_id,
                    holder_name,
                    executor_ref,
                    qual_type,
                    cert_no,
                    "iCRM worker registry",
                    issued_at,
                    valid_from,
                    specialty,
                    level,
                    scope,
                    attachment_url,
                    note,
                    TENANT_ID,
                    created_at,
                    updated_at,
                    default_max_concurrent(qual_type),
                ),
            )
            log_migration(pg_cur, "qualification", row["id"], employee_id, "SUCCESS")
            inserted += 1
        except Exception as e:
            log.error("  qualification worker id=%s failed: %s", row.get("id"), e)
            pg_conn.rollback()
            log_migration(pg_cur, "qualification", row["id"], None, "FAILED", str(e))
            pg_conn.commit()
            continue

        if inserted % BATCH_SIZE == 0:
            pg_conn.commit()

    pg_conn.commit()
    log.info("  qualification migration complete: inserted_or_updated=%s, skipped=%s", inserted, skipped)
    mysql_conn.close()
    pg_conn.close()


def migrate_regulations():
    log.info("=== PHASE 5: migrate regulations (csv -> regulation_documents/versions) ===")
    source_csv = REGULATION_SOURCE_CSV or os.getenv("REGULATION_CSV", "").strip()
    if not source_csv:
        log.info("  skipped: REGULATION_SOURCE_CSV is not set")
        return
    if not os.path.isfile(source_csv):
        log.warning("  skipped: regulation source csv not found: %s", source_csv)
        return

    allowed_status = {"DRAFT", "EFFECTIVE", "SUPERSEDED", "REPEALED", "ARCHIVED"}
    pg_conn = get_pg()
    pg_cur = pg_conn.cursor()

    inserted_docs = 0
    updated_docs = 0
    inserted_versions = 0
    updated_versions = 0
    skipped = 0
    processed = 0

    with open(source_csv, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            log.warning("  skipped: csv has no header: %s", source_csv)
            pg_conn.close()
            return

        for line_no, row in enumerate(reader, start=2):
            title = _pick_first_non_empty(row, "title", "name", "document_title", "regulation_title")
            if not title:
                skipped += 1
                continue

            version_no = _safe_int(_pick_first_non_empty(row, "version_no", "version", "rev"), 1) or 1
            ref = _pick_first_non_empty(row, "ref", "document_ref", "vref")
            doc_no = _pick_first_non_empty(row, "doc_no", "document_no", "code")
            effective_from_raw = _pick_first_non_empty(row, "effective_from", "effective_date", "effective_time")

            legacy_id = _safe_int(_pick_first_non_empty(row, "legacy_id", "id", "source_id"))
            if legacy_id is None:
                legacy_id = _stable_legacy_key(ref, doc_no, title, version_no, effective_from_raw)

            doc_type = _pick_first_non_empty(row, "doc_type", "type", "category_type") or "REGULATION"
            jurisdiction = _pick_first_non_empty(row, "jurisdiction", "region", "country") or "CN"
            publisher = _pick_first_non_empty(row, "publisher", "issued_by", "agency")
            status = (_pick_first_non_empty(row, "status", "doc_status") or "EFFECTIVE").upper()
            if status not in allowed_status:
                status = "EFFECTIVE"
            category = _pick_first_non_empty(row, "category", "topic")
            keywords = _pick_first_non_empty(row, "keywords", "tags")
            summary = _pick_first_non_empty(row, "summary", "abstract", "description")
            source_url = _pick_first_non_empty(row, "source_url", "url", "source")

            effective_from = _safe_datetime(effective_from_raw)
            effective_to = _safe_datetime(_pick_first_non_empty(row, "effective_to", "expire_at", "expired_at"))
            published_at = _safe_datetime(_pick_first_non_empty(row, "published_at", "publish_time", "published_time"))
            content_hash = _pick_first_non_empty(row, "content_hash", "hash")
            content_text = _pick_first_non_empty(row, "content_text", "content", "full_text")
            attachment_url = _pick_first_non_empty(row, "attachment_url", "file_url", "document_url")
            source_note = _pick_first_non_empty(row, "source_note", "note", "remark")
            now = datetime.now()

            try:
                document_id = None
                if ref:
                    pg_cur.execute(
                        """
                        SELECT id
                        FROM regulation_documents
                        WHERE tenant_id=%s AND ref=%s AND deleted=FALSE
                        ORDER BY id
                        LIMIT 1
                        """,
                        (TENANT_ID, ref),
                    )
                    found = pg_cur.fetchone()
                    document_id = found[0] if found else None
                if document_id is None and doc_no:
                    pg_cur.execute(
                        """
                        SELECT id
                        FROM regulation_documents
                        WHERE tenant_id=%s AND doc_no=%s AND deleted=FALSE
                        ORDER BY id
                        LIMIT 1
                        """,
                        (TENANT_ID, doc_no),
                    )
                    found = pg_cur.fetchone()
                    document_id = found[0] if found else None
                if document_id is None:
                    pg_cur.execute(
                        """
                        SELECT id
                        FROM regulation_documents
                        WHERE tenant_id=%s AND title=%s AND deleted=FALSE
                        ORDER BY id
                        LIMIT 1
                        """,
                        (TENANT_ID, title),
                    )
                    found = pg_cur.fetchone()
                    document_id = found[0] if found else None

                if document_id is None:
                    pg_cur.execute(
                        """
                        INSERT INTO regulation_documents (
                            legacy_id, doc_no, title, doc_type, jurisdiction, publisher,
                            status, category, keywords, summary, source_url, ref,
                            deleted, tenant_id, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            FALSE, %s, %s, %s
                        )
                        RETURNING id
                        """,
                        (
                            legacy_id,
                            doc_no or None,
                            title,
                            doc_type,
                            jurisdiction,
                            publisher or None,
                            status,
                            category or None,
                            keywords or None,
                            summary or None,
                            source_url or None,
                            ref or None,
                            TENANT_ID,
                            now,
                            now,
                        ),
                    )
                    document_id = pg_cur.fetchone()[0]
                    inserted_docs += 1
                else:
                    pg_cur.execute(
                        """
                        UPDATE regulation_documents
                        SET legacy_id = COALESCE(%s, legacy_id),
                            doc_no = COALESCE(%s, doc_no),
                            title = %s,
                            doc_type = %s,
                            jurisdiction = %s,
                            publisher = COALESCE(%s, publisher),
                            status = %s,
                            category = COALESCE(%s, category),
                            keywords = COALESCE(%s, keywords),
                            summary = COALESCE(%s, summary),
                            source_url = COALESCE(%s, source_url),
                            ref = COALESCE(%s, ref),
                            deleted = FALSE,
                            updated_at = %s
                        WHERE id=%s
                        """,
                        (
                            legacy_id,
                            doc_no or None,
                            title,
                            doc_type,
                            jurisdiction,
                            publisher or None,
                            status,
                            category or None,
                            keywords or None,
                            summary or None,
                            source_url or None,
                            ref or None,
                            now,
                            document_id,
                        ),
                    )
                    updated_docs += 1

                pg_cur.execute(
                    """
                    SELECT id
                    FROM regulation_versions
                    WHERE tenant_id=%s AND document_id=%s AND version_no=%s
                    LIMIT 1
                    """,
                    (TENANT_ID, document_id, version_no),
                )
                existed = pg_cur.fetchone()
                if existed:
                    pg_cur.execute(
                        """
                        UPDATE regulation_versions
                        SET effective_from=%s,
                            effective_to=%s,
                            published_at=%s,
                            content_hash=COALESCE(%s, content_hash),
                            content_text=COALESCE(%s, content_text),
                            attachment_url=COALESCE(%s, attachment_url),
                            source_note=COALESCE(%s, source_note),
                            updated_at=%s
                        WHERE id=%s
                        """,
                        (
                            effective_from,
                            effective_to,
                            published_at,
                            content_hash or None,
                            content_text or None,
                            attachment_url or None,
                            source_note or None,
                            now,
                            existed[0],
                        ),
                    )
                    updated_versions += 1
                else:
                    pg_cur.execute(
                        """
                        INSERT INTO regulation_versions (
                            document_id, version_no, effective_from, effective_to, published_at,
                            content_hash, content_text, attachment_url, source_note,
                            tenant_id, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s
                        )
                        """,
                        (
                            document_id,
                            version_no,
                            effective_from,
                            effective_to,
                            published_at,
                            content_hash or None,
                            content_text or None,
                            attachment_url or None,
                            source_note or None,
                            TENANT_ID,
                            now,
                            now,
                        ),
                    )
                    inserted_versions += 1

                log_migration(pg_cur, "regulation", legacy_id, document_id, "SUCCESS")
                processed += 1
            except Exception as e:
                log.error("  regulation line=%s failed: %s", line_no, e)
                pg_conn.rollback()
                fallback_legacy_id = legacy_id if legacy_id is not None else line_no
                try:
                    log_migration(pg_cur, "regulation", fallback_legacy_id, None, "FAILED", str(e))
                    pg_conn.commit()
                except Exception:
                    pg_conn.rollback()
                continue

            if processed % BATCH_SIZE == 0:
                pg_conn.commit()

    pg_conn.commit()
    log.info(
        "  regulation migration complete: docs(inserted=%s updated=%s), versions(inserted=%s updated=%s), skipped=%s",
        inserted_docs,
        updated_docs,
        inserted_versions,
        updated_versions,
        skipped,
    )
    pg_conn.close()


def map_worker_qual_type(row):
    reg_id = int(row.get("regprofession_id") or 0)
    code = str(row.get("regprofession_code") or "").strip()
    parent_code = str(row.get("regprofession_parent_code") or "").strip()

    if reg_id in (2, 3, 4) or code.startswith("001"):
        return "REG_ARCH"
    if reg_id in (5, 7, 8) or code.startswith("002"):
        if reg_id == 8 or code == "00202":
            return "REG_STRUCTURE_2"
        return "REG_STRUCTURE"
    if reg_id in (6, 9, 10, 11) or parent_code == "003":
        if reg_id == 9 or code == "00301":
            return "REG_MEP_WATER"
        if reg_id == 10 or code == "00303":
            return "REG_MEP_POWER"
        if reg_id == 11 or code == "00302":
            return "REG_MEP_HVAC"
        return "REG_MECH"
    if reg_id == 12 or code.startswith("004"):
        return "REG_ELECTRIC_POWER"
    if reg_id == 13 or code.startswith("005"):
        return "REG_CIVIL"
    if reg_id == 14 or code.startswith("006"):
        return "REG_COST"
    if reg_id == 15 or code.startswith("007"):
        return "REG_SAFETY"
    if reg_id == 16 or code.startswith("008"):
        return "REG_CIVIL"
    if reg_id in (18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31) or code.startswith("010") or code.startswith("011"):
        return "REG_CIVIL"
    return ""


def normalize_worker_cert_no(raw, worker_id, qual_type):
    value = str(raw or "").strip()
    if value in ("", "#", "*", "-", "/", "0", "NULL", "null"):
        return f"LEGACY-WORKER-{worker_id}-{qual_type}"
    return value


def normalize_worker_attachment(raw):
    value = str(raw or "").strip()
    if value in ("", "#", "*", "-", "/", "0", "NULL", "null"):
        return ""
    return value


def first_non_empty(*values):
    for v in values:
        s = str(v or "").strip()
        if s:
            return s
    return ""


def extract_worker_level(reg_name, title):
    reg_name = str(reg_name or "").lower()
    title = str(title or "").lower()
    if ("level 1" in reg_name) or ("\u4e00\u7ea7" in reg_name):
        return "L1"
    if ("level 2" in reg_name) or ("\u4e8c\u7ea7" in reg_name):
        return "L2"
    if ("senior" in title) or ("\u9ad8\u7ea7" in title):
        return "SENIOR"
    if ("assistant" in title) or ("\u52a9\u7406" in title):
        return "ASSISTANT"
    if ("engineer" in title) or ("\u5de5\u7a0b\u5e08" in title):
        return "MID"
    return ""


def default_max_concurrent(qual_type):
    if qual_type in ("REG_STRUCTURE", "REG_ARCH"):
        return 6
    if qual_type in (
        "REG_COST",
        "REG_CIVIL",
        "REG_ELECTRIC_POWER",
        "REG_MEP_POWER",
        "REG_MEP_WATER",
        "REG_MEP_HVAC",
        "REG_MECH",
    ):
        return 5
    return 4


def _safe_int(value, default=None):
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _safe_decimal(value):
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _pick_first_non_empty(row, *keys):
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _safe_datetime(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    )
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _stable_legacy_key(*parts):
    seed = "|".join(str(p or "") for p in parts)
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:15]
    return int(digest, 16)


def _as_bool(value):
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    try:
        return int(value) != 0
    except Exception:
        return str(value).strip().lower() in ("true", "yes", "y")


def _jsonb_payload(row):
    def convert(v):
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, str):
            return v.replace("\x00", "")
        if isinstance(v, bytes):
            return {
                "__type__": "bytes_base64",
                "value": base64.b64encode(v).decode("ascii"),
            }
        return v

    return psycopg2.extras.Json({k: convert(v) for k, v in row.items()})


def _ensure_traceability_prereq(pg_cur):
    required_tables = [
        "costticket_items",
        "payment_items",
        "payment_attachments",
        "contract_details",
        "contract_attributes",
        "contract_attachments",
        "invoice_items",
        "drawing_attachments",
        "bankflow_entries",
    ]
    required_columns = [
        ("approve_tasks", "legacy_id"),
        ("approve_records", "legacy_id"),
        ("approve_flows", "legacy_oid"),
        ("payments", "legacy_balance_id"),
        ("payments", "serial_number"),
        ("costtickets", "flow_id"),
    ]

    missing = []
    for table_name in required_tables:
        pg_cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name=%s
            """,
            (table_name,),
        )
        if not pg_cur.fetchone():
            missing.append(f"table:{table_name}")

    for table_name, column_name in required_columns:
        pg_cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s AND column_name=%s
            """,
            (table_name, column_name),
        )
        if not pg_cur.fetchone():
            missing.append(f"column:{table_name}.{column_name}")

    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(
            "traceability schema is not ready (%s). "
            "Run: psql \"$DATABASE_URL\" -f scripts/migrations/20260304_traceability_phase2.sql"
            % joined
        )


def _ensure_traceability_extra_tables(pg_cur):
    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS approve_flow_approvals (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            flow_id BIGINT REFERENCES approve_flows(id) ON DELETE CASCADE,
            task_id BIGINT REFERENCES approve_tasks(id) ON DELETE SET NULL,
            legacy_flow_id BIGINT,
            hierarchy INT,
            legacy_user_id BIGINT,
            actor_ref VARCHAR(255),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_approve_flow_approvals_flow
        ON approve_flow_approvals(flow_id, hierarchy)
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_approve_flow_approvals_legacy_flow
        ON approve_flow_approvals(legacy_flow_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS balance_invoices (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            balance_id BIGINT REFERENCES balances(id) ON DELETE SET NULL,
            balance_legacy_id BIGINT,
            contract_id BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
            amount DECIMAL(19,2),
            money DECIMAL(19,2),
            invoice DECIMAL(19,2),
            management DECIMAL(19,2),
            file_bond_money DECIMAL(19,2),
            fast_money DECIMAL(19,2),
            management_rate DECIMAL(10,4),
            rate INT,
            tax_expenses DECIMAL(10,4),
            bank_name VARCHAR(255),
            bank_no VARCHAR(255),
            unit VARCHAR(255),
            balance_type INT,
            invoice_type INT,
            settlement_type INT,
            fast_type INT,
            file_num VARCHAR(255),
            bond_type_check INT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_balance_invoices_balance
        ON balance_invoices(balance_id)
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_balance_invoices_contract
        ON balance_invoices(contract_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gathering_items (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            gathering_id BIGINT REFERENCES gatherings(id) ON DELETE SET NULL,
            gathering_legacy_id BIGINT,
            contract_id BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
            contract_legacy_id BIGINT,
            invoice_id BIGINT REFERENCES invoices(id) ON DELETE SET NULL,
            invoice_legacy_id BIGINT,
            relation_state INT,
            money DECIMAL(19,2),
            invoice_money DECIMAL(19,2),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gathering_items_gathering
        ON gathering_items(gathering_id)
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gathering_items_contract
        ON gathering_items(contract_id)
        """
    )


def _ensure_business_extra_tables(pg_cur):
    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            name VARCHAR(500) NOT NULL,
            state VARCHAR(50),
            address TEXT,
            telephone VARCHAR(100),
            phone VARCHAR(100),
            mail VARCHAR(255),
            charger_name VARCHAR(255),
            charger_phone VARCHAR(100),
            charger_position VARCHAR(100),
            bank_name VARCHAR(255),
            bank_no VARCHAR(255),
            bank_account VARCHAR(255),
            deposit_bank VARCHAR(255),
            taxpayer_no VARCHAR(255),
            card_number VARCHAR(255),
            job VARCHAR(255),
            principal VARCHAR(255),
            extra TEXT,
            deleted BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_customers_company
        ON customers(company_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS balance_records (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            balance_id BIGINT REFERENCES balances(id) ON DELETE SET NULL,
            balance_legacy_id BIGINT,
            money DECIMAL(19,2),
            before_money DECIMAL(19,2),
            after_money DECIMAL(19,2),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_balance_records_balance
        ON balance_records(balance_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gathering_records (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            gathering_id BIGINT REFERENCES gatherings(id) ON DELETE SET NULL,
            gathering_legacy_id BIGINT,
            money DECIMAL(19,2),
            before_money DECIMAL(19,2),
            after_money DECIMAL(19,2),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gathering_records_gathering
        ON gathering_records(gathering_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_records (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            invoice_id BIGINT REFERENCES invoices(id) ON DELETE SET NULL,
            invoice_legacy_id BIGINT,
            money DECIMAL(19,2),
            before_money DECIMAL(19,2),
            after_money DECIMAL(19,2),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_invoice_records_invoice
        ON invoice_records(invoice_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_creations (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            legacy_parent_id BIGINT,
            parent_id BIGINT REFERENCES contract_creations(id) ON DELETE SET NULL,
            name TEXT,
            contract_number VARCHAR(255),
            contract_type INT,
            signing_type INT,
            zb_wt VARCHAR(100),
            state VARCHAR(100),
            store_state INT,
            leader VARCHAR(255),
            leader_phone VARCHAR(100),
            contacts VARCHAR(255),
            contacts_phone VARCHAR(100),
            size TEXT,
            note TEXT,
            contract_money DECIMAL(19,2),
            investment_money DECIMAL(19,2),
            sign_date TEXT,
            confirm_date TEXT,
            flow_id BIGINT,
            owner_legacy_id BIGINT,
            user_legacy_id BIGINT,
            draft BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_creation_attachments (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            contract_creation_id BIGINT REFERENCES contract_creations(id) ON DELETE CASCADE,
            contract_creation_legacy_id BIGINT,
            filename VARCHAR(500),
            url TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_contract_creation_attachments_creation
        ON contract_creation_attachments(contract_creation_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_extras (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            contract_id BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
            contract_creation_id BIGINT REFERENCES contract_creations(id) ON DELETE SET NULL,
            contract_creation_legacy_id BIGINT,
            state VARCHAR(100),
            payment_type VARCHAR(100),
            binding_style VARCHAR(100),
            sender VARCHAR(255),
            receiver VARCHAR(255),
            submitter VARCHAR(255),
            stamper VARCHAR(255),
            printer VARCHAR(255),
            contact VARCHAR(255),
            mailing_address TEXT,
            express_number VARCHAR(255),
            express_file TEXT,
            express_date TEXT,
            stamp_date TEXT,
            application_time TEXT,
            received_date TEXT,
            stamp_require TEXT,
            note TEXT,
            publish_num INT,
            sealed TEXT,
            mailed TEXT,
            received TEXT,
            plan_receiver TEXT,
            real_receiver TEXT,
            legacy_user_id BIGINT,
            sender_user_id BIGINT,
            receiver_user_id BIGINT,
            submitter_id BIGINT,
            stamper_user_id BIGINT,
            receiver_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_contract_extras_contract
        ON contract_extras(contract_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_extra_attachments (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            contract_extra_id BIGINT REFERENCES contract_extras(id) ON DELETE CASCADE,
            contract_extra_legacy_id BIGINT,
            filename VARCHAR(500),
            url TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_contract_extra_attachments_extra
        ON contract_extra_attachments(contract_extra_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bid_assures (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            approve_task_id BIGINT REFERENCES approve_tasks(id) ON DELETE SET NULL,
            legacy_user_id BIGINT,
            assure_number VARCHAR(255),
            project TEXT,
            purpose TEXT,
            state VARCHAR(100),
            state_back VARCHAR(100),
            state_return VARCHAR(100),
            pay_type INT,
            assure_type INT,
            partner_type INT,
            payee VARCHAR(255),
            payer VARCHAR(255),
            assure_payee VARCHAR(255),
            partner VARCHAR(255),
            other VARCHAR(255),
            other_phone VARCHAR(100),
            piao_hao VARCHAR(255),
            assure_fund DECIMAL(19,2),
            import_money DECIMAL(19,2),
            money_back DECIMAL(19,2),
            return_money DECIMAL(19,2),
            assure_fund_chinese VARCHAR(255),
            pay_date TEXT,
            import_date TEXT,
            money_back_date TEXT,
            return_pay_date TEXT,
            time_end TEXT,
            bank_name VARCHAR(255),
            bank_account VARCHAR(255),
            assure_bank_name VARCHAR(255),
            assure_bank_account VARCHAR(255),
            return_bank_name VARCHAR(255),
            return_bank_account VARCHAR(255),
            return_payee VARCHAR(255),
            return_payer VARCHAR(255),
            return_zhuanyuan VARCHAR(255),
            tou_zhuanyuan VARCHAR(255),
            pay_voucher TEXT,
            return_file TEXT,
            bid_file TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bid_assures_company
        ON bid_assures(company_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bid_assure_flows (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            bid_assure_id BIGINT REFERENCES bid_assures(id) ON DELETE CASCADE,
            bid_assure_legacy_id BIGINT,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            bankflow_entry_id BIGINT REFERENCES bankflow_entries(id) ON DELETE SET NULL,
            legacy_bankflow_id BIGINT,
            legacy_user_id BIGINT,
            project TEXT,
            note TEXT,
            opposite_name VARCHAR(255),
            payee VARCHAR(255),
            assure_payee VARCHAR(255),
            piao_hao VARCHAR(255),
            assure_fund DECIMAL(19,2),
            import_money DECIMAL(19,2),
            money_back DECIMAL(19,2),
            return_money DECIMAL(19,2),
            pay_date TEXT,
            import_date TEXT,
            money_back_date TEXT,
            return_pay_date TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bid_assure_flows_assure
        ON bid_assure_flows(bid_assure_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_archives (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            contract_id BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
            contract_legacy_id BIGINT,
            archive_date TIMESTAMPTZ,
            archive_note TEXT,
            archive_operator VARCHAR(255),
            check_date TIMESTAMPTZ,
            check_note TEXT,
            check_operator VARCHAR(255),
            signing_time TIMESTAMPTZ,
            create_by VARCHAR(255),
            update_by VARCHAR(255),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_contract_archives_contract
        ON contract_archives(contract_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gathering_attachments (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            gathering_id BIGINT REFERENCES gatherings(id) ON DELETE SET NULL,
            gathering_legacy_id BIGINT,
            filename VARCHAR(500),
            url TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gathering_attachments_gathering
        ON gathering_attachments(gathering_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS filebonds (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            contract_id BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
            balance_invoice_id BIGINT REFERENCES balance_invoices(id) ON DELETE SET NULL,
            balance_invoice_legacy_id BIGINT,
            user_legacy_id BIGINT,
            state VARCHAR(100),
            bond_fund DECIMAL(19,2),
            bond_type INT,
            bond_number VARCHAR(255),
            partner_type INT,
            return_file TEXT,
            return_pay_date TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_filebonds_contract
        ON filebonds(contract_id)
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_filebonds_balance_invoice
        ON filebonds(balance_invoice_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS project_file_uploads (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            user_legacy_id BIGINT,
            name TEXT,
            note TEXT,
            leader VARCHAR(255),
            sign_date TIMESTAMPTZ,
            category_legacy_id BIGINT,
            industry_legacy_id BIGINT,
            contract_money DECIMAL(19,2),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS project_files (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            project_file_upload_id BIGINT REFERENCES project_file_uploads(id) ON DELETE SET NULL,
            project_file_upload_legacy_id BIGINT,
            filename VARCHAR(500),
            url TEXT,
            state VARCHAR(50),
            project_file_type VARCHAR(255),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_project_files_upload
        ON project_files(project_file_upload_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_cancels (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            contract_id BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
            contract_legacy_id BIGINT,
            cancel_note TEXT,
            extra TEXT,
            deleted BOOLEAN NOT NULL DEFAULT FALSE,
            create_by VARCHAR(255),
            update_by VARCHAR(255),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_contract_cancels_contract
        ON contract_cancels(contract_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS project_partners (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            name VARCHAR(255),
            id_card VARCHAR(255),
            id_card_scanning TEXT,
            rel_cert_scanning TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_project_partners_company
        ON project_partners(company_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_contracts (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            user_legacy_id BIGINT,
            name VARCHAR(500),
            filename VARCHAR(500),
            url TEXT,
            state VARCHAR(100),
            start_date TIMESTAMPTZ,
            end_date TIMESTAMPTZ,
            upload_time TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_company_contracts_company
        ON company_contracts(company_id)
        """
    )


def _ensure_business_phase3_tables(pg_cur):
    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS balance_print_records (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            balance_id BIGINT REFERENCES balances(id) ON DELETE SET NULL,
            balance_legacy_id BIGINT,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            employee_legacy_id BIGINT,
            ip VARCHAR(64),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_balance_print_records_balance
        ON balance_print_records(balance_id)
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS balance_fast_settlement_quotas (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            employee_legacy_id BIGINT,
            user_legacy_id BIGINT,
            flow_id BIGINT,
            number VARCHAR(255),
            year INT,
            quota TEXT,
            times INT,
            state TEXT,
            apply_state TEXT,
            note TEXT,
            file TEXT,
            bank_name TEXT,
            unit_name TEXT,
            bank_account TEXT,
            apply_date TEXT,
            apply_complete_date TEXT,
            end_date TEXT,
            draft TEXT,
            store_state TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS balance_fast_settlements (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            quota_id BIGINT REFERENCES balance_fast_settlement_quotas(id) ON DELETE SET NULL,
            quota_legacy_id BIGINT,
            gathering_id BIGINT REFERENCES gatherings(id) ON DELETE SET NULL,
            gathering_legacy_id BIGINT,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            employee_legacy_id BIGINT,
            user_legacy_id BIGINT,
            flow_id BIGINT,
            number VARCHAR(255),
            money TEXT,
            state TEXT,
            note TEXT,
            bank_name TEXT,
            unit_name TEXT,
            bank_account TEXT,
            draft TEXT,
            store_state TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS balance_fast_settlement_quota_files (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            quota_id BIGINT REFERENCES balance_fast_settlement_quotas(id) ON DELETE SET NULL,
            quota_legacy_id BIGINT,
            filename VARCHAR(500),
            url TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settlement_manage_fees (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            quarter_legacy_id BIGINT,
            name TEXT,
            number VARCHAR(255),
            state TEXT,
            note TEXT,
            extra TEXT,
            total_money TEXT,
            paid_money TEXT,
            lastest_payment_date TEXT,
            gathering_card_number TEXT,
            gathering_account_type TEXT,
            settlement_period_begin TEXT,
            settlement_period_end TEXT,
            deleted BOOLEAN NOT NULL DEFAULT FALSE,
            version BIGINT,
            create_by VARCHAR(255),
            update_by VARCHAR(255),
            source_tenant_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settlement_manage_fee_gatherings (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            settlement_manage_fee_id BIGINT REFERENCES settlement_manage_fees(id) ON DELETE SET NULL,
            settlement_manage_fee_legacy_id BIGINT,
            gathering_id BIGINT REFERENCES gatherings(id) ON DELETE SET NULL,
            gathering_legacy_id BIGINT,
            deleted BOOLEAN NOT NULL DEFAULT FALSE,
            version BIGINT,
            create_by VARCHAR(255),
            update_by VARCHAR(255),
            source_tenant_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_scraps (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            invoice_id BIGINT REFERENCES invoices(id) ON DELETE SET NULL,
            invoice_legacy_id BIGINT,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            employee_legacy_id BIGINT,
            user_legacy_id BIGINT,
            flow_id BIGINT,
            note TEXT,
            state TEXT,
            scrap_type TEXT,
            invoice_state TEXT,
            money TEXT,
            draft TEXT,
            store_state TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_gatherings (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            employee_legacy_id BIGINT,
            user_legacy_id BIGINT,
            flow_id BIGINT,
            settlement_manage_fee_id BIGINT REFERENCES settlement_manage_fees(id) ON DELETE SET NULL,
            settlement_manage_fee_legacy_id BIGINT,
            name TEXT,
            note TEXT,
            extra TEXT,
            money TEXT,
            state TEXT,
            invoice TEXT,
            card_number TEXT,
            gathering_number TEXT,
            gathering_type TEXT,
            gathering_date TEXT,
            collection_time TEXT,
            gathering_account_type TEXT,
            catalog BIGINT,
            relation_state BIGINT,
            draft TEXT,
            deleted BOOLEAN NOT NULL DEFAULT FALSE,
            version BIGINT,
            store_state TEXT,
            create_by VARCHAR(255),
            update_by VARCHAR(255),
            source_tenant_id BIGINT,
            save_date TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_gathering_files (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            audit_gathering_id BIGINT REFERENCES audit_gatherings(id) ON DELETE SET NULL,
            audit_gathering_legacy_id BIGINT,
            filename VARCHAR(500),
            url TEXT,
            path TEXT,
            extra TEXT,
            deleted BOOLEAN,
            version BIGINT,
            create_by VARCHAR(255),
            update_by VARCHAR(255),
            source_tenant_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_invoices (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            employee_legacy_id BIGINT,
            user_legacy_id BIGINT,
            flow_id BIGINT,
            note TEXT,
            money TEXT,
            state TEXT,
            draft TEXT,
            store_state TEXT,
            address TEXT,
            telephone TEXT,
            card_number TEXT,
            deposit_bank TEXT,
            deposit_name TEXT,
            invoice_type TEXT,
            invoice_content TEXT,
            invoice_number TEXT,
            identification_num TEXT,
            apply_date TEXT,
            upload_file TEXT,
            audit_number TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_receipt_invoiced_links (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            audit_invoice_id BIGINT REFERENCES audit_invoices(id) ON DELETE SET NULL,
            audit_invoice_legacy_id BIGINT,
            audit_gathering_id BIGINT REFERENCES audit_gatherings(id) ON DELETE SET NULL,
            audit_gathering_legacy_id BIGINT,
            money TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bank_infos (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            employee_legacy_id BIGINT,
            gathering_id BIGINT REFERENCES gatherings(id) ON DELETE SET NULL,
            gathering_legacy_id BIGINT,
            user_legacy_id BIGINT,
            card_number TEXT,
            account_type TEXT,
            gathering_account_type TEXT,
            deposit_bank TEXT,
            company_name TEXT,
            account TEXT,
            entry_and_exit TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_manage_moneys (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            industry_legacy_id BIGINT,
            program_type TEXT,
            rate TEXT,
            rate1 TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_year_manage_moneys (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            year INT,
            manage_fee TEXT,
            contract_fee TEXT,
            brand_earnest_fee TEXT,
            note TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_auth_areas (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            area_legacy_id BIGINT,
            deleted BOOLEAN NOT NULL DEFAULT FALSE,
            version BIGINT,
            create_by VARCHAR(255),
            update_by VARCHAR(255),
            source_tenant_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_nos (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            data_key VARCHAR(255),
            no INT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS project_reports (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            project_name TEXT,
            owner_name TEXT,
            owners TEXT,
            owner_iphone TEXT,
            intermediator TEXT,
            project_money TEXT,
            report_number TEXT,
            project_notes TEXT,
            upload_file TEXT,
            add_times TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS project_report_files (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            project_report_id BIGINT REFERENCES project_reports(id) ON DELETE SET NULL,
            project_report_legacy_id BIGINT,
            filename VARCHAR(500),
            url TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS drawing_file_types (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            name VARCHAR(255),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS approve_flow_definitions_legacy (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            name VARCHAR(255),
            note TEXT,
            view_url TEXT,
            update_url TEXT,
            state_url TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS approve_flow_definition_items_legacy (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            flow_definition_id BIGINT REFERENCES approve_flow_definitions_legacy(id) ON DELETE SET NULL,
            flow_definition_legacy_id BIGINT,
            user_legacy_id BIGINT,
            name VARCHAR(255),
            type TEXT,
            catalog TEXT,
            hierarchy BIGINT,
            work_url TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS approve_flow_by_areas_legacy (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            area_extra_legacy_id BIGINT,
            flow_define_legacy_id BIGINT,
            old_flow_define_legacy_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS approve_flow_by_zones_legacy (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            zone_legacy_id BIGINT,
            flow_define_legacy_id BIGINT,
            old_flow_define_legacy_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payrolls_legacy (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            worker_legacy_id BIGINT,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            employee_legacy_id BIGINT,
            user_legacy_id BIGINT,
            month_key TEXT,
            money TEXT,
            total_money TEXT,
            real_wages TEXT,
            pre_tax TEXT,
            basic_salary TEXT,
            bonus TEXT,
            debit TEXT,
            social_security TEXT,
            medicare TEXT,
            income_tax TEXT,
            fund TEXT,
            company_cost TEXT,
            draft TEXT,
            store_state TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS balance_records_legacy (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            contract_id BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
            contract_legacy_id BIGINT,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            employee_legacy_id BIGINT,
            user_legacy_id BIGINT,
            flow_id BIGINT,
            file TEXT,
            drawing TEXT,
            record_reason TEXT,
            record_type TEXT,
            state TEXT,
            draft TEXT,
            store_state TEXT,
            money TEXT,
            used_money TEXT,
            ticket_used_money TEXT,
            rate TEXT,
            rate1 TEXT,
            rate3 TEXT,
            rate5 TEXT,
            rate6 TEXT,
            rate9 TEXT,
            rate13 TEXT,
            rate16 TEXT,
            rate_common TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS workers_legacy (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            company_legacy_id BIGINT,
            user_legacy_id BIGINT,
            name VARCHAR(255),
            phone VARCHAR(100),
            no VARCHAR(255),
            sex TEXT,
            state TEXT,
            profession TEXT,
            title TEXT,
            protitle TEXT,
            register TEXT,
            regprofession_legacy_id BIGINT,
            certificate_file TEXT,
            join_date TEXT,
            graduation_date TEXT,
            school TEXT,
            bank_name TEXT,
            bank_no TEXT,
            bank_account TEXT,
            social_security TEXT,
            medicare TEXT,
            fund TEXT,
            basic_salary TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )

    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS log_safety_records (
            id BIGSERIAL PRIMARY KEY,
            legacy_id BIGINT UNIQUE,
            tenant_id INT NOT NULL DEFAULT 10000,
            employee_id BIGINT REFERENCES employees(id) ON DELETE SET NULL,
            employee_legacy_id BIGINT,
            user_legacy_id BIGINT,
            catalog BIGINT,
            model TEXT,
            object TEXT,
            note TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )


def _ensure_legacy_source_rows_table(pg_cur):
    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS legacy_source_rows (
            id BIGSERIAL PRIMARY KEY,
            batch_id BIGINT NOT NULL,
            source_table TEXT NOT NULL,
            legacy_id BIGINT,
            source_pk JSONB,
            row_hash TEXT NOT NULL,
            tenant_id INT NOT NULL DEFAULT 10000,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw JSONB NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE (batch_id, source_table, row_hash)
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_legacy_source_rows_table_legacy
        ON legacy_source_rows(source_table, legacy_id)
        """
    )


def _sync_migration_log_from_raw(pg_cur, batch_id: int, source_table: str, target_table: str):
    pg_cur.execute(
        f"""
        INSERT INTO migration_log (table_name, legacy_id, new_id, status, error_msg)
        SELECT %s, s.legacy_id, t.id, 'SUCCESS', NULL
        FROM (
            SELECT (row_data->>'id')::bigint AS legacy_id
            FROM icrm_raw.landing_rows
            WHERE batch_id=%s AND table_name=%s
        ) s
        LEFT JOIN {target_table} t ON t.legacy_id=s.legacy_id
        ON CONFLICT (table_name, legacy_id) DO UPDATE
        SET new_id=EXCLUDED.new_id,
            status=EXCLUDED.status,
            error_msg=EXCLUDED.error_msg,
            migrated_at=NOW()
        """,
        (source_table, batch_id, source_table),
    )


def _load_legacy_id_map(pg_cur, table):
    pg_cur.execute(f"SELECT legacy_id, id FROM {table} WHERE legacy_id IS NOT NULL")
    data = {}
    for legacy_id, row_id in pg_cur.fetchall():
        if legacy_id is None:
            continue
        data[int(legacy_id)] = int(row_id)
    return data


def _load_actor_ref_map(pg_cur):
    pg_cur.execute(
        """
        SELECT user_id, id, COALESCE(NULLIF(executor_ref, ''), '')
        FROM employees
        WHERE user_id IS NOT NULL
        """
    )
    m = {}
    for user_id, employee_id, executor_ref in pg_cur.fetchall():
        uid = _safe_int(user_id)
        if uid is None:
            continue
        if executor_ref:
            m[uid] = executor_ref
        else:
            m[uid] = f"v://cn.zhongbei/executor/person/legacy-{employee_id}@v1"
    return m


def _actor_ref(actor_map, user_id):
    uid = _safe_int(user_id)
    if uid is None:
        return "legacy:user/unknown"
    return actor_map.get(uid, f"legacy:user/{uid}")


def _flow_state_from_legacy(state):
    s = _safe_int(state, 0)
    if s in (2, 3):
        return "APPROVED"
    if s in (-1, 4, 5):
        return "REJECTED"
    if s in (9, 10):
        return "WITHDRAWN"
    return "PENDING"


def _task_state_from_legacy(state):
    s = _safe_int(state, 0)
    if s in (2, 3, 4, 5):
        return "DONE"
    if s in (-1, 9, 10):
        return "SKIPPED"
    return "WAITING"


def _record_action_from_legacy(state):
    s = _safe_int(state, 0)
    if s in (2, 3):
        return "APPROVE"
    if s in (-1, 4, 5):
        return "REJECT"
    if s in (9, 10):
        return "WITHDRAW"
    return "ACT"


def _infer_approve_biz(catalog, oid, legacy_maps, biz_ref_maps):
    oid_i = _safe_int(oid)
    cat_i = _safe_int(catalog, 0)

    cat_pref = {
        1: "CONTRACT",
        2: "GATHERING",
        3: "BALANCE",
        4: "INVOICE",
        5: "DRAWING",
        6: "COST_TICKET",
        7: "PAYMENT",
    }
    ordered = []
    preferred = cat_pref.get(cat_i)
    if preferred:
        ordered.append(preferred)
    ordered.extend(
        [x for x in ("CONTRACT", "GATHERING", "BALANCE", "INVOICE", "DRAWING", "COST_TICKET", "PAYMENT") if x not in ordered]
    )

    if oid_i is not None:
        for biz_type in ordered:
            pg_id = legacy_maps.get(biz_type, {}).get(oid_i)
            if pg_id is None:
                continue
            biz_ref = biz_ref_maps.get(biz_type, {}).get(pg_id, "")
            return biz_type, pg_id, biz_ref

    fallback_type = preferred or f"LEGACY_CATALOG_{cat_i}"
    return fallback_type, oid_i or 0, ""


def migrate_approve_history():
    log.info("=== PHASE 8: migrate approval history ===")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    try:
        _ensure_traceability_prereq(pg_cur)

        legacy_maps = {
            "CONTRACT": _load_legacy_id_map(pg_cur, "contracts"),
            "GATHERING": _load_legacy_id_map(pg_cur, "gatherings"),
            "BALANCE": _load_legacy_id_map(pg_cur, "balances"),
            "INVOICE": _load_legacy_id_map(pg_cur, "invoices"),
            "DRAWING": _load_legacy_id_map(pg_cur, "drawings"),
            "COST_TICKET": _load_legacy_id_map(pg_cur, "costtickets"),
            "PAYMENT": _load_legacy_id_map(pg_cur, "payments"),
        }

        biz_ref_maps = {}
        pg_cur.execute("SELECT id, COALESCE(ref, '') FROM contracts")
        biz_ref_maps["CONTRACT"] = {int(i): ref for i, ref in pg_cur.fetchall()}
        pg_cur.execute("SELECT id, COALESCE(project_ref, '') FROM gatherings")
        biz_ref_maps["GATHERING"] = {int(i): ref for i, ref in pg_cur.fetchall()}
        pg_cur.execute("SELECT id, COALESCE(settlement_ref, '') FROM balances")
        biz_ref_maps["BALANCE"] = {int(i): ref for i, ref in pg_cur.fetchall()}
        pg_cur.execute("SELECT id, COALESCE(project_ref, '') FROM invoices")
        biz_ref_maps["INVOICE"] = {int(i): ref for i, ref in pg_cur.fetchall()}
        pg_cur.execute("SELECT id, COALESCE(project_ref, '') FROM drawings")
        biz_ref_maps["DRAWING"] = {int(i): ref for i, ref in pg_cur.fetchall()}
        pg_cur.execute("SELECT id, COALESCE(project_ref, '') FROM costtickets")
        biz_ref_maps["COST_TICKET"] = {int(i): ref for i, ref in pg_cur.fetchall()}
        pg_cur.execute("SELECT id, COALESCE(project_ref, '') FROM payments")
        biz_ref_maps["PAYMENT"] = {int(i): ref for i, ref in pg_cur.fetchall()}

        actor_map = _load_actor_ref_map(pg_cur)
        flow_map = {}

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, catalog, hierarchy, note,
                   oid, state, title, user_id, flow_id
            FROM approve_flow
            ORDER BY id
            """
        )
        rows = mysql_cur.fetchall()
        log.info("  approve_flow rows: %s", len(rows))

        success = 0
        for row in rows:
            legacy_id = _safe_int(row.get("id"))
            if legacy_id is None:
                continue
            biz_type, biz_id, biz_ref = _infer_approve_biz(
                row.get("catalog"), row.get("oid"), legacy_maps, biz_ref_maps
            )
            biz_ref = str(biz_ref or "")[:500]
            title = str(row.get("title") or "")[:500]
            applicant = _actor_ref(actor_map, row.get("user_id"))[:255]
            flow_state = _flow_state_from_legacy(row.get("state"))
            finished_at = (
                row.get("lastDate")
                if flow_state in ("APPROVED", "REJECTED", "WITHDRAWN")
                else None
            )
            try:
                pg_cur.execute(
                    """
                    INSERT INTO approve_flows (
                        legacy_id, tenant_id, biz_type, biz_id, biz_ref, title,
                        applicant, state, flow_id,
                        legacy_catalog, legacy_hierarchy, legacy_oid, legacy_user_id,
                        created_at, updated_at, finished_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET biz_type = EXCLUDED.biz_type,
                        biz_id = EXCLUDED.biz_id,
                        biz_ref = EXCLUDED.biz_ref,
                        title = EXCLUDED.title,
                        applicant = EXCLUDED.applicant,
                        state = EXCLUDED.state,
                        flow_id = EXCLUDED.flow_id,
                        legacy_catalog = EXCLUDED.legacy_catalog,
                        legacy_hierarchy = EXCLUDED.legacy_hierarchy,
                        legacy_oid = EXCLUDED.legacy_oid,
                        legacy_user_id = EXCLUDED.legacy_user_id,
                        updated_at = EXCLUDED.updated_at,
                        finished_at = EXCLUDED.finished_at
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        biz_type,
                        biz_id,
                        biz_ref,
                        title,
                        applicant,
                        flow_state,
                        row.get("flow_id"),
                        row.get("catalog"),
                        row.get("hierarchy"),
                        row.get("oid"),
                        row.get("user_id"),
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                        finished_at,
                    ),
                )
                flow_id = pg_cur.fetchone()[0]
                flow_map[legacy_id] = flow_id
                log_migration(pg_cur, "approve_flow", legacy_id, flow_id, "SUCCESS")
                success += 1
            except Exception as e:
                log.error("  approve_flow id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "approve_flow", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  approve_flow migrated: %s/%s", success, len(rows))
        # Rebuild legacy->new id map from DB to avoid stale ids after rollback batches.
        pg_cur.execute("SELECT legacy_id, id FROM approve_flows WHERE legacy_id IS NOT NULL")
        flow_map = {int(legacy_id): int(flow_id) for legacy_id, flow_id in pg_cur.fetchall()}

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, catalog, note, oid, state, style, title, flow_id, user_id
            FROM approve_task
            ORDER BY flow_id, id
            """
        )
        task_rows = mysql_cur.fetchall()
        log.info("  approve_task rows: %s", len(task_rows))
        seq_by_flow = {}
        task_success = 0

        for row in task_rows:
            legacy_id = _safe_int(row.get("id"))
            legacy_flow_id = _safe_int(row.get("flow_id"))
            target_flow_id = flow_map.get(legacy_flow_id)
            if legacy_id is None or target_flow_id is None:
                continue
            seq = seq_by_flow.get(target_flow_id, 0) + 1
            seq_by_flow[target_flow_id] = seq
            task_state = _task_state_from_legacy(row.get("state"))
            acted_at = row.get("lastDate") if task_state in ("DONE", "SKIPPED") else None
            try:
                pg_cur.execute(
                    """
                    INSERT INTO approve_tasks (
                        legacy_id, flow_id, seq, legacy_catalog, legacy_oid, legacy_style, legacy_user_id,
                        approver_ref, state, comment, acted_at, created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET flow_id = EXCLUDED.flow_id,
                        seq = EXCLUDED.seq,
                        legacy_catalog = EXCLUDED.legacy_catalog,
                        legacy_oid = EXCLUDED.legacy_oid,
                        legacy_style = EXCLUDED.legacy_style,
                        legacy_user_id = EXCLUDED.legacy_user_id,
                        approver_ref = EXCLUDED.approver_ref,
                        state = EXCLUDED.state,
                        comment = EXCLUDED.comment,
                        acted_at = EXCLUDED.acted_at
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        target_flow_id,
                        seq,
                        row.get("catalog"),
                        row.get("oid"),
                        row.get("style"),
                        row.get("user_id"),
                        _actor_ref(actor_map, row.get("user_id")),
                        task_state,
                        row.get("note") or row.get("title"),
                        acted_at,
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                    ),
                )
                task_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "approve_task", legacy_id, task_id, "SUCCESS")
                task_success += 1
            except Exception as e:
                log.error("  approve_task id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "approve_task", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if task_success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  approve_task migrated: %s/%s", task_success, len(task_rows))

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, hierarchy, note, state, flow_id, user_id, name
            FROM approve_flow_record
            ORDER BY flow_id, id
            """
        )
        rec_rows = mysql_cur.fetchall()
        log.info("  approve_flow_record rows: %s", len(rec_rows))
        rec_success = 0
        for row in rec_rows:
            legacy_id = _safe_int(row.get("id"))
            target_flow_id = flow_map.get(_safe_int(row.get("flow_id")))
            if legacy_id is None or target_flow_id is None:
                continue
            task_id = None
            seq = _safe_int(row.get("hierarchy"))
            if seq is not None:
                pg_cur.execute(
                    "SELECT id FROM approve_tasks WHERE flow_id=%s AND seq=%s ORDER BY id LIMIT 1",
                    (target_flow_id, seq),
                )
                x = pg_cur.fetchone()
                if x:
                    task_id = x[0]
            if task_id is None:
                pg_cur.execute(
                    "SELECT id FROM approve_tasks WHERE flow_id=%s ORDER BY seq, id LIMIT 1",
                    (target_flow_id,),
                )
                x = pg_cur.fetchone()
                if x:
                    task_id = x[0]
            if task_id is None:
                synthetic_legacy_task_id = 800000000000 + legacy_id
                synthetic_seq = seq if seq is not None and seq > 0 else 9999
                pg_cur.execute(
                    """
                    INSERT INTO approve_tasks (
                        legacy_id, flow_id, seq, legacy_user_id, approver_ref, state, comment, acted_at, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET flow_id = EXCLUDED.flow_id,
                        seq = EXCLUDED.seq,
                        legacy_user_id = EXCLUDED.legacy_user_id,
                        approver_ref = EXCLUDED.approver_ref,
                        state = EXCLUDED.state,
                        comment = EXCLUDED.comment,
                        acted_at = EXCLUDED.acted_at
                    RETURNING id
                    """,
                    (
                        synthetic_legacy_task_id,
                        target_flow_id,
                        synthetic_seq,
                        row.get("user_id"),
                        _actor_ref(actor_map, row.get("user_id")),
                        "DONE",
                        "synthetic task for legacy approve_flow_record",
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                    ),
                )
                task_id = pg_cur.fetchone()[0]
            try:
                pg_cur.execute(
                    """
                    INSERT INTO approve_records (
                        legacy_id, flow_id, task_id, legacy_hierarchy, legacy_state, legacy_user_id,
                        action, actor, comment, created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET flow_id = EXCLUDED.flow_id,
                        task_id = EXCLUDED.task_id,
                        legacy_hierarchy = EXCLUDED.legacy_hierarchy,
                        legacy_state = EXCLUDED.legacy_state,
                        legacy_user_id = EXCLUDED.legacy_user_id,
                        action = EXCLUDED.action,
                        actor = EXCLUDED.actor,
                        comment = EXCLUDED.comment,
                        created_at = EXCLUDED.created_at
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        target_flow_id,
                        task_id,
                        row.get("hierarchy"),
                        row.get("state"),
                        row.get("user_id"),
                        _record_action_from_legacy(row.get("state")),
                        _actor_ref(actor_map, row.get("user_id")),
                        first_non_empty(row.get("note"), row.get("name")),
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                    ),
                )
                rec_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "approve_flow_record", legacy_id, rec_id, "SUCCESS")
                rec_success += 1
            except Exception as e:
                log.error("  approve_flow_record id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "approve_flow_record", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if rec_success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  approve_flow_record migrated: %s/%s", rec_success, len(rec_rows))

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, hierarchy, flow_id, user_id
            FROM approve_flow_approval
            ORDER BY flow_id, hierarchy, id
            """
        )
        approval_rows = mysql_cur.fetchall()
        fallback_added = 0
        for row in approval_rows:
            flow_id = flow_map.get(_safe_int(row.get("flow_id")))
            seq = _safe_int(row.get("hierarchy"))
            if flow_id is None or seq is None or seq <= 0:
                continue
            pg_cur.execute(
                "SELECT 1 FROM approve_tasks WHERE flow_id=%s AND seq=%s LIMIT 1",
                (flow_id, seq),
            )
            if pg_cur.fetchone():
                continue
            try:
                pg_cur.execute(
                    """
                    INSERT INTO approve_tasks (
                        legacy_id, flow_id, seq, legacy_user_id, approver_ref, state, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (legacy_id) DO NOTHING
                    """,
                    (
                        900000000000 + _safe_int(row.get("id"), 0),
                        flow_id,
                        seq,
                        row.get("user_id"),
                        _actor_ref(actor_map, row.get("user_id")),
                        "WAITING",
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                    ),
                )
                fallback_added += pg_cur.rowcount
            except Exception:
                pg_conn.rollback()
                pg_conn.commit()
                continue
        pg_conn.commit()
        log.info("  approval fallback tasks added: %s", fallback_added)
    finally:
        mysql_conn.close()
        pg_conn.close()


def migrate_cost_payment():
    log.info("=== PHASE 9: migrate costticket/payment domain ===")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    try:
        _ensure_traceability_prereq(pg_cur)
        contract_map = _load_legacy_id_map(pg_cur, "contracts")
        employee_map = _load_legacy_id_map(pg_cur, "employees")
        invoice_map = _load_legacy_id_map(pg_cur, "invoices")

        pg_cur.execute("SELECT id, COALESCE(ref, ''), COALESCE(project_ref, '') FROM contracts")
        contract_meta = {int(i): {"ref": ref, "project_ref": p_ref} for i, ref, p_ref in pg_cur.fetchall()}

        mysql_cur.execute(
            """
            SELECT c.id, c.addDate, c.lastDate, c.draft,
                   c.costTicketNumber, c.balanceType, c.bankMoney, c.cashSettlement AS cashMoney,
                   c.bankSettlement, c.cashSettlement, c.VATRate, c.VATSum,
                   c.deductRate, c.deductSum, c.managementCostSum, c.costTicketSum,
                   c.totalInvoice, c.noTicketSum, c.taxExpensesSum, c.state, c.payDate,
                   c.employee_id, c.bank_id, c.payEmployee_id, c.invoice_id, c.flow_id, c.record_id, c.note,
                   br.contract_id AS record_contract_id,
                   i.contract_id AS invoice_contract_id
            FROM costticket c
            LEFT JOIN balance_record br ON br.id = c.record_id
            LEFT JOIN invoice i ON i.id = c.invoice_id
            ORDER BY c.id
            """
        )
        rows = mysql_cur.fetchall()
        log.info("  costticket rows: %s", len(rows))
        success = 0

        for row in rows:
            legacy_id = _safe_int(row.get("id"))
            if legacy_id is None:
                continue
            legacy_contract_id = _safe_int(row.get("record_contract_id")) or _safe_int(row.get("invoice_contract_id"))
            pg_contract_id = contract_map.get(legacy_contract_id) if legacy_contract_id is not None else None
            contract_info = contract_meta.get(pg_contract_id or -1, {})

            state = str(row.get("state") or "").strip().upper()
            mapped_state = "PENDING"
            if "PAID" in state:
                mapped_state = "PAID"
            elif "APPRO" in state:
                mapped_state = "APPROVED"
            elif "REJECT" in state:
                mapped_state = "REJECTED"
            elif row.get("payDate") is not None:
                mapped_state = "PAID"

            try:
                pg_cur.execute(
                    """
                    INSERT INTO costtickets (
                        legacy_id, tenant_id, cost_ticket_number, balance_type,
                        bank_money, cash_money, bank_settlement, cash_settlement,
                        vat_rate, vat_sum, deduct_rate, deduct_sum,
                        management_cost_sum, cost_ticket_sum, total_invoice, no_ticket_sum,
                        state, pay_date, employee_id, bank_id, pay_employee_id,
                        flow_id, invoice_id, record_id, tax_expenses_sum,
                        contract_id, project_ref, note, draft, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET cost_ticket_number = EXCLUDED.cost_ticket_number,
                        balance_type = EXCLUDED.balance_type,
                        bank_money = EXCLUDED.bank_money,
                        cash_money = EXCLUDED.cash_money,
                        bank_settlement = EXCLUDED.bank_settlement,
                        cash_settlement = EXCLUDED.cash_settlement,
                        vat_rate = EXCLUDED.vat_rate,
                        vat_sum = EXCLUDED.vat_sum,
                        deduct_rate = EXCLUDED.deduct_rate,
                        deduct_sum = EXCLUDED.deduct_sum,
                        management_cost_sum = EXCLUDED.management_cost_sum,
                        cost_ticket_sum = EXCLUDED.cost_ticket_sum,
                        total_invoice = EXCLUDED.total_invoice,
                        no_ticket_sum = EXCLUDED.no_ticket_sum,
                        state = EXCLUDED.state,
                        pay_date = EXCLUDED.pay_date,
                        employee_id = EXCLUDED.employee_id,
                        bank_id = EXCLUDED.bank_id,
                        pay_employee_id = EXCLUDED.pay_employee_id,
                        flow_id = EXCLUDED.flow_id,
                        invoice_id = EXCLUDED.invoice_id,
                        record_id = EXCLUDED.record_id,
                        tax_expenses_sum = EXCLUDED.tax_expenses_sum,
                        contract_id = EXCLUDED.contract_id,
                        project_ref = EXCLUDED.project_ref,
                        note = EXCLUDED.note,
                        draft = EXCLUDED.draft,
                        updated_at = EXCLUDED.updated_at
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        row.get("costTicketNumber"),
                        row.get("balanceType"),
                        row.get("bankMoney"),
                        row.get("cashMoney"),
                        row.get("bankSettlement"),
                        row.get("cashSettlement"),
                        row.get("VATRate"),
                        row.get("VATSum"),
                        row.get("deductRate"),
                        row.get("deductSum"),
                        row.get("managementCostSum"),
                        row.get("costTicketSum"),
                        row.get("totalInvoice"),
                        row.get("noTicketSum"),
                        mapped_state,
                        row.get("payDate"),
                        employee_map.get(_safe_int(row.get("employee_id"))),
                        row.get("bank_id"),
                        employee_map.get(_safe_int(row.get("payEmployee_id"))),
                        row.get("flow_id"),
                        invoice_map.get(_safe_int(row.get("invoice_id"))),
                        row.get("record_id"),
                        row.get("taxExpensesSum"),
                        pg_contract_id,
                        contract_info.get("project_ref", ""),
                        row.get("note"),
                        _as_bool(row.get("draft")),
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "costticket", legacy_id, new_id, "SUCCESS")
                success += 1
            except Exception as e:
                log.error("  costticket id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "costticket", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  costticket migrated: %s/%s", success, len(rows))

        costticket_map = _load_legacy_id_map(pg_cur, "costtickets")

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, amount, balanceType, bankName, bankNo,
                   invoice, invoiceType, management, managementRate, money, rate,
                   settlementType, taxExpenses, ticketDate, ticketNumber, unit, costTicket_id
            FROM costticket_invoice
            ORDER BY id
            """
        )
        rows = mysql_cur.fetchall()
        item_success = 0
        for row in rows:
            legacy_id = _safe_int(row.get("id"))
            pg_costticket_id = costticket_map.get(_safe_int(row.get("costTicket_id")))
            if legacy_id is None or pg_costticket_id is None:
                continue
            try:
                pg_cur.execute(
                    """
                    INSERT INTO costticket_items (
                        legacy_id, tenant_id, costticket_id,
                        amount, balance_type, bank_name, bank_no,
                        invoice_amount, invoice_type,
                        management_amount, management_rate, money, rate,
                        settlement_type, tax_expenses, ticket_date, ticket_number, unit,
                        created_at, updated_at, raw
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET costticket_id = EXCLUDED.costticket_id,
                        amount = EXCLUDED.amount,
                        balance_type = EXCLUDED.balance_type,
                        bank_name = EXCLUDED.bank_name,
                        bank_no = EXCLUDED.bank_no,
                        invoice_amount = EXCLUDED.invoice_amount,
                        invoice_type = EXCLUDED.invoice_type,
                        management_amount = EXCLUDED.management_amount,
                        management_rate = EXCLUDED.management_rate,
                        money = EXCLUDED.money,
                        rate = EXCLUDED.rate,
                        settlement_type = EXCLUDED.settlement_type,
                        tax_expenses = EXCLUDED.tax_expenses,
                        ticket_date = EXCLUDED.ticket_date,
                        ticket_number = EXCLUDED.ticket_number,
                        unit = EXCLUDED.unit,
                        updated_at = EXCLUDED.updated_at,
                        raw = EXCLUDED.raw
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        pg_costticket_id,
                        row.get("amount"),
                        row.get("balanceType"),
                        row.get("bankName"),
                        row.get("bankNo"),
                        row.get("invoice"),
                        row.get("invoiceType"),
                        row.get("management"),
                        row.get("managementRate"),
                        row.get("money"),
                        row.get("rate"),
                        row.get("settlementType"),
                        row.get("taxExpenses"),
                        row.get("ticketDate"),
                        row.get("ticketNumber"),
                        row.get("unit"),
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                        _jsonb_payload(row),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "costticket_invoice", legacy_id, new_id, "SUCCESS")
                item_success += 1
            except Exception as e:
                log.error("  costticket_invoice id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "costticket_invoice", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if item_success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  costticket_invoice migrated: %s/%s", item_success, len(rows))

        mysql_cur.execute(
            """
            SELECT
                p.id, p.addDate, p.lastDate, p.paymentDate, p.serialNumber,
                p.employee_id, p.balance_id, g.contract_id AS contract_id,
                COALESCE(SUM(pi.paymentAmount), 0) AS payment_amount
            FROM balance_payment p
            LEFT JOIN balance b ON b.id = p.balance_id
            LEFT JOIN gathering g ON g.id = b.gathering_id
            LEFT JOIN balance_payment_item pi ON pi.balancePayment_id = p.id
            GROUP BY p.id, p.addDate, p.lastDate, p.paymentDate, p.serialNumber,
                     p.employee_id, p.balance_id, g.contract_id
            ORDER BY p.id
            """
        )
        rows = mysql_cur.fetchall()
        log.info("  balance_payment rows: %s", len(rows))
        payment_success = 0
        payment_map = {}
        for row in rows:
            legacy_id = _safe_int(row.get("id"))
            pg_contract_id = contract_map.get(_safe_int(row.get("contract_id")))
            if legacy_id is None or pg_contract_id is None:
                continue
            info = contract_meta.get(pg_contract_id, {})
            pay_state = "PAID" if row.get("paymentDate") else "APPROVED"
            amount = _safe_decimal(row.get("payment_amount")) or Decimal("0")
            try:
                pg_cur.execute(
                    """
                    INSERT INTO payments (
                        legacy_id, amount, pay_date, contract_id, contract_ref, project_ref,
                        legacy_balance_id, serial_number, source_table,
                        bank_id, employee_id, state, note, tenant_id, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET amount = EXCLUDED.amount,
                        pay_date = EXCLUDED.pay_date,
                        contract_id = EXCLUDED.contract_id,
                        contract_ref = EXCLUDED.contract_ref,
                        project_ref = EXCLUDED.project_ref,
                        legacy_balance_id = EXCLUDED.legacy_balance_id,
                        serial_number = EXCLUDED.serial_number,
                        source_table = EXCLUDED.source_table,
                        bank_id = EXCLUDED.bank_id,
                        employee_id = EXCLUDED.employee_id,
                        state = EXCLUDED.state,
                        note = EXCLUDED.note,
                        updated_at = EXCLUDED.updated_at
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        amount,
                        row.get("paymentDate"),
                        pg_contract_id,
                        info.get("ref", ""),
                        info.get("project_ref", ""),
                        row.get("balance_id"),
                        row.get("serialNumber"),
                        "balance_payment",
                        None,
                        employee_map.get(_safe_int(row.get("employee_id"))),
                        pay_state,
                        f"legacy serial={row.get('serialNumber') or ''}",
                        TENANT_ID,
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                payment_map[legacy_id] = new_id
                log_migration(pg_cur, "balance_payment", legacy_id, new_id, "SUCCESS")
                payment_success += 1
            except Exception as e:
                log.error("  balance_payment id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "balance_payment", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if payment_success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  balance_payment migrated: %s/%s", payment_success, len(rows))

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, deductionAmount, paymentAmount, remark,
                   balanceInvoice_id, balancePayment_id
            FROM balance_payment_item
            ORDER BY id
            """
        )
        rows = mysql_cur.fetchall()
        item_success = 0
        for row in rows:
            legacy_id = _safe_int(row.get("id"))
            payment_id = payment_map.get(_safe_int(row.get("balancePayment_id")))
            if legacy_id is None or payment_id is None:
                continue
            try:
                pg_cur.execute(
                    """
                    INSERT INTO payment_items (
                        legacy_id, tenant_id, payment_id, balance_invoice_legacy_id,
                        deduction_amount, payment_amount, remark,
                        created_at, updated_at, raw
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET payment_id = EXCLUDED.payment_id,
                        balance_invoice_legacy_id = EXCLUDED.balance_invoice_legacy_id,
                        deduction_amount = EXCLUDED.deduction_amount,
                        payment_amount = EXCLUDED.payment_amount,
                        remark = EXCLUDED.remark,
                        updated_at = EXCLUDED.updated_at,
                        raw = EXCLUDED.raw
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        payment_id,
                        row.get("balanceInvoice_id"),
                        row.get("deductionAmount"),
                        row.get("paymentAmount"),
                        row.get("remark"),
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                        _jsonb_payload(row),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "balance_payment_item", legacy_id, new_id, "SUCCESS")
                item_success += 1
            except Exception as e:
                log.error("  balance_payment_item id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "balance_payment_item", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if item_success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  balance_payment_item migrated: %s/%s", item_success, len(rows))

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, filename, url, balancePayment_id
            FROM balance_payment_file
            ORDER BY id
            """
        )
        rows = mysql_cur.fetchall()
        file_success = 0
        for row in rows:
            legacy_id = _safe_int(row.get("id"))
            payment_id = payment_map.get(_safe_int(row.get("balancePayment_id")))
            if legacy_id is None or payment_id is None:
                continue
            try:
                pg_cur.execute(
                    """
                    INSERT INTO payment_attachments (
                        legacy_id, tenant_id, payment_id, filename, url,
                        created_at, updated_at, raw
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET payment_id = EXCLUDED.payment_id,
                        filename = EXCLUDED.filename,
                        url = EXCLUDED.url,
                        updated_at = EXCLUDED.updated_at,
                        raw = EXCLUDED.raw
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        payment_id,
                        row.get("filename"),
                        row.get("url"),
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                        _jsonb_payload(row),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "balance_payment_file", legacy_id, new_id, "SUCCESS")
                file_success += 1
            except Exception as e:
                log.error("  balance_payment_file id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "balance_payment_file", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if file_success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  balance_payment_file migrated: %s/%s", file_success, len(rows))
    finally:
        mysql_conn.close()
        pg_conn.close()


def migrate_artifacts():
    log.info("=== PHASE 10: migrate contract/invoice/drawing/bankflow artifacts ===")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    try:
        _ensure_traceability_prereq(pg_cur)
        contract_map = _load_legacy_id_map(pg_cur, "contracts")
        invoice_map = _load_legacy_id_map(pg_cur, "invoices")
        drawing_map = _load_legacy_id_map(pg_cur, "drawings")

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, investment, money, note, payType, programType, rate,
                   contract_id, invoice_id, create_time, update_time, create_by, update_by
            FROM contractdetail
            ORDER BY id
            """
        )
        rows = mysql_cur.fetchall()
        success = 0
        for row in rows:
            legacy_id = _safe_int(row.get("id"))
            if legacy_id is None:
                continue
            try:
                pg_cur.execute(
                    """
                    INSERT INTO contract_details (
                        legacy_id, tenant_id, contract_id, invoice_id,
                        investment, money, note, pay_type, program_type, rate,
                        created_at, updated_at, raw
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET contract_id = EXCLUDED.contract_id,
                        invoice_id = EXCLUDED.invoice_id,
                        investment = EXCLUDED.investment,
                        money = EXCLUDED.money,
                        note = EXCLUDED.note,
                        pay_type = EXCLUDED.pay_type,
                        program_type = EXCLUDED.program_type,
                        rate = EXCLUDED.rate,
                        updated_at = EXCLUDED.updated_at,
                        raw = EXCLUDED.raw
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        contract_map.get(_safe_int(row.get("contract_id"))),
                        invoice_map.get(_safe_int(row.get("invoice_id"))),
                        row.get("investment"),
                        row.get("money"),
                        row.get("note"),
                        row.get("payType"),
                        row.get("programType"),
                        row.get("rate"),
                        row.get("create_time") or row.get("addDate") or datetime.now(),
                        row.get("update_time") or row.get("lastDate") or datetime.now(),
                        _jsonb_payload(row),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "contractdetail", legacy_id, new_id, "SUCCESS")
                success += 1
            except Exception as e:
                log.error("  contractdetail id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "contractdetail", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  contractdetail migrated: %s/%s", success, len(rows))

        mysql_cur.execute(
            """
            SELECT contract_id, attr, name
            FROM contract_attribute
            ORDER BY contract_id, name
            """
        )
        rows = mysql_cur.fetchall()
        success = 0
        for row in rows:
            pg_contract_id = contract_map.get(_safe_int(row.get("contract_id")))
            if pg_contract_id is None:
                continue
            try:
                pg_cur.execute(
                    """
                    INSERT INTO contract_attributes (
                        tenant_id, contract_id, name, attr, created_at, updated_at, raw
                    ) VALUES (%s, %s, %s, %s, NOW(), NOW(), %s)
                    ON CONFLICT (tenant_id, contract_id, name) DO UPDATE
                    SET attr = EXCLUDED.attr,
                        updated_at = EXCLUDED.updated_at,
                        raw = EXCLUDED.raw
                    """,
                    (
                        TENANT_ID,
                        pg_contract_id,
                        row.get("name"),
                        row.get("attr"),
                        _jsonb_payload(row),
                    ),
                )
                success += 1
            except Exception as e:
                log.error(
                    "  contract_attribute contract=%s name=%s failed: %s",
                    row.get("contract_id"),
                    row.get("name"),
                    e,
                )
                pg_conn.rollback()
                pg_conn.commit()
                continue
            if success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  contract_attribute migrated: %s/%s", success, len(rows))

        attachment_specs = [
            (
                "contract_file",
                """
                SELECT id, contract_id, type, name, path, note, create_time, update_time, addDate, lastDate
                FROM contract_file
                ORDER BY id
                """,
                None,
            ),
            (
                "contract_archive_file",
                """
                SELECT id, contract_id, contract_archive_id, type, name, path, note, create_time, update_time, addDate, lastDate
                FROM contract_archive_file
                ORDER BY id
                """,
                "contract_archive_id",
            ),
            (
                "contract_seal_file",
                """
                SELECT id, contract_id, contract_extra_id, type, name, path, note, create_time, update_time
                FROM contract_seal_file
                ORDER BY id
                """,
                "contract_extra_id",
            ),
        ]

        for table_name, sql, related_key in attachment_specs:
            mysql_cur.execute(sql)
            rows = mysql_cur.fetchall()
            success = 0
            for row in rows:
                legacy_id = _safe_int(row.get("id"))
                if legacy_id is None:
                    continue
                try:
                    pg_cur.execute(
                        """
                        INSERT INTO contract_attachments (
                            legacy_id, source_table, tenant_id, contract_id, related_legacy_id,
                            attachment_type, name, path, url, note,
                            created_at, updated_at, raw
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s
                        )
                        ON CONFLICT (source_table, legacy_id) DO UPDATE
                        SET contract_id = EXCLUDED.contract_id,
                            related_legacy_id = EXCLUDED.related_legacy_id,
                            attachment_type = EXCLUDED.attachment_type,
                            name = EXCLUDED.name,
                            path = EXCLUDED.path,
                            url = EXCLUDED.url,
                            note = EXCLUDED.note,
                            updated_at = EXCLUDED.updated_at,
                            raw = EXCLUDED.raw
                        RETURNING id
                        """,
                        (
                            legacy_id,
                            table_name,
                            TENANT_ID,
                            contract_map.get(_safe_int(row.get("contract_id"))),
                            row.get(related_key) if related_key else None,
                            row.get("type"),
                            row.get("name"),
                            row.get("path"),
                            row.get("path"),
                            row.get("note"),
                            row.get("create_time") or row.get("addDate") or datetime.now(),
                            row.get("update_time") or row.get("lastDate") or datetime.now(),
                            _jsonb_payload(row),
                        ),
                    )
                    new_id = pg_cur.fetchone()[0]
                    log_migration(pg_cur, table_name, legacy_id, new_id, "SUCCESS")
                    success += 1
                except Exception as e:
                    log.error("  %s id=%s failed: %s", table_name, legacy_id, e)
                    pg_conn.rollback()
                    log_migration(pg_cur, table_name, legacy_id, None, "FAILED", str(e))
                    pg_conn.commit()
                    continue
                if success % BATCH_SIZE == 0:
                    pg_conn.commit()
            pg_conn.commit()
            log.info("  %s migrated: %s/%s", table_name, success, len(rows))

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, money, programType, invoice_id
            FROM invoice_item
            ORDER BY id
            """
        )
        rows = mysql_cur.fetchall()
        success = 0
        for row in rows:
            legacy_id = _safe_int(row.get("id"))
            if legacy_id is None:
                continue
            try:
                pg_cur.execute(
                    """
                    INSERT INTO invoice_items (
                        legacy_id, tenant_id, invoice_id, money, program_type,
                        created_at, updated_at, raw
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET invoice_id = EXCLUDED.invoice_id,
                        money = EXCLUDED.money,
                        program_type = EXCLUDED.program_type,
                        updated_at = EXCLUDED.updated_at,
                        raw = EXCLUDED.raw
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        invoice_map.get(_safe_int(row.get("invoice_id"))),
                        row.get("money"),
                        row.get("programType"),
                        row.get("addDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                        _jsonb_payload(row),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "invoice_item", legacy_id, new_id, "SUCCESS")
                success += 1
            except Exception as e:
                log.error("  invoice_item id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "invoice_item", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  invoice_item migrated: %s/%s", success, len(rows))

        drawing_specs = [
            (
                "drawing_files",
                """
                SELECT id, addDate, lastDate, approveDate, name, remarks, state, url, version, drawing_id
                FROM drawing_files
                ORDER BY id
                """,
                "name",
            ),
            (
                "drawing_result_file",
                """
                SELECT id, addDate, lastDate, filename, url, drawing_id
                FROM drawing_result_file
                ORDER BY id
                """,
                "filename",
            ),
        ]
        for table_name, sql, name_key in drawing_specs:
            mysql_cur.execute(sql)
            rows = mysql_cur.fetchall()
            success = 0
            for row in rows:
                legacy_id = _safe_int(row.get("id"))
                if legacy_id is None:
                    continue
                try:
                    pg_cur.execute(
                        """
                        INSERT INTO drawing_attachments (
                            legacy_id, source_table, tenant_id, drawing_id,
                            approve_date, name, remarks, state, url, version,
                            created_at, updated_at, raw
                        ) VALUES (
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s
                        )
                        ON CONFLICT (source_table, legacy_id) DO UPDATE
                        SET drawing_id = EXCLUDED.drawing_id,
                            approve_date = EXCLUDED.approve_date,
                            name = EXCLUDED.name,
                            remarks = EXCLUDED.remarks,
                            state = EXCLUDED.state,
                            url = EXCLUDED.url,
                            version = EXCLUDED.version,
                            updated_at = EXCLUDED.updated_at,
                            raw = EXCLUDED.raw
                        RETURNING id
                        """,
                        (
                            legacy_id,
                            table_name,
                            TENANT_ID,
                            drawing_map.get(_safe_int(row.get("drawing_id"))),
                            row.get("approveDate"),
                            row.get(name_key),
                            row.get("remarks"),
                            row.get("state"),
                            row.get("url"),
                            row.get("version"),
                            row.get("addDate") or datetime.now(),
                            row.get("lastDate") or row.get("addDate") or datetime.now(),
                            _jsonb_payload(row),
                        ),
                    )
                    new_id = pg_cur.fetchone()[0]
                    log_migration(pg_cur, table_name, legacy_id, new_id, "SUCCESS")
                    success += 1
                except Exception as e:
                    log.error("  %s id=%s failed: %s", table_name, legacy_id, e)
                    pg_conn.rollback()
                    log_migration(pg_cur, table_name, legacy_id, None, "FAILED", str(e))
                    pg_conn.commit()
                    continue
                if success % BATCH_SIZE == 0:
                    pg_conn.commit()
            pg_conn.commit()
            log.info("  %s migrated: %s/%s", table_name, success, len(rows))

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, balanceMoney, businessNo, cardNumber, creditAmount, currency,
                   debitAmount, guanlianType, note, oppositeAccount, oppositeName, transactionTime,
                   voucherNumber, voucherType, bankType_id
            FROM bankflow
            ORDER BY id
            """
        )
        rows = mysql_cur.fetchall()
        success = 0
        for row in rows:
            legacy_id = _safe_int(row.get("id"))
            if legacy_id is None:
                continue
            try:
                pg_cur.execute(
                    """
                    INSERT INTO bankflow_entries (
                        legacy_id, tenant_id, bank_type_legacy_id, balance_money, business_no, card_number,
                        credit_amount, currency, debit_amount, guanlian_type, note,
                        opposite_account, opposite_name, transaction_time, voucher_number, voucher_type,
                        created_at, updated_at, raw
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET bank_type_legacy_id = EXCLUDED.bank_type_legacy_id,
                        balance_money = EXCLUDED.balance_money,
                        business_no = EXCLUDED.business_no,
                        card_number = EXCLUDED.card_number,
                        credit_amount = EXCLUDED.credit_amount,
                        currency = EXCLUDED.currency,
                        debit_amount = EXCLUDED.debit_amount,
                        guanlian_type = EXCLUDED.guanlian_type,
                        note = EXCLUDED.note,
                        opposite_account = EXCLUDED.opposite_account,
                        opposite_name = EXCLUDED.opposite_name,
                        transaction_time = EXCLUDED.transaction_time,
                        voucher_number = EXCLUDED.voucher_number,
                        voucher_type = EXCLUDED.voucher_type,
                        updated_at = EXCLUDED.updated_at,
                        raw = EXCLUDED.raw
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        row.get("bankType_id"),
                        row.get("balanceMoney"),
                        row.get("businessNo"),
                        row.get("cardNumber"),
                        row.get("creditAmount"),
                        row.get("currency"),
                        row.get("debitAmount"),
                        row.get("guanlianType"),
                        row.get("note"),
                        row.get("oppositeAccount"),
                        row.get("oppositeName"),
                        row.get("transactionTime"),
                        row.get("voucherNumber"),
                        row.get("voucherType"),
                        row.get("addDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                        _jsonb_payload(row),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "bankflow", legacy_id, new_id, "SUCCESS")
                success += 1
            except Exception as e:
                log.error("  bankflow id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "bankflow", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  bankflow migrated: %s/%s", success, len(rows))
    finally:
        mysql_conn.close()
        pg_conn.close()


def migrate_traceability_extra():
    log.info("=== PHASE 11: migrate approval/payment/gathering traceability extras ===")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    try:
        _ensure_traceability_prereq(pg_cur)
        _ensure_traceability_extra_tables(pg_cur)
        pg_conn.commit()

        flow_map = _load_legacy_id_map(pg_cur, "approve_flows")
        balance_map = _load_legacy_id_map(pg_cur, "balances")
        gathering_map = _load_legacy_id_map(pg_cur, "gatherings")
        contract_map = _load_legacy_id_map(pg_cur, "contracts")
        invoice_map = _load_legacy_id_map(pg_cur, "invoices")
        actor_map = _load_actor_ref_map(pg_cur)

        pg_cur.execute("SELECT id, contract_id FROM balances")
        balance_contract_map = {int(i): c for i, c in pg_cur.fetchall()}

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, hierarchy, flow_id, user_id
            FROM approve_flow_approval
            ORDER BY id
            """
        )
        approval_rows = mysql_cur.fetchall()
        approval_success = 0
        for row in approval_rows:
            legacy_id = _safe_int(row.get("id"))
            legacy_flow_id = _safe_int(row.get("flow_id"))
            target_flow_id = flow_map.get(legacy_flow_id)
            seq = _safe_int(row.get("hierarchy"))
            task_id = None
            legacy_user_id = _safe_int(row.get("user_id"))
            actor_ref = _actor_ref(actor_map, legacy_user_id)

            if target_flow_id is not None and seq is not None:
                pg_cur.execute(
                    """
                    SELECT id
                    FROM approve_tasks
                    WHERE flow_id=%s AND seq=%s
                    ORDER BY CASE WHEN legacy_user_id=%s THEN 0 ELSE 1 END, id
                    LIMIT 1
                    """,
                    (target_flow_id, seq, legacy_user_id),
                )
                x = pg_cur.fetchone()
                if x:
                    task_id = x[0]

            if task_id is None and target_flow_id is not None:
                synthetic_legacy_task_id = 950000000000 + (legacy_id or 0)
                fallback_seq = seq if seq is not None and seq > 0 else 9999
                pg_cur.execute(
                    """
                    INSERT INTO approve_tasks (
                        legacy_id, flow_id, seq, legacy_user_id, approver_ref, state, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET flow_id = EXCLUDED.flow_id,
                        seq = EXCLUDED.seq,
                        legacy_user_id = EXCLUDED.legacy_user_id,
                        approver_ref = EXCLUDED.approver_ref
                    RETURNING id
                    """,
                    (
                        synthetic_legacy_task_id,
                        target_flow_id,
                        fallback_seq,
                        legacy_user_id,
                        actor_ref,
                        "WAITING",
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                    ),
                )
                task_id = pg_cur.fetchone()[0]

            try:
                pg_cur.execute(
                    """
                    INSERT INTO approve_flow_approvals (
                        legacy_id, tenant_id, flow_id, task_id, legacy_flow_id, hierarchy,
                        legacy_user_id, actor_ref, created_at, updated_at, raw
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET flow_id = EXCLUDED.flow_id,
                        task_id = EXCLUDED.task_id,
                        legacy_flow_id = EXCLUDED.legacy_flow_id,
                        hierarchy = EXCLUDED.hierarchy,
                        legacy_user_id = EXCLUDED.legacy_user_id,
                        actor_ref = EXCLUDED.actor_ref,
                        updated_at = EXCLUDED.updated_at,
                        raw = EXCLUDED.raw
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        target_flow_id,
                        task_id,
                        legacy_flow_id,
                        seq,
                        legacy_user_id,
                        actor_ref,
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                        _jsonb_payload(row),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "approve_flow_approval", legacy_id, new_id, "SUCCESS")
                approval_success += 1
            except Exception as e:
                log.error("  approve_flow_approval id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "approve_flow_approval", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if approval_success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  approve_flow_approval migrated: %s/%s", approval_success, len(approval_rows))

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, balance_id, amount, money, invoice, management,
                   fileBondMoney, fastMoney, rate, managementRate, taxExpenses,
                   bankName, bankNo, unit, balanceType, invoiceType, settlementType,
                   fastType, fileNum, bondTypeCheck
            FROM balance_invoice
            ORDER BY id
            """
        )
        bi_rows = mysql_cur.fetchall()
        bi_success = 0
        for row in bi_rows:
            legacy_id = _safe_int(row.get("id"))
            legacy_balance_id = _safe_int(row.get("balance_id"))
            balance_id = balance_map.get(legacy_balance_id)
            contract_id = balance_contract_map.get(balance_id) if balance_id is not None else None
            try:
                pg_cur.execute(
                    """
                    INSERT INTO balance_invoices (
                        legacy_id, tenant_id, balance_id, balance_legacy_id, contract_id,
                        amount, money, invoice, management, file_bond_money, fast_money,
                        management_rate, rate, tax_expenses,
                        bank_name, bank_no, unit,
                        balance_type, invoice_type, settlement_type, fast_type,
                        file_num, bond_type_check,
                        created_at, updated_at, raw
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET balance_id = EXCLUDED.balance_id,
                        balance_legacy_id = EXCLUDED.balance_legacy_id,
                        contract_id = EXCLUDED.contract_id,
                        amount = EXCLUDED.amount,
                        money = EXCLUDED.money,
                        invoice = EXCLUDED.invoice,
                        management = EXCLUDED.management,
                        file_bond_money = EXCLUDED.file_bond_money,
                        fast_money = EXCLUDED.fast_money,
                        management_rate = EXCLUDED.management_rate,
                        rate = EXCLUDED.rate,
                        tax_expenses = EXCLUDED.tax_expenses,
                        bank_name = EXCLUDED.bank_name,
                        bank_no = EXCLUDED.bank_no,
                        unit = EXCLUDED.unit,
                        balance_type = EXCLUDED.balance_type,
                        invoice_type = EXCLUDED.invoice_type,
                        settlement_type = EXCLUDED.settlement_type,
                        fast_type = EXCLUDED.fast_type,
                        file_num = EXCLUDED.file_num,
                        bond_type_check = EXCLUDED.bond_type_check,
                        updated_at = EXCLUDED.updated_at,
                        raw = EXCLUDED.raw
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        balance_id,
                        legacy_balance_id,
                        contract_id,
                        row.get("amount"),
                        row.get("money"),
                        row.get("invoice"),
                        row.get("management"),
                        row.get("fileBondMoney"),
                        row.get("fastMoney"),
                        row.get("managementRate"),
                        row.get("rate"),
                        row.get("taxExpenses"),
                        row.get("bankName"),
                        row.get("bankNo"),
                        row.get("unit"),
                        row.get("balanceType"),
                        row.get("invoiceType"),
                        row.get("settlementType"),
                        row.get("fastType"),
                        row.get("fileNum"),
                        row.get("bondTypeCheck"),
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                        _jsonb_payload(row),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "balance_invoice", legacy_id, new_id, "SUCCESS")
                bi_success += 1
            except Exception as e:
                log.error("  balance_invoice id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "balance_invoice", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if bi_success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  balance_invoice migrated: %s/%s", bi_success, len(bi_rows))

        mysql_cur.execute(
            """
            SELECT id, addDate, lastDate, gathering_id, contract_id, invoice_id,
                   relationState, money, InvoiceMoney
            FROM gathering_item
            ORDER BY id
            """
        )
        gi_rows = mysql_cur.fetchall()
        gi_success = 0
        for row in gi_rows:
            legacy_id = _safe_int(row.get("id"))
            legacy_gathering_id = _safe_int(row.get("gathering_id"))
            legacy_contract_id = _safe_int(row.get("contract_id"))
            legacy_invoice_id = _safe_int(row.get("invoice_id"))
            try:
                pg_cur.execute(
                    """
                    INSERT INTO gathering_items (
                        legacy_id, tenant_id, gathering_id, gathering_legacy_id,
                        contract_id, contract_legacy_id, invoice_id, invoice_legacy_id,
                        relation_state, money, invoice_money,
                        created_at, updated_at, raw
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (legacy_id) DO UPDATE
                    SET gathering_id = EXCLUDED.gathering_id,
                        gathering_legacy_id = EXCLUDED.gathering_legacy_id,
                        contract_id = EXCLUDED.contract_id,
                        contract_legacy_id = EXCLUDED.contract_legacy_id,
                        invoice_id = EXCLUDED.invoice_id,
                        invoice_legacy_id = EXCLUDED.invoice_legacy_id,
                        relation_state = EXCLUDED.relation_state,
                        money = EXCLUDED.money,
                        invoice_money = EXCLUDED.invoice_money,
                        updated_at = EXCLUDED.updated_at,
                        raw = EXCLUDED.raw
                    RETURNING id
                    """,
                    (
                        legacy_id,
                        TENANT_ID,
                        gathering_map.get(legacy_gathering_id),
                        legacy_gathering_id,
                        contract_map.get(legacy_contract_id),
                        legacy_contract_id,
                        invoice_map.get(legacy_invoice_id),
                        legacy_invoice_id,
                        _safe_int(row.get("relationState")),
                        row.get("money"),
                        row.get("InvoiceMoney"),
                        row.get("addDate") or row.get("lastDate") or datetime.now(),
                        row.get("lastDate") or row.get("addDate") or datetime.now(),
                        _jsonb_payload(row),
                    ),
                )
                new_id = pg_cur.fetchone()[0]
                log_migration(pg_cur, "gathering_item", legacy_id, new_id, "SUCCESS")
                gi_success += 1
            except Exception as e:
                log.error("  gathering_item id=%s failed: %s", legacy_id, e)
                pg_conn.rollback()
                log_migration(pg_cur, "gathering_item", legacy_id, None, "FAILED", str(e))
                pg_conn.commit()
                continue
            if gi_success % BATCH_SIZE == 0:
                pg_conn.commit()
        pg_conn.commit()
        log.info("  gathering_item migrated: %s/%s", gi_success, len(gi_rows))
    finally:
        mysql_conn.close()
        pg_conn.close()


def migrate_business_extra():
    log.info("=== PHASE 13: migrate business extra domain tables ===")
    pg_conn = get_pg()
    pg_cur = pg_conn.cursor()
    try:
        _ensure_traceability_prereq(pg_cur)
        _ensure_business_extra_tables(pg_cur)
        pg_conn.commit()

        pg_cur.execute(
            """
            SELECT batch_id
            FROM icrm_raw.landing_batches
            WHERE source_db=%s AND status='SUCCESS'
            ORDER BY batch_id DESC
            LIMIT 1
            """,
            (MYSQL_CONFIG["database"],),
        )
        row = pg_cur.fetchone()
        if not row:
            raise RuntimeError("no successful raw landing batch for source_db=icrm")
        batch_id = int(row[0])
        log.info("  use raw batch_id=%s", batch_id)

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='customer'
            )
            INSERT INTO customers (
                legacy_id, tenant_id, company_id, name, state, address, telephone, phone, mail,
                charger_name, charger_phone, charger_position, bank_name, bank_no, bank_account,
                deposit_bank, taxpayer_no, card_number, job, principal, extra, deleted,
                created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                COALESCE(NULLIF(s.row_data->>'tenant_id','')::int, %s),
                c.id,
                COALESCE(NULLIF(s.row_data->>'name',''), 'legacy-customer-' || s.legacy_id::text),
                NULLIF(s.row_data->>'state',''),
                NULLIF(s.row_data->>'address',''),
                NULLIF(s.row_data->>'telephone',''),
                NULLIF(s.row_data->>'phone',''),
                NULLIF(s.row_data->>'mail',''),
                NULLIF(s.row_data->>'chargerName',''),
                NULLIF(s.row_data->>'chargerPhone',''),
                NULLIF(s.row_data->>'chargerPostion',''),
                NULLIF(s.row_data->>'bankName',''),
                NULLIF(s.row_data->>'bankNo',''),
                NULLIF(s.row_data->>'bankAccount',''),
                NULLIF(s.row_data->>'depositBank',''),
                NULLIF(s.row_data->>'taxpayerNo',''),
                NULLIF(s.row_data->>'cardNumber',''),
                NULLIF(s.row_data->>'job',''),
                NULLIF(s.row_data->>'principal',''),
                NULLIF(s.row_data->>'extra',''),
                CASE WHEN COALESCE(s.row_data->>'deleted','0') IN ('1','true','TRUE') THEN TRUE ELSE FALSE END,
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN companies c ON c.legacy_id = NULLIF(s.row_data->>'company_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET tenant_id=EXCLUDED.tenant_id,
                company_id=EXCLUDED.company_id,
                name=EXCLUDED.name,
                state=EXCLUDED.state,
                address=EXCLUDED.address,
                telephone=EXCLUDED.telephone,
                phone=EXCLUDED.phone,
                mail=EXCLUDED.mail,
                charger_name=EXCLUDED.charger_name,
                charger_phone=EXCLUDED.charger_phone,
                charger_position=EXCLUDED.charger_position,
                bank_name=EXCLUDED.bank_name,
                bank_no=EXCLUDED.bank_no,
                bank_account=EXCLUDED.bank_account,
                deposit_bank=EXCLUDED.deposit_bank,
                taxpayer_no=EXCLUDED.taxpayer_no,
                card_number=EXCLUDED.card_number,
                job=EXCLUDED.job,
                principal=EXCLUDED.principal,
                extra=EXCLUDED.extra,
                deleted=EXCLUDED.deleted,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "customer", "customers")
        pg_conn.commit()
        log.info("  customer migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='record_balance'
            )
            INSERT INTO balance_records (
                legacy_id, tenant_id, balance_id, balance_legacy_id,
                money, before_money, after_money, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id, %s, b.id, NULLIF(s.row_data->>'balance_id','')::bigint,
                NULLIF(s.row_data->>'money','')::numeric,
                NULLIF(s.row_data->>'beforeMoney','')::numeric,
                NULLIF(s.row_data->>'afterMoney','')::numeric,
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN balances b ON b.legacy_id = NULLIF(s.row_data->>'balance_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET balance_id=EXCLUDED.balance_id,
                balance_legacy_id=EXCLUDED.balance_legacy_id,
                money=EXCLUDED.money,
                before_money=EXCLUDED.before_money,
                after_money=EXCLUDED.after_money,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "record_balance", "balance_records")
        pg_conn.commit()
        log.info("  record_balance migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='record_gathering'
            )
            INSERT INTO gathering_records (
                legacy_id, tenant_id, gathering_id, gathering_legacy_id,
                money, before_money, after_money, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id, %s, g.id, NULLIF(s.row_data->>'gathering_id','')::bigint,
                NULLIF(s.row_data->>'money','')::numeric,
                NULLIF(s.row_data->>'beforeMoney','')::numeric,
                NULLIF(s.row_data->>'afterMoney','')::numeric,
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN gatherings g ON g.legacy_id = NULLIF(s.row_data->>'gathering_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET gathering_id=EXCLUDED.gathering_id,
                gathering_legacy_id=EXCLUDED.gathering_legacy_id,
                money=EXCLUDED.money,
                before_money=EXCLUDED.before_money,
                after_money=EXCLUDED.after_money,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "record_gathering", "gathering_records")
        pg_conn.commit()
        log.info("  record_gathering migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='record_invoice'
            )
            INSERT INTO invoice_records (
                legacy_id, tenant_id, invoice_id, invoice_legacy_id,
                money, before_money, after_money, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id, %s, i.id, NULLIF(s.row_data->>'invoice_id','')::bigint,
                NULLIF(s.row_data->>'money','')::numeric,
                NULLIF(s.row_data->>'beforeMoney','')::numeric,
                NULLIF(s.row_data->>'afterMoney','')::numeric,
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN invoices i ON i.legacy_id = NULLIF(s.row_data->>'invoice_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET invoice_id=EXCLUDED.invoice_id,
                invoice_legacy_id=EXCLUDED.invoice_legacy_id,
                money=EXCLUDED.money,
                before_money=EXCLUDED.before_money,
                after_money=EXCLUDED.after_money,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "record_invoice", "invoice_records")
        pg_conn.commit()
        log.info("  record_invoice migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='contract_creation'
            )
            INSERT INTO contract_creations (
                legacy_id, tenant_id, company_id, employee_id, legacy_parent_id, parent_id,
                name, contract_number, contract_type, signing_type, zb_wt, state, store_state,
                leader, leader_phone, contacts, contacts_phone, size, note,
                contract_money, investment_money, sign_date, confirm_date,
                flow_id, owner_legacy_id, user_legacy_id, draft, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                c.id,
                e.id,
                NULLIF(s.row_data->>'contractCreation_id','')::bigint,
                NULL::bigint,
                NULLIF(s.row_data->>'name',''),
                NULLIF(s.row_data->>'number',''),
                NULLIF(s.row_data->>'contractType','')::int,
                NULLIF(s.row_data->>'signingType','')::int,
                NULLIF(s.row_data->>'zb_wt',''),
                NULLIF(s.row_data->>'state',''),
                NULLIF(s.row_data->>'storeState','')::int,
                NULLIF(s.row_data->>'leader',''),
                NULLIF(s.row_data->>'leaderPhone',''),
                NULLIF(s.row_data->>'contacts',''),
                NULLIF(s.row_data->>'contactsPhone',''),
                NULLIF(s.row_data->>'size',''),
                NULLIF(s.row_data->>'note',''),
                NULLIF(s.row_data->>'contractMoney','')::numeric,
                NULLIF(s.row_data->>'investmentMoney','')::numeric,
                NULLIF(s.row_data->>'signdate',''),
                NULLIF(s.row_data->>'confirmDate',''),
                NULLIF(s.row_data->>'flow_id','')::bigint,
                NULLIF(s.row_data->>'owner_id','')::bigint,
                NULLIF(s.row_data->>'user_id','')::bigint,
                CASE WHEN COALESCE(s.row_data->>'draft','0') IN ('1','true','TRUE') THEN TRUE ELSE FALSE END,
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN companies c ON c.legacy_id = NULLIF(s.row_data->>'company_id','')::bigint
            LEFT JOIN employees e ON e.legacy_id = NULLIF(s.row_data->>'employee_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET company_id=EXCLUDED.company_id,
                employee_id=EXCLUDED.employee_id,
                legacy_parent_id=EXCLUDED.legacy_parent_id,
                name=EXCLUDED.name,
                contract_number=EXCLUDED.contract_number,
                contract_type=EXCLUDED.contract_type,
                signing_type=EXCLUDED.signing_type,
                zb_wt=EXCLUDED.zb_wt,
                state=EXCLUDED.state,
                store_state=EXCLUDED.store_state,
                leader=EXCLUDED.leader,
                leader_phone=EXCLUDED.leader_phone,
                contacts=EXCLUDED.contacts,
                contacts_phone=EXCLUDED.contacts_phone,
                size=EXCLUDED.size,
                note=EXCLUDED.note,
                contract_money=EXCLUDED.contract_money,
                investment_money=EXCLUDED.investment_money,
                sign_date=EXCLUDED.sign_date,
                confirm_date=EXCLUDED.confirm_date,
                flow_id=EXCLUDED.flow_id,
                owner_legacy_id=EXCLUDED.owner_legacy_id,
                user_legacy_id=EXCLUDED.user_legacy_id,
                draft=EXCLUDED.draft,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        pg_cur.execute(
            """
            UPDATE contract_creations c
            SET parent_id = p.id
            FROM contract_creations p
            WHERE c.legacy_parent_id IS NOT NULL
              AND p.legacy_id = c.legacy_parent_id
              AND c.parent_id IS DISTINCT FROM p.id
            """
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "contract_creation", "contract_creations")
        pg_conn.commit()
        log.info("  contract_creation migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='contract_creation_file'
            )
            INSERT INTO contract_creation_attachments (
                legacy_id, tenant_id, contract_creation_id, contract_creation_legacy_id,
                filename, url, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                cc.id,
                NULLIF(s.row_data->>'contractCreation_id','')::bigint,
                NULLIF(s.row_data->>'filename',''),
                NULLIF(s.row_data->>'url',''),
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN contract_creations cc ON cc.legacy_id = NULLIF(s.row_data->>'contractCreation_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET contract_creation_id=EXCLUDED.contract_creation_id,
                contract_creation_legacy_id=EXCLUDED.contract_creation_legacy_id,
                filename=EXCLUDED.filename,
                url=EXCLUDED.url,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "contract_creation_file", "contract_creation_attachments")
        pg_conn.commit()
        log.info("  contract_creation_file migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='contract_extra'
            )
            INSERT INTO contract_extras (
                legacy_id, tenant_id, contract_id, contract_creation_id, contract_creation_legacy_id,
                state, payment_type, binding_style, sender, receiver, submitter, stamper, printer, contact,
                mailing_address, express_number, express_file, express_date, stamp_date, application_time,
                received_date, stamp_require, note, publish_num, sealed, mailed, received, plan_receiver,
                real_receiver, legacy_user_id, sender_user_id, receiver_user_id, submitter_id, stamper_user_id,
                receiver_id, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                c.id,
                cc.id,
                NULLIF(s.row_data->>'contractCreation_id','')::bigint,
                NULLIF(s.row_data->>'state',''),
                NULLIF(s.row_data->>'paymentType',''),
                NULLIF(s.row_data->>'bindingStyle',''),
                NULLIF(s.row_data->>'sender',''),
                NULLIF(s.row_data->>'receiver',''),
                NULLIF(s.row_data->>'submitter',''),
                NULLIF(s.row_data->>'stamper',''),
                NULLIF(s.row_data->>'printer',''),
                NULLIF(s.row_data->>'contact',''),
                NULLIF(s.row_data->>'mailingAddress',''),
                NULLIF(s.row_data->>'expressNumber',''),
                NULLIF(s.row_data->>'express_file',''),
                NULLIF(s.row_data->>'expressDate',''),
                NULLIF(s.row_data->>'stampDate',''),
                NULLIF(s.row_data->>'applicationTime',''),
                NULLIF(s.row_data->>'receivedDate',''),
                NULLIF(s.row_data->>'stampRequire',''),
                NULLIF(s.row_data->>'note',''),
                NULLIF(s.row_data->>'publishNum','')::int,
                NULLIF(s.row_data->>'sealed',''),
                NULLIF(s.row_data->>'mailed',''),
                NULLIF(s.row_data->>'received',''),
                NULLIF(s.row_data->>'plan_receiver',''),
                NULLIF(s.row_data->>'real_receiver',''),
                NULLIF(s.row_data->>'user_id','')::bigint,
                NULLIF(s.row_data->>'sender_user_id','')::bigint,
                NULLIF(s.row_data->>'receiver_user_id','')::bigint,
                NULLIF(s.row_data->>'submitter_id','')::bigint,
                NULLIF(s.row_data->>'stamper_user_id','')::bigint,
                NULLIF(s.row_data->>'receiver_id','')::bigint,
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN contracts c ON c.legacy_id = NULLIF(s.row_data->>'contract_id','')::bigint
            LEFT JOIN contract_creations cc ON cc.legacy_id = NULLIF(s.row_data->>'contractCreation_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET contract_id=EXCLUDED.contract_id,
                contract_creation_id=EXCLUDED.contract_creation_id,
                contract_creation_legacy_id=EXCLUDED.contract_creation_legacy_id,
                state=EXCLUDED.state,
                payment_type=EXCLUDED.payment_type,
                binding_style=EXCLUDED.binding_style,
                sender=EXCLUDED.sender,
                receiver=EXCLUDED.receiver,
                submitter=EXCLUDED.submitter,
                stamper=EXCLUDED.stamper,
                printer=EXCLUDED.printer,
                contact=EXCLUDED.contact,
                mailing_address=EXCLUDED.mailing_address,
                express_number=EXCLUDED.express_number,
                express_file=EXCLUDED.express_file,
                express_date=EXCLUDED.express_date,
                stamp_date=EXCLUDED.stamp_date,
                application_time=EXCLUDED.application_time,
                received_date=EXCLUDED.received_date,
                stamp_require=EXCLUDED.stamp_require,
                note=EXCLUDED.note,
                publish_num=EXCLUDED.publish_num,
                sealed=EXCLUDED.sealed,
                mailed=EXCLUDED.mailed,
                received=EXCLUDED.received,
                plan_receiver=EXCLUDED.plan_receiver,
                real_receiver=EXCLUDED.real_receiver,
                legacy_user_id=EXCLUDED.legacy_user_id,
                sender_user_id=EXCLUDED.sender_user_id,
                receiver_user_id=EXCLUDED.receiver_user_id,
                submitter_id=EXCLUDED.submitter_id,
                stamper_user_id=EXCLUDED.stamper_user_id,
                receiver_id=EXCLUDED.receiver_id,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "contract_extra", "contract_extras")
        pg_conn.commit()
        log.info("  contract_extra migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='contract_extra_file'
            )
            INSERT INTO contract_extra_attachments (
                legacy_id, tenant_id, contract_extra_id, contract_extra_legacy_id,
                filename, url, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                ce.id,
                NULLIF(s.row_data->>'contractExtra_id','')::bigint,
                NULLIF(s.row_data->>'filename',''),
                NULLIF(s.row_data->>'url',''),
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN contract_extras ce ON ce.legacy_id = NULLIF(s.row_data->>'contractExtra_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET contract_extra_id=EXCLUDED.contract_extra_id,
                contract_extra_legacy_id=EXCLUDED.contract_extra_legacy_id,
                filename=EXCLUDED.filename,
                url=EXCLUDED.url,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "contract_extra_file", "contract_extra_attachments")
        pg_conn.commit()
        log.info("  contract_extra_file migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='bid_assure'
            )
            INSERT INTO bid_assures (
                legacy_id, tenant_id, company_id, employee_id, approve_task_id, legacy_user_id,
                assure_number, project, purpose, state, state_back, state_return,
                pay_type, assure_type, partner_type, payee, payer, assure_payee, partner,
                other, other_phone, piao_hao, assure_fund, import_money, money_back, return_money,
                assure_fund_chinese, pay_date, import_date, money_back_date, return_pay_date, time_end,
                bank_name, bank_account, assure_bank_name, assure_bank_account,
                return_bank_name, return_bank_account, return_payee, return_payer, return_zhuanyuan,
                tou_zhuanyuan, pay_voucher, return_file, bid_file, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                c.id,
                e.id,
                at.id,
                NULLIF(s.row_data->>'user_id','')::bigint,
                NULLIF(s.row_data->>'assureNumber',''),
                NULLIF(s.row_data->>'project',''),
                NULLIF(s.row_data->>'purpose',''),
                NULLIF(s.row_data->>'state',''),
                NULLIF(s.row_data->>'stateBack',''),
                NULLIF(s.row_data->>'stateReturn',''),
                NULLIF(s.row_data->>'payType','')::int,
                NULLIF(s.row_data->>'assureType','')::int,
                NULLIF(s.row_data->>'partnerType','')::int,
                NULLIF(s.row_data->>'payee',''),
                NULLIF(s.row_data->>'payer',''),
                NULLIF(s.row_data->>'assurePayee',''),
                NULLIF(s.row_data->>'partner',''),
                NULLIF(s.row_data->>'other',''),
                NULLIF(s.row_data->>'otherPhone',''),
                NULLIF(s.row_data->>'piaoHao',''),
                NULLIF(s.row_data->>'assureFund','')::numeric,
                NULLIF(s.row_data->>'importMoney','')::numeric,
                NULLIF(s.row_data->>'moneyBack','')::numeric,
                NULLIF(s.row_data->>'returnMoney','')::numeric,
                NULLIF(s.row_data->>'assureFundChinese',''),
                NULLIF(s.row_data->>'payDate',''),
                NULLIF(s.row_data->>'importDate',''),
                NULLIF(s.row_data->>'moneyBackDate',''),
                NULLIF(s.row_data->>'returnPayDate',''),
                NULLIF(s.row_data->>'timeEnd',''),
                NULLIF(s.row_data->>'bankName',''),
                NULLIF(s.row_data->>'bankAccount',''),
                NULLIF(s.row_data->>'assureBankName',''),
                NULLIF(s.row_data->>'assureBankAccount',''),
                NULLIF(s.row_data->>'returnBankName',''),
                NULLIF(s.row_data->>'returnBankAccount',''),
                NULLIF(s.row_data->>'returnPayee',''),
                NULLIF(s.row_data->>'returnPayer',''),
                NULLIF(s.row_data->>'returnZhuanyuan',''),
                NULLIF(s.row_data->>'touZhuanyuan',''),
                NULLIF(s.row_data->>'payVoucher',''),
                NULLIF(s.row_data->>'returnFile',''),
                NULLIF(s.row_data->>'bidFile',''),
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN companies c ON c.legacy_id = NULLIF(s.row_data->>'company_id','')::bigint
            LEFT JOIN employees e ON e.legacy_id = NULLIF(s.row_data->>'employee_id','')::bigint
            LEFT JOIN approve_tasks at ON at.legacy_id = NULLIF(s.row_data->>'task_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET company_id=EXCLUDED.company_id,
                employee_id=EXCLUDED.employee_id,
                approve_task_id=EXCLUDED.approve_task_id,
                legacy_user_id=EXCLUDED.legacy_user_id,
                assure_number=EXCLUDED.assure_number,
                project=EXCLUDED.project,
                purpose=EXCLUDED.purpose,
                state=EXCLUDED.state,
                state_back=EXCLUDED.state_back,
                state_return=EXCLUDED.state_return,
                pay_type=EXCLUDED.pay_type,
                assure_type=EXCLUDED.assure_type,
                partner_type=EXCLUDED.partner_type,
                payee=EXCLUDED.payee,
                payer=EXCLUDED.payer,
                assure_payee=EXCLUDED.assure_payee,
                partner=EXCLUDED.partner,
                other=EXCLUDED.other,
                other_phone=EXCLUDED.other_phone,
                piao_hao=EXCLUDED.piao_hao,
                assure_fund=EXCLUDED.assure_fund,
                import_money=EXCLUDED.import_money,
                money_back=EXCLUDED.money_back,
                return_money=EXCLUDED.return_money,
                assure_fund_chinese=EXCLUDED.assure_fund_chinese,
                pay_date=EXCLUDED.pay_date,
                import_date=EXCLUDED.import_date,
                money_back_date=EXCLUDED.money_back_date,
                return_pay_date=EXCLUDED.return_pay_date,
                time_end=EXCLUDED.time_end,
                bank_name=EXCLUDED.bank_name,
                bank_account=EXCLUDED.bank_account,
                assure_bank_name=EXCLUDED.assure_bank_name,
                assure_bank_account=EXCLUDED.assure_bank_account,
                return_bank_name=EXCLUDED.return_bank_name,
                return_bank_account=EXCLUDED.return_bank_account,
                return_payee=EXCLUDED.return_payee,
                return_payer=EXCLUDED.return_payer,
                return_zhuanyuan=EXCLUDED.return_zhuanyuan,
                tou_zhuanyuan=EXCLUDED.tou_zhuanyuan,
                pay_voucher=EXCLUDED.pay_voucher,
                return_file=EXCLUDED.return_file,
                bid_file=EXCLUDED.bid_file,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "bid_assure", "bid_assures")
        pg_conn.commit()
        log.info("  bid_assure migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='bid_assure_flow'
            )
            INSERT INTO bid_assure_flows (
                legacy_id, tenant_id, bid_assure_id, bid_assure_legacy_id, company_id, employee_id,
                bankflow_entry_id, legacy_bankflow_id, legacy_user_id, project, note, opposite_name,
                payee, assure_payee, piao_hao, assure_fund, import_money, money_back, return_money,
                pay_date, import_date, money_back_date, return_pay_date, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                ba.id,
                NULLIF(s.row_data->>'bidAssure_id','')::bigint,
                c.id,
                e.id,
                bf.id,
                NULLIF(s.row_data->>'bankFlow_id','')::bigint,
                NULLIF(s.row_data->>'user_id','')::bigint,
                NULLIF(s.row_data->>'project',''),
                NULLIF(s.row_data->>'note',''),
                NULLIF(s.row_data->>'oppositeName',''),
                NULLIF(s.row_data->>'payee',''),
                NULLIF(s.row_data->>'assurePayee',''),
                NULLIF(s.row_data->>'piaoHao',''),
                NULLIF(s.row_data->>'assureFund','')::numeric,
                NULLIF(s.row_data->>'importMoney','')::numeric,
                NULLIF(s.row_data->>'moneyBack','')::numeric,
                NULLIF(s.row_data->>'returnMoney','')::numeric,
                NULLIF(s.row_data->>'payDate',''),
                NULLIF(s.row_data->>'importDate',''),
                NULLIF(s.row_data->>'moneyBackDate',''),
                NULLIF(s.row_data->>'returnPayDate',''),
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN bid_assures ba ON ba.legacy_id = NULLIF(s.row_data->>'bidAssure_id','')::bigint
            LEFT JOIN companies c ON c.legacy_id = NULLIF(s.row_data->>'company_id','')::bigint
            LEFT JOIN employees e ON e.legacy_id = NULLIF(s.row_data->>'employee_id','')::bigint
            LEFT JOIN bankflow_entries bf ON bf.legacy_id = NULLIF(s.row_data->>'bankFlow_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET bid_assure_id=EXCLUDED.bid_assure_id,
                bid_assure_legacy_id=EXCLUDED.bid_assure_legacy_id,
                company_id=EXCLUDED.company_id,
                employee_id=EXCLUDED.employee_id,
                bankflow_entry_id=EXCLUDED.bankflow_entry_id,
                legacy_bankflow_id=EXCLUDED.legacy_bankflow_id,
                legacy_user_id=EXCLUDED.legacy_user_id,
                project=EXCLUDED.project,
                note=EXCLUDED.note,
                opposite_name=EXCLUDED.opposite_name,
                payee=EXCLUDED.payee,
                assure_payee=EXCLUDED.assure_payee,
                piao_hao=EXCLUDED.piao_hao,
                assure_fund=EXCLUDED.assure_fund,
                import_money=EXCLUDED.import_money,
                money_back=EXCLUDED.money_back,
                return_money=EXCLUDED.return_money,
                pay_date=EXCLUDED.pay_date,
                import_date=EXCLUDED.import_date,
                money_back_date=EXCLUDED.money_back_date,
                return_pay_date=EXCLUDED.return_pay_date,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "bid_assure_flow", "bid_assure_flows")
        pg_conn.commit()
        log.info("  bid_assure_flow migrated")
    finally:
        pg_conn.close()


def migrate_business_phase2():
    log.info("=== PHASE 14: migrate business phase2 (archive/attachments/partner files) ===")
    pg_conn = get_pg()
    pg_cur = pg_conn.cursor()
    try:
        _ensure_traceability_prereq(pg_cur)
        _ensure_business_extra_tables(pg_cur)
        pg_conn.commit()

        pg_cur.execute(
            """
            SELECT batch_id
            FROM icrm_raw.landing_batches
            WHERE source_db=%s AND status='SUCCESS'
            ORDER BY batch_id DESC
            LIMIT 1
            """,
            (MYSQL_CONFIG["database"],),
        )
        row = pg_cur.fetchone()
        if not row:
            raise RuntimeError("no successful raw landing batch for source_db=icrm")
        batch_id = int(row[0])
        log.info("  use raw batch_id=%s", batch_id)

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='contract_archive'
            )
            INSERT INTO contract_archives (
                legacy_id, tenant_id, contract_id, contract_legacy_id,
                archive_date, archive_note, archive_operator,
                check_date, check_note, check_operator, signing_time,
                create_by, update_by, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                c.id,
                NULLIF(s.row_data->>'contract_id','')::bigint,
                NULLIF(s.row_data->>'archive_date','')::timestamptz,
                NULLIF(s.row_data->>'archive_note',''),
                NULLIF(s.row_data->>'archive_operator',''),
                NULLIF(s.row_data->>'check_date','')::timestamptz,
                NULLIF(s.row_data->>'check_note',''),
                NULLIF(s.row_data->>'check_operator',''),
                NULLIF(s.row_data->>'signing_time','')::timestamptz,
                NULLIF(s.row_data->>'create_by',''),
                NULLIF(s.row_data->>'update_by',''),
                COALESCE((s.row_data->>'create_time')::timestamptz, NOW()),
                COALESCE((s.row_data->>'update_time')::timestamptz, (s.row_data->>'create_time')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN contracts c ON c.legacy_id = NULLIF(s.row_data->>'contract_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET contract_id=EXCLUDED.contract_id,
                contract_legacy_id=EXCLUDED.contract_legacy_id,
                archive_date=EXCLUDED.archive_date,
                archive_note=EXCLUDED.archive_note,
                archive_operator=EXCLUDED.archive_operator,
                check_date=EXCLUDED.check_date,
                check_note=EXCLUDED.check_note,
                check_operator=EXCLUDED.check_operator,
                signing_time=EXCLUDED.signing_time,
                create_by=EXCLUDED.create_by,
                update_by=EXCLUDED.update_by,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "contract_archive", "contract_archives")
        pg_conn.commit()
        log.info("  contract_archive migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='gathering_file'
            )
            INSERT INTO gathering_attachments (
                legacy_id, tenant_id, gathering_id, gathering_legacy_id,
                filename, url, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                g.id,
                NULLIF(s.row_data->>'gathering_id','')::bigint,
                NULLIF(s.row_data->>'filename',''),
                NULLIF(s.row_data->>'url',''),
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN gatherings g ON g.legacy_id = NULLIF(s.row_data->>'gathering_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET gathering_id=EXCLUDED.gathering_id,
                gathering_legacy_id=EXCLUDED.gathering_legacy_id,
                filename=EXCLUDED.filename,
                url=EXCLUDED.url,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "gathering_file", "gathering_attachments")
        pg_conn.commit()
        log.info("  gathering_file migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='filebond'
            )
            INSERT INTO filebonds (
                legacy_id, tenant_id, company_id, employee_id, contract_id,
                balance_invoice_id, balance_invoice_legacy_id, user_legacy_id,
                state, bond_fund, bond_type, bond_number, partner_type, return_file, return_pay_date,
                created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                c.id,
                e.id,
                ct.id,
                bi.id,
                NULLIF(s.row_data->>'balanceInvoice_id','')::bigint,
                NULLIF(s.row_data->>'user_id','')::bigint,
                NULLIF(s.row_data->>'state',''),
                NULLIF(s.row_data->>'bondFund','')::numeric,
                NULLIF(s.row_data->>'bondType','')::int,
                NULLIF(s.row_data->>'bondNumber',''),
                NULLIF(s.row_data->>'partnerType','')::int,
                NULLIF(s.row_data->>'returnFile',''),
                NULLIF(s.row_data->>'returnPayDate',''),
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN companies c ON c.legacy_id = NULLIF(s.row_data->>'company_id','')::bigint
            LEFT JOIN employees e ON e.legacy_id = NULLIF(s.row_data->>'employee_id','')::bigint
            LEFT JOIN contracts ct ON ct.legacy_id = NULLIF(s.row_data->>'contract_id','')::bigint
            LEFT JOIN balance_invoices bi ON bi.legacy_id = NULLIF(s.row_data->>'balanceInvoice_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET company_id=EXCLUDED.company_id,
                employee_id=EXCLUDED.employee_id,
                contract_id=EXCLUDED.contract_id,
                balance_invoice_id=EXCLUDED.balance_invoice_id,
                balance_invoice_legacy_id=EXCLUDED.balance_invoice_legacy_id,
                user_legacy_id=EXCLUDED.user_legacy_id,
                state=EXCLUDED.state,
                bond_fund=EXCLUDED.bond_fund,
                bond_type=EXCLUDED.bond_type,
                bond_number=EXCLUDED.bond_number,
                partner_type=EXCLUDED.partner_type,
                return_file=EXCLUDED.return_file,
                return_pay_date=EXCLUDED.return_pay_date,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "filebond", "filebonds")
        pg_conn.commit()
        log.info("  filebond migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='project_fileupload'
            )
            INSERT INTO project_file_uploads (
                legacy_id, tenant_id, employee_id, user_legacy_id,
                name, note, leader, sign_date, category_legacy_id, industry_legacy_id, contract_money,
                created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                e.id,
                NULLIF(s.row_data->>'user_id','')::bigint,
                NULLIF(s.row_data->>'name',''),
                NULLIF(s.row_data->>'note',''),
                NULLIF(s.row_data->>'leader',''),
                NULLIF(s.row_data->>'signdate','')::timestamptz,
                NULLIF(s.row_data->>'category_id','')::bigint,
                NULLIF(s.row_data->>'industry_id','')::bigint,
                NULLIF(s.row_data->>'contractMoney','')::numeric,
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN employees e ON e.legacy_id = NULLIF(s.row_data->>'employee_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET employee_id=EXCLUDED.employee_id,
                user_legacy_id=EXCLUDED.user_legacy_id,
                name=EXCLUDED.name,
                note=EXCLUDED.note,
                leader=EXCLUDED.leader,
                sign_date=EXCLUDED.sign_date,
                category_legacy_id=EXCLUDED.category_legacy_id,
                industry_legacy_id=EXCLUDED.industry_legacy_id,
                contract_money=EXCLUDED.contract_money,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "project_fileupload", "project_file_uploads")
        pg_conn.commit()
        log.info("  project_fileupload migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='project_file'
            )
            INSERT INTO project_files (
                legacy_id, tenant_id, project_file_upload_id, project_file_upload_legacy_id,
                filename, url, state, project_file_type, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                pfu.id,
                NULLIF(s.row_data->>'projectFileupload_id','')::bigint,
                NULLIF(s.row_data->>'filename',''),
                NULLIF(s.row_data->>'url',''),
                NULLIF(s.row_data->>'state',''),
                NULLIF(s.row_data->>'projectFileType',''),
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN project_file_uploads pfu ON pfu.legacy_id = NULLIF(s.row_data->>'projectFileupload_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET project_file_upload_id=EXCLUDED.project_file_upload_id,
                project_file_upload_legacy_id=EXCLUDED.project_file_upload_legacy_id,
                filename=EXCLUDED.filename,
                url=EXCLUDED.url,
                state=EXCLUDED.state,
                project_file_type=EXCLUDED.project_file_type,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "project_file", "project_files")
        pg_conn.commit()
        log.info("  project_file migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='contract_cancel'
            )
            INSERT INTO contract_cancels (
                legacy_id, tenant_id, contract_id, contract_legacy_id,
                cancel_note, extra, deleted, create_by, update_by, created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                c.id,
                NULLIF(s.row_data->>'contract_id','')::bigint,
                NULLIF(s.row_data->>'cancel_note',''),
                NULLIF(s.row_data->>'extra',''),
                CASE WHEN COALESCE(s.row_data->>'deleted','0') IN ('1','true','TRUE') THEN TRUE ELSE FALSE END,
                NULLIF(s.row_data->>'create_by',''),
                NULLIF(s.row_data->>'update_by',''),
                COALESCE((s.row_data->>'create_time')::timestamptz, NOW()),
                COALESCE((s.row_data->>'update_time')::timestamptz, (s.row_data->>'create_time')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN contracts c ON c.legacy_id = NULLIF(s.row_data->>'contract_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET contract_id=EXCLUDED.contract_id,
                contract_legacy_id=EXCLUDED.contract_legacy_id,
                cancel_note=EXCLUDED.cancel_note,
                extra=EXCLUDED.extra,
                deleted=EXCLUDED.deleted,
                create_by=EXCLUDED.create_by,
                update_by=EXCLUDED.update_by,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "contract_cancel", "contract_cancels")
        pg_conn.commit()
        log.info("  contract_cancel migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='projectpartner'
            )
            INSERT INTO project_partners (
                legacy_id, tenant_id, company_id, company_legacy_id,
                name, id_card, id_card_scanning, rel_cert_scanning,
                created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                c.id,
                NULLIF(s.row_data->>'company_id','')::bigint,
                NULLIF(s.row_data->>'name',''),
                NULLIF(s.row_data->>'idCard',''),
                NULLIF(s.row_data->>'idCardScanning',''),
                NULLIF(s.row_data->>'relCertScanning',''),
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN companies c ON c.legacy_id = NULLIF(s.row_data->>'company_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET company_id=EXCLUDED.company_id,
                company_legacy_id=EXCLUDED.company_legacy_id,
                name=EXCLUDED.name,
                id_card=EXCLUDED.id_card,
                id_card_scanning=EXCLUDED.id_card_scanning,
                rel_cert_scanning=EXCLUDED.rel_cert_scanning,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "projectpartner", "project_partners")
        pg_conn.commit()
        log.info("  projectpartner migrated")

        pg_cur.execute(
            """
            WITH src AS (
                SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name='company_contract'
            )
            INSERT INTO company_contracts (
                legacy_id, tenant_id, company_id, company_legacy_id, user_legacy_id,
                name, filename, url, state, start_date, end_date, upload_time,
                created_at, updated_at, raw
            )
            SELECT
                s.legacy_id,
                %s,
                c.id,
                NULLIF(s.row_data->>'company_id','')::bigint,
                NULLIF(s.row_data->>'user_id','')::bigint,
                NULLIF(s.row_data->>'name',''),
                NULLIF(s.row_data->>'filename',''),
                NULLIF(s.row_data->>'url',''),
                NULLIF(s.row_data->>'state',''),
                NULLIF(s.row_data->>'startDate','')::timestamptz,
                NULLIF(s.row_data->>'endDate','')::timestamptz,
                NULLIF(s.row_data->>'uploadTime','')::timestamptz,
                COALESCE((s.row_data->>'addDate')::timestamptz, NOW()),
                COALESCE((s.row_data->>'lastDate')::timestamptz, (s.row_data->>'addDate')::timestamptz, NOW()),
                s.row_data
            FROM src s
            LEFT JOIN companies c ON c.legacy_id = NULLIF(s.row_data->>'company_id','')::bigint
            ON CONFLICT (legacy_id) DO UPDATE
            SET company_id=EXCLUDED.company_id,
                company_legacy_id=EXCLUDED.company_legacy_id,
                user_legacy_id=EXCLUDED.user_legacy_id,
                name=EXCLUDED.name,
                filename=EXCLUDED.filename,
                url=EXCLUDED.url,
                state=EXCLUDED.state,
                start_date=EXCLUDED.start_date,
                end_date=EXCLUDED.end_date,
                upload_time=EXCLUDED.upload_time,
                updated_at=EXCLUDED.updated_at,
                raw=EXCLUDED.raw
            """,
            (batch_id, TENANT_ID),
        )
        _sync_migration_log_from_raw(pg_cur, batch_id, "company_contract", "company_contracts")
        pg_conn.commit()
        log.info("  company_contract migrated")
    finally:
        pg_conn.close()


def migrate_business_phase3():
    log.info("=== PHASE 15: migrate business phase3 (tail business tables) ===")
    pg_conn = get_pg()
    pg_cur = pg_conn.cursor()
    try:
        _ensure_traceability_prereq(pg_cur)
        _ensure_business_phase3_tables(pg_cur)
        pg_conn.commit()

        pg_cur.execute(
            """
            SELECT batch_id
            FROM icrm_raw.landing_batches
            WHERE source_db=%s AND status='SUCCESS'
            ORDER BY batch_id DESC
            LIMIT 1
            """,
            (MYSQL_CONFIG["database"],),
        )
        row = pg_cur.fetchone()
        if not row:
            raise RuntimeError("no successful raw landing batch for source_db=icrm")
        batch_id = int(row[0])
        log.info("  use raw batch_id=%s", batch_id)

        source_target = [
            ("balance_print_record", "balance_print_records"),
            ("balance_fast_settlement", "balance_fast_settlements"),
            ("blance_fast_settlement_quota", "balance_fast_settlement_quotas"),
            ("balance_fast_settlement_quota_file", "balance_fast_settlement_quota_files"),
            ("settlement_manage_fee", "settlement_manage_fees"),
            ("settlement_manage_fee_gathering", "settlement_manage_fee_gatherings"),
            ("invoice_scrap", "invoice_scraps"),
            ("audit_gathering", "audit_gatherings"),
            ("audit_gathering_file", "audit_gathering_files"),
            ("audit_invoice", "audit_invoices"),
            ("audit_receipt_invoiced", "audit_receipt_invoiced_links"),
            ("bankinfo", "bank_infos"),
            ("company_managermoney", "company_manage_moneys"),
            ("company_year_managemoney", "company_year_manage_moneys"),
            ("company_auth_area", "company_auth_areas"),
            ("contract_no", "contract_nos"),
            ("project_report", "project_reports"),
            ("project_report_file", "project_report_files"),
            ("drawing_file_type", "drawing_file_types"),
            ("approve_flow_definition", "approve_flow_definitions_legacy"),
            ("approve_flow_definition_item", "approve_flow_definition_items_legacy"),
            ("approve_flow_by_area", "approve_flow_by_areas_legacy"),
            ("approve_flow_by_zone", "approve_flow_by_zones_legacy"),
            ("payroll", "payrolls_legacy"),
            ("balance_record", "balance_records_legacy"),
            ("worker", "workers_legacy"),
            ("log_safety_record", "log_safety_records"),
        ]

        for source_table, target_table in source_target:
            pg_cur.execute(
                f"""
                WITH src AS (
                    SELECT (row_data->>'id')::bigint AS legacy_id, row_data
                    FROM icrm_raw.landing_rows
                    WHERE batch_id=%s AND table_name=%s
                )
                INSERT INTO {target_table} (legacy_id, tenant_id, raw, created_at, updated_at)
                SELECT
                    s.legacy_id,
                    %s,
                    s.row_data,
                    COALESCE(
                        (s.row_data->>'addDate')::timestamptz,
                        (s.row_data->>'create_time')::timestamptz,
                        NOW()
                    ),
                    COALESCE(
                        (s.row_data->>'lastDate')::timestamptz,
                        (s.row_data->>'update_time')::timestamptz,
                        (s.row_data->>'addDate')::timestamptz,
                        (s.row_data->>'create_time')::timestamptz,
                        NOW()
                    )
                FROM src s
                WHERE s.legacy_id IS NOT NULL
                ON CONFLICT (legacy_id) DO UPDATE
                SET tenant_id=EXCLUDED.tenant_id,
                    raw=EXCLUDED.raw,
                    updated_at=EXCLUDED.updated_at
                """,
                (batch_id, source_table, TENANT_ID),
            )
            _sync_migration_log_from_raw(pg_cur, batch_id, source_table, target_table)
            pg_conn.commit()
            log.info("  %s migrated", source_table)
    finally:
        pg_conn.close()


def migrate_system_archive():
    log.info("=== PHASE 16: archive remaining system/log tables ===")
    pg_conn = get_pg()
    pg_cur = pg_conn.cursor()
    try:
        _ensure_legacy_source_rows_table(pg_cur)
        pg_conn.commit()

        pg_cur.execute(
            """
            SELECT batch_id
            FROM icrm_raw.landing_batches
            WHERE source_db=%s AND status='SUCCESS'
            ORDER BY batch_id DESC
            LIMIT 1
            """,
            (MYSQL_CONFIG["database"],),
        )
        row = pg_cur.fetchone()
        if not row:
            raise RuntimeError("no successful raw landing batch for source_db=icrm")
        batch_id = int(row[0])
        log.info("  use raw batch_id=%s", batch_id)

        # contract_attribute has no legacy_id column in target contract_attributes;
        # it will be archived generically below to close full-table traceability.

        pg_cur.execute(
            """
            SELECT s.table_name, s.source_row_count
            FROM icrm_raw.landing_table_stats s
            WHERE s.batch_id=%s
              AND s.source_row_count>0
              AND NOT EXISTS (
                  SELECT 1
                  FROM migration_log ml
                  WHERE ml.table_name=s.table_name
                    AND ml.status='SUCCESS'
              )
            ORDER BY s.source_row_count DESC
            """,
            (batch_id,),
        )
        remains = pg_cur.fetchall()
        if not remains:
            log.info("  no remaining source tables to archive")
            return

        log.info("  remaining source tables: %s", len(remains))
        for table_name, src_rows in remains:
            pg_cur.execute(
                """
                INSERT INTO legacy_source_rows (
                    batch_id, source_table, legacy_id, source_pk, row_hash,
                    tenant_id, created_at, updated_at, raw
                )
                SELECT
                    r.batch_id,
                    r.table_name,
                    NULLIF(r.row_data->>'id', '')::bigint,
                    r.source_pk,
                    r.row_hash,
                    %s,
                    COALESCE(
                        (r.row_data->>'addDate')::timestamptz,
                        (r.row_data->>'create_time')::timestamptz,
                        NOW()
                    ),
                    COALESCE(
                        (r.row_data->>'lastDate')::timestamptz,
                        (r.row_data->>'update_time')::timestamptz,
                        (r.row_data->>'addDate')::timestamptz,
                        (r.row_data->>'create_time')::timestamptz,
                        NOW()
                    ),
                    r.row_data
                FROM icrm_raw.landing_rows r
                WHERE r.batch_id=%s AND r.table_name=%s
                ON CONFLICT (batch_id, source_table, row_hash) DO UPDATE
                SET legacy_id=EXCLUDED.legacy_id,
                    source_pk=EXCLUDED.source_pk,
                    raw=EXCLUDED.raw,
                    updated_at=EXCLUDED.updated_at
                """,
                (TENANT_ID, batch_id, table_name),
            )

            pg_cur.execute(
                """
                INSERT INTO migration_log (table_name, legacy_id, new_id, status, error_msg)
                SELECT
                    %s,
                    COALESCE(
                        NULLIF(r.row_data->>'id', '')::bigint,
                        ABS((('x' || SUBSTR(md5(r.table_name || ':' || r.row_hash), 1, 15))::bit(60)::bigint))
                    ) AS legacy_id,
                    l.id,
                    'SUCCESS',
                    NULL
                FROM icrm_raw.landing_rows r
                JOIN legacy_source_rows l
                  ON l.batch_id=r.batch_id
                 AND l.source_table=r.table_name
                 AND l.row_hash=r.row_hash
                WHERE r.batch_id=%s
                  AND r.table_name=%s
                ON CONFLICT (table_name, legacy_id) DO UPDATE
                SET new_id=EXCLUDED.new_id,
                    status=EXCLUDED.status,
                    error_msg=EXCLUDED.error_msg,
                    migrated_at=NOW()
                """,
                (table_name, batch_id, table_name),
            )
            pg_conn.commit()
            log.info("  archived %s (%s rows)", table_name, src_rows)
    finally:
        pg_conn.close()


def migrate_supplement():
    log.info("=== PHASE 7: supplemental backfill (credentials/profile) ===")
    pg_conn = get_pg()
    pg_cur = pg_conn.cursor()
    try:
        inserted_credentials = _backfill_credentials_from_qualifications(pg_cur)
        inserted_profiles = _backfill_profiles_from_contracts(pg_cur)
        inserted_personnel = _backfill_profile_personnel(pg_cur)
        pg_conn.commit()
        log.info(
            "  supplement completed: credentials=%s, profiles=%s, profile_personnel=%s",
            inserted_credentials,
            inserted_profiles,
            inserted_personnel,
        )
    except Exception as e:
        pg_conn.rollback()
        log.error("  supplement failed: %s", e)
        raise
    finally:
        pg_conn.close()


def _backfill_credentials_from_qualifications(pg_cur):
    pg_cur.execute(
        """
        WITH qual_norm AS (
            SELECT
                q.tenant_id,
                q.holder_type::text AS holder_type,
                q.qual_type::text AS cert_type,
                q.cert_no::text AS cert_number,
                q.issued_at::date AS issued_at,
                q.valid_until::date AS expires_at,
                COALESCE(q.scope, '')::text AS scope,
                CASE q.status
                    WHEN 'VALID' THEN 'ACTIVE'
                    WHEN 'EXPIRE_SOON' THEN 'ACTIVE'
                    WHEN 'EXPIRED' THEN 'EXPIRED'
                    WHEN 'REVOKED' THEN 'REVOKED'
                    ELSE 'SUSPENDED'
                END::text AS mapped_status,
                COALESCE(
                    NULLIF(q.executor_ref, ''),
                    CASE
                        WHEN q.holder_type = 'PERSON'
                            THEN COALESCE(e.executor_ref, 'v://' || q.tenant_id || '/person/' || q.holder_id)
                        ELSE COALESCE(c.executor_ref, 'v://' || q.tenant_id || '/company/' || q.holder_id)
                    END
                )::text AS holder_ref,
                COALESCE(q.created_at, NOW()) AS created_at,
                COALESCE(q.updated_at, NOW()) AS updated_at
            FROM qualifications q
            LEFT JOIN employees e
                ON q.holder_type = 'PERSON' AND e.id = q.holder_id
            LEFT JOIN companies c
                ON q.holder_type = 'COMPANY' AND c.id = q.holder_id
            WHERE q.deleted = FALSE
        )
        INSERT INTO credentials (
            holder_ref, holder_type, cert_type, cert_number,
            issued_at, expires_at, scope, status,
            tenant_id, created_at, updated_at
        )
        SELECT
            qn.holder_ref,
            qn.holder_type,
            qn.cert_type,
            qn.cert_number,
            qn.issued_at,
            qn.expires_at,
            qn.scope,
            qn.mapped_status,
            qn.tenant_id,
            qn.created_at,
            qn.updated_at
        FROM qual_norm qn
        WHERE NOT EXISTS (
            SELECT 1
            FROM credentials c
            WHERE c.tenant_id = qn.tenant_id
              AND c.holder_ref = qn.holder_ref
              AND c.cert_type = qn.cert_type
              AND COALESCE(c.cert_number, '') = COALESCE(qn.cert_number, '')
        )
        """
    )
    return pg_cur.rowcount


def _backfill_profiles_from_contracts(pg_cur):
    pg_cur.execute(
        """
        INSERT INTO achievement_profiles (
            project_name, project_type, building_unit, location,
            start_date, end_date, our_scope,
            contract_amount, our_amount, scale_metrics,
            contract_id, project_ref, utxo_ref,
            status, company_id, source, note,
            tenant_id, created_at, updated_at
        )
        SELECT
            LEFT(COALESCE(NULLIF(c.contract_name, ''), NULLIF(c.num, ''), 'legacy-contract-' || c.id)::text, 500) AS project_name,
            'OTHER'::text AS project_type,
            LEFT(COALESCE(co.name, '')::text, 255) AS building_unit,
            LEFT(COALESCE(co.address, '')::text, 255) AS location,
            c.contract_date AS start_date,
            c.updated_at AS end_date,
            COALESCE(NULLIF(c.contract_type, ''), 'LEGACY_IMPORT')::text AS our_scope,
            COALESCE(c.contract_balance, 0)::numeric AS contract_amount,
            COALESCE(c.contract_balance, 0)::numeric AS our_amount,
            '{}'::jsonb AS scale_metrics,
            c.id AS contract_id,
            LEFT(COALESCE(c.project_ref, '')::text, 500) AS project_ref,
            NULL::text AS utxo_ref,
            CASE
                WHEN COALESCE(c.contract_balance, 0) > 0 AND c.contract_date IS NOT NULL
                    THEN 'COMPLETE'
                ELSE 'DRAFT'
            END::text AS status,
            c.company_id AS company_id,
            'MANUAL'::text AS source,
            'legacy contract backfill'::text AS note,
            c.tenant_id AS tenant_id,
            COALESCE(c.created_at, NOW()) AS created_at,
            COALESCE(c.updated_at, NOW()) AS updated_at
        FROM contracts c
        LEFT JOIN companies co ON co.id = c.company_id
        WHERE c.deleted = FALSE
          AND c.company_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM achievement_profiles p
              WHERE p.contract_id = c.id
                AND p.deleted = FALSE
          )
        """
    )
    return pg_cur.rowcount


def _backfill_profile_personnel(pg_cur):
    pg_cur.execute(
        """
        INSERT INTO achievement_profile_personnel (
            profile_id, employee_id, employee_name, executor_ref,
            role, specialty, qual_type, cert_no
        )
        SELECT
            p.id AS profile_id,
            e.id AS employee_id,
            COALESCE(e.name, 'UNKNOWN')::text AS employee_name,
            e.executor_ref,
            '妞ゅ湱娲扮拹鐔荤煑娴?::text AS role,
            e.position::text AS specialty,
            NULL::text AS qual_type,
            NULL::text AS cert_no
        FROM achievement_profiles p
        JOIN contracts c ON c.id = p.contract_id
        JOIN employees e ON e.id = c.employee_id
        WHERE p.deleted = FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM achievement_profile_personnel pp
              WHERE pp.profile_id = p.id
                AND pp.employee_id = e.id
          )
        """
    )
    return pg_cur.rowcount


def _mysql_quote_ident(name: str) -> str:
    return "`%s`" % str(name).replace("`", "``")


def _normalize_json_object(row: dict) -> dict:
    def convert(v):
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, str):
            return v.replace("\x00", "")
        if isinstance(v, bytes):
            return {
                "__type__": "bytes_base64",
                "value": base64.b64encode(v).decode("ascii"),
            }
        return v

    return {str(k): convert(v) for k, v in row.items()}


def _build_source_pk_object(row: dict, pk_columns: list) -> Optional[dict]:
    if not pk_columns:
        return None
    obj = {}
    for col in pk_columns:
        if col in row:
            obj[col] = row[col]
    if not obj:
        return None
    return _normalize_json_object(obj)


def _insert_raw_rows(pg_cur, batch_id: int, table_name: str, rows: list, pk_columns: list,
                     start_index: int = 0):
    payload = []
    hashes = []
    for idx, row in enumerate(rows):
        row_obj = _normalize_json_object(row)
        pk_obj = _build_source_pk_object(row, pk_columns)
        pk_json = json.dumps(pk_obj, sort_keys=True, ensure_ascii=False) if pk_obj is not None else ""
        row_json = json.dumps(row_obj, sort_keys=True, ensure_ascii=False)
        # Include row ordinal to avoid accidental collisions on tables without PK.
        row_hash = hashlib.sha256(
            f"{table_name}|{pk_json}|{row_json}|{start_index + idx}".encode("utf-8")
        ).hexdigest()
        payload.append((
            batch_id,
            table_name,
            psycopg2.extras.Json(pk_obj) if pk_obj is not None else None,
            row_hash,
            psycopg2.extras.Json(row_obj),
        ))
        hashes.append(row_hash)

    if not payload:
        return 0, []

    psycopg2.extras.execute_values(
        pg_cur,
        """
        INSERT INTO icrm_raw.landing_rows (
            batch_id, table_name, source_pk, row_hash, row_data
        ) VALUES %s
        ON CONFLICT (batch_id, table_name, row_hash) DO NOTHING
        """,
        payload,
        template="(%s, %s, %s::jsonb, %s, %s::jsonb)",
        page_size=max(100, min(BATCH_SIZE, 2000)),
    )
    return len(payload), hashes


def _ensure_raw_landing_schema(pg_cur):
    pg_cur.execute("CREATE SCHEMA IF NOT EXISTS icrm_raw")
    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS icrm_raw.landing_batches (
            batch_id BIGSERIAL PRIMARY KEY,
            source_db TEXT NOT NULL,
            source_host TEXT,
            source_schema TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            status TEXT NOT NULL DEFAULT 'RUNNING',
            note TEXT,
            table_count INTEGER NOT NULL DEFAULT 0,
            row_count BIGINT NOT NULL DEFAULT 0
        )
        """
    )
    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS icrm_raw.landing_table_stats (
            id BIGSERIAL PRIMARY KEY,
            batch_id BIGINT NOT NULL REFERENCES icrm_raw.landing_batches(batch_id) ON DELETE CASCADE,
            table_name TEXT NOT NULL,
            source_row_count BIGINT NOT NULL DEFAULT 0,
            landed_row_count BIGINT NOT NULL DEFAULT 0,
            checksum TEXT,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            status TEXT NOT NULL DEFAULT 'RUNNING',
            error TEXT,
            UNIQUE(batch_id, table_name)
        )
        """
    )
    pg_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS icrm_raw.landing_rows (
            batch_id BIGINT NOT NULL REFERENCES icrm_raw.landing_batches(batch_id) ON DELETE CASCADE,
            table_name TEXT NOT NULL,
            source_pk JSONB,
            row_hash TEXT NOT NULL,
            row_data JSONB NOT NULL,
            landed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(batch_id, table_name, row_hash)
        )
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_landing_rows_batch_table
        ON icrm_raw.landing_rows(batch_id, table_name)
        """
    )
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_landing_rows_table_pk
        ON icrm_raw.landing_rows(table_name, source_pk)
        """
    )


def _list_mysql_base_tables(mysql_cur):
    mysql_cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (MYSQL_CONFIG["database"],),
    )
    return [r["table_name"] for r in mysql_cur.fetchall()]


def _list_mysql_pk_columns(mysql_cur, table_name: str):
    mysql_cur.execute(
        """
        SELECT k.column_name
        FROM information_schema.table_constraints t
        JOIN information_schema.key_column_usage k
          ON k.constraint_name = t.constraint_name
         AND k.table_schema = t.table_schema
         AND k.table_name = t.table_name
        WHERE t.table_schema = %s
          AND t.table_name = %s
          AND t.constraint_type = 'PRIMARY KEY'
        ORDER BY k.ordinal_position
        """,
        (MYSQL_CONFIG["database"], table_name),
    )
    return [r["column_name"] for r in mysql_cur.fetchall()]


def _pg_quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _list_pg_base_tables(pg_cur, schema_name: str):
    pg_cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema_name,),
    )
    return [r[0] for r in pg_cur.fetchall()]


def _list_pg_pk_columns(pg_cur, schema_name: str, table_name: str):
    pg_cur.execute(
        """
        SELECT k.column_name
        FROM information_schema.table_constraints t
        JOIN information_schema.key_column_usage k
          ON k.constraint_name = t.constraint_name
         AND k.table_schema = t.table_schema
         AND k.table_name = t.table_name
        WHERE t.table_schema = %s
          AND t.table_name = %s
          AND t.constraint_type = 'PRIMARY KEY'
        ORDER BY k.ordinal_position
        """,
        (schema_name, table_name),
    )
    return [r[0] for r in pg_cur.fetchall()]


def migrate_raw_full():
    source_kind = RAW_SOURCE if RAW_SOURCE in ("mysql", "pg") else "mysql"
    log.info("=== PHASE 11: raw full landing (source=%s -> icrm_raw) ===", source_kind)

    pg_target_conn = get_pg()
    pg_target_cur = pg_target_conn.cursor()
    pg_source_conn = None
    mysql_conn = None

    batch_id = None
    success_tables = 0
    failed_tables = 0
    total_rows = 0

    try:
        _ensure_raw_landing_schema(pg_target_cur)
        pg_target_conn.commit()

        source_db = MYSQL_CONFIG["database"] if source_kind == "mysql" else PG_CONFIG["database"]
        source_host = MYSQL_CONFIG.get("host") if source_kind == "mysql" else PG_CONFIG.get("host")
        source_schema = MYSQL_CONFIG["database"] if source_kind == "mysql" else RAW_PG_SOURCE_SCHEMA

        pg_target_cur.execute(
            """
            INSERT INTO icrm_raw.landing_batches (
                source_db, source_host, source_schema, status, note
            ) VALUES (%s, %s, %s, 'RUNNING', %s)
            RETURNING batch_id
            """,
            (
                source_db,
                source_host,
                source_schema,
                f"scripts/migrate.py --phase raw_full --source={source_kind}",
            ),
        )
        batch_id = pg_target_cur.fetchone()[0]
        pg_target_conn.commit()
        log.info("  raw landing batch created: batch_id=%s", batch_id)

        if source_kind == "mysql":
            mysql_conn = get_mysql()
            source_meta_cur = mysql_conn.cursor(dictionary=True, buffered=True)
            tables = _list_mysql_base_tables(source_meta_cur)
            list_pk_cols = lambda t: _list_mysql_pk_columns(source_meta_cur, t)
            count_sql = lambda t: f"SELECT COUNT(*) AS c FROM {_mysql_quote_ident(t)}"
            select_sql = lambda t: f"SELECT * FROM {_mysql_quote_ident(t)}"
        else:
            pg_source_conn = get_pg()
            source_meta_cur = pg_source_conn.cursor()
            tables = _list_pg_base_tables(source_meta_cur, RAW_PG_SOURCE_SCHEMA)
            # Avoid self-ingest when source schema is icrm_raw.
            tables = [t for t in tables if t not in ("landing_batches", "landing_table_stats", "landing_rows")]
            list_pk_cols = lambda t: _list_pg_pk_columns(source_meta_cur, RAW_PG_SOURCE_SCHEMA, t)
            count_sql = lambda t: (
                f"SELECT COUNT(*) AS c FROM {_pg_quote_ident(RAW_PG_SOURCE_SCHEMA)}.{_pg_quote_ident(t)}"
            )
            select_sql = lambda t: (
                f"SELECT * FROM {_pg_quote_ident(RAW_PG_SOURCE_SCHEMA)}.{_pg_quote_ident(t)}"
            )

        log.info("  source base tables: %s", len(tables))

        for table_name in tables:
            table_checksum = hashlib.sha256()
            landed_row_count = 0
            source_row_count = 0
            row_offset = 0

            pg_target_cur.execute(
                """
                INSERT INTO icrm_raw.landing_table_stats (
                    batch_id, table_name, status, started_at
                ) VALUES (%s, %s, 'RUNNING', NOW())
                ON CONFLICT (batch_id, table_name) DO UPDATE
                SET status='RUNNING', started_at=NOW(), finished_at=NULL, error=NULL
                """,
                (batch_id, table_name),
            )
            pg_target_cur.execute(
                "DELETE FROM icrm_raw.landing_rows WHERE batch_id=%s AND table_name=%s",
                (batch_id, table_name),
            )
            pg_target_conn.commit()

            try:
                pk_columns = list_pk_cols(table_name)
                source_meta_cur.execute(count_sql(table_name))
                source_row_count = int(source_meta_cur.fetchone()["c"] if source_kind == "mysql" else source_meta_cur.fetchone()[0])

                pg_target_cur.execute(
                    """
                    UPDATE icrm_raw.landing_table_stats
                    SET source_row_count=%s
                    WHERE batch_id=%s AND table_name=%s
                    """,
                    (source_row_count, batch_id, table_name),
                )
                pg_target_conn.commit()

                if source_kind == "mysql":
                    source_table_cur = mysql_conn.cursor(dictionary=True, buffered=True)
                    source_table_cur.execute(select_sql(table_name))
                else:
                    source_table_cur = pg_source_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    source_table_cur.execute(select_sql(table_name))

                try:
                    while True:
                        rows = source_table_cur.fetchmany(BATCH_SIZE)
                        if not rows:
                            break
                        inserted, row_hashes = _insert_raw_rows(
                            pg_target_cur, batch_id, table_name, rows, pk_columns, start_index=row_offset
                        )
                        landed_row_count += inserted
                        row_offset += len(rows)
                        for h in row_hashes:
                            table_checksum.update(h.encode("utf-8"))
                        if landed_row_count % max(BATCH_SIZE * 10, 5000) == 0:
                            pg_target_conn.commit()
                finally:
                    source_table_cur.close()

                pg_target_cur.execute(
                    """
                    UPDATE icrm_raw.landing_table_stats
                    SET landed_row_count=%s,
                        checksum=%s,
                        status='SUCCESS',
                        finished_at=NOW(),
                        error=NULL
                    WHERE batch_id=%s AND table_name=%s
                    """,
                    (landed_row_count, table_checksum.hexdigest(), batch_id, table_name),
                )
                pg_target_conn.commit()

                success_tables += 1
                total_rows += landed_row_count
                log.info(
                    "  raw table ok: %s source=%s landed=%s",
                    table_name, source_row_count, landed_row_count
                )
            except Exception as e:
                pg_target_conn.rollback()
                failed_tables += 1
                pg_target_cur.execute(
                    """
                    UPDATE icrm_raw.landing_table_stats
                    SET landed_row_count=%s,
                        checksum=%s,
                        status='FAILED',
                        finished_at=NOW(),
                        error=%s
                    WHERE batch_id=%s AND table_name=%s
                    """,
                    (landed_row_count, table_checksum.hexdigest(), str(e), batch_id, table_name),
                )
                pg_target_conn.commit()
                log.error("  raw table failed: %s (%s)", table_name, e)

        batch_status = "SUCCESS" if failed_tables == 0 else "PARTIAL_FAILED"
        pg_target_cur.execute(
            """
            UPDATE icrm_raw.landing_batches
            SET status=%s,
                finished_at=NOW(),
                table_count=%s,
                row_count=%s,
                note=COALESCE(note, '') || %s
            WHERE batch_id=%s
            """,
            (
                batch_status,
                success_tables + failed_tables,
                total_rows,
                f"; success_tables={success_tables}; failed_tables={failed_tables}",
                batch_id,
            ),
        )
        pg_target_conn.commit()
        log.info(
            "  raw landing completed: batch_id=%s status=%s tables=%s rows=%s failed_tables=%s",
            batch_id, batch_status, success_tables + failed_tables, total_rows, failed_tables
        )
    except Exception:
        pg_target_conn.rollback()
        if batch_id is not None:
            try:
                pg_target_cur.execute(
                    """
                    UPDATE icrm_raw.landing_batches
                    SET status='FAILED', finished_at=NOW()
                    WHERE batch_id=%s
                    """,
                    (batch_id,),
                )
                pg_target_conn.commit()
            except Exception:
                pg_target_conn.rollback()
        raise
    finally:
        if mysql_conn is not None:
            mysql_conn.close()
        if pg_source_conn is not None:
            pg_source_conn.close()
        pg_target_conn.close()


def verify_raw_full_latest():
    source_kind = RAW_SOURCE if RAW_SOURCE in ("mysql", "pg") else "mysql"
    log.info("=== PHASE 12: verify latest raw_full landing (source=%s) ===", source_kind)

    pg_conn = get_pg()
    pg_cur = pg_conn.cursor()
    mysql_conn = None
    pg_source_conn = None
    source_cur = None

    try:
        pg_cur.execute(
            """
            SELECT batch_id, status, started_at, finished_at
            FROM icrm_raw.landing_batches
            WHERE note LIKE %s
            ORDER BY batch_id DESC
            LIMIT 1
            """,
            ("%raw_full%",),
        )
        latest = pg_cur.fetchone()
        if not latest:
            raise RuntimeError("no raw_full batch found in icrm_raw.landing_batches")

        batch_id = latest[0]
        log.info(
            "  latest batch_id=%s status=%s started_at=%s finished_at=%s",
            latest[0], latest[1], latest[2], latest[3]
        )

        if source_kind == "mysql":
            mysql_conn = get_mysql()
            source_cur = mysql_conn.cursor(dictionary=True, buffered=True)
            tables = _list_mysql_base_tables(source_cur)
            count_sql = lambda t: f"SELECT COUNT(*) AS c FROM {_mysql_quote_ident(t)}"
            read_count = lambda row: int(row["c"])
        else:
            pg_source_conn = get_pg()
            source_cur = pg_source_conn.cursor()
            tables = _list_pg_base_tables(source_cur, RAW_PG_SOURCE_SCHEMA)
            tables = [t for t in tables if t not in ("landing_batches", "landing_table_stats", "landing_rows")]
            count_sql = lambda t: (
                f"SELECT COUNT(*) FROM {_pg_quote_ident(RAW_PG_SOURCE_SCHEMA)}.{_pg_quote_ident(t)}"
            )
            read_count = lambda row: int(row[0])

        mismatches = []
        verified_rows = 0

        for table_name in tables:
            source_cur.execute(count_sql(table_name))
            src_count = read_count(source_cur.fetchone())

            pg_cur.execute(
                """
                SELECT COUNT(*) FROM icrm_raw.landing_rows
                WHERE batch_id=%s AND table_name=%s
                """,
                (batch_id, table_name),
            )
            landed_count = int(pg_cur.fetchone()[0])
            verified_rows += landed_count

            if src_count != landed_count:
                mismatches.append((table_name, src_count, landed_count))

        if mismatches:
            log.error("  raw verify mismatches: %s tables", len(mismatches))
            for table_name, src_count, landed_count in mismatches[:50]:
                log.error(
                    "    table=%s source=%s landed=%s",
                    table_name, src_count, landed_count
                )
            raise RuntimeError(
                f"raw_full verify failed: {len(mismatches)} table(s) mismatched in batch {batch_id}"
            )

        log.info(
            "  raw verify passed: batch_id=%s tables=%s rows=%s",
            batch_id, len(tables), verified_rows
        )
    finally:
        if source_cur is not None:
            source_cur.close()
        if mysql_conn is not None:
            mysql_conn.close()
        if pg_source_conn is not None:
            pg_source_conn.close()
        pg_conn.close()


PHASES = {
    "company":  migrate_companies,
    "employee": migrate_employees,
    "qualification": migrate_qualifications,
    "regulation": migrate_regulations,
    "contract": migrate_contracts,
    "finance":  migrate_finance,
    "drawing":  migrate_drawings,
    "approve_history": migrate_approve_history,
    "cost_payment": migrate_cost_payment,
    "artifacts": migrate_artifacts,
    "traceability_extra": migrate_traceability_extra,
    "business_extra": migrate_business_extra,
    "business_phase2": migrate_business_phase2,
    "business_phase3": migrate_business_phase3,
    "system_archive": migrate_system_archive,
    "supplement": migrate_supplement,
    "verify":   verify,
    "raw_full": migrate_raw_full,
    "verify_raw_full": verify_raw_full_latest,
}

DEFAULT_ALL_PHASES = [
    "company",
    "employee",
    "qualification",
    "regulation",
    "contract",
    "finance",
    "drawing",
    "approve_history",
    "cost_payment",
    "artifacts",
    "traceability_extra",
    "business_extra",
    "business_phase2",
    "business_phase3",
    "system_archive",
    "supplement",
    "verify",
]

def main():
    parser = argparse.ArgumentParser(description="iCRM -> CoordOS migration script")
    parser.add_argument(
        "--phase",
        required=True,
        choices=list(PHASES.keys()) + ["all"],
        help="migration phase",
    )
    args = parser.parse_args()

    log.info("start migration phase=%s", args.phase)
    start = datetime.now()

    if args.phase == "all":
        for name in DEFAULT_ALL_PHASES:
            PHASES[name]()
    else:
        PHASES[args.phase]()

    elapsed = (datetime.now() - start).total_seconds()
    log.info("migration completed in %.1fs", elapsed)


if __name__ == "__main__":
    main()
