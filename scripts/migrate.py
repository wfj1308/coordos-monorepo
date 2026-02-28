#!/usr/bin/env python3
"""
iCRM MySQL 5.7 → CoordOS PostgreSQL 14 迁移脚本
双写过渡模式：存量数据一次性导入，增量靠应用层双写

用法：
  python3 migrate.py --phase schema    # 建表
  python3 migrate.py --phase company   # 先迁分公司（被所有表依赖）
  python3 migrate.py --phase employee  # 迁员工
  python3 migrate.py --phase contract  # 迁合同（含委托链重建）
  python3 migrate.py --phase finance   # 迁收款/结算/发票
  python3 migrate.py --phase drawing   # 迁图纸
  python3 migrate.py --phase verify    # 校验数据完整性
  python3 migrate.py --phase all       # 全部执行
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Optional

import mysql.connector
import psycopg2
import psycopg2.extras

# ── 配置 ──────────────────────────────────────────────────────
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
log = logging.getLogger(__name__)


# ── 连接 ──────────────────────────────────────────────────────
def get_mysql():
    return mysql.connector.connect(**MYSQL_CONFIG)

def get_pg():
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    return conn


# ── 迁移日志 ──────────────────────────────────────────────────
def log_migration(pg_cur, table: str, legacy_id: int, new_id: Optional[int],
                  status: str, error: str = None):
    pg_cur.execute("""
        INSERT INTO migration_log (table_name, legacy_id, new_id, status, error_msg)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (table_name, legacy_id) DO UPDATE
        SET status=EXCLUDED.status, new_id=EXCLUDED.new_id,
            error_msg=EXCLUDED.error_msg, migrated_at=NOW()
    """, (table, legacy_id, new_id, status, error))


# ════════════════════════════════════════════════════════════════
#  PHASE 1: 分公司迁移（所有表的基础）
# ════════════════════════════════════════════════════════════════
def migrate_companies():
    log.info("=== PHASE 1: 迁移分公司 (company → companies) ===")
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
    log.info(f"  读取 {len(rows)} 条分公司记录")

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
            log.error(f"  company id={row['id']} 失败: {e}")
            log_migration(pg_cur, "company", row["id"], None, "FAILED", str(e))

    # 第二步：填充 parent_id（company 有自引用）
    mysql_cur.execute("SELECT id, company_id FROM company WHERE company_id IS NOT NULL AND deleted=0")
    for row in mysql_cur.fetchall():
        pg_cur.execute("""
            UPDATE companies c
            SET parent_id = (SELECT id FROM companies WHERE legacy_id = %s)
            WHERE legacy_id = %s
        """, (row["company_id"], row["id"]))

    pg_conn.commit()
    log.info(f"  ✓ 分公司迁移完成: {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


# ════════════════════════════════════════════════════════════════
#  PHASE 2: 员工迁移
# ════════════════════════════════════════════════════════════════
def migrate_employees():
    log.info("=== PHASE 2: 迁移员工 (employee → employees) ===")
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
    log.info(f"  读取 {len(rows)} 条员工记录")

    success = 0
    for row in rows:
        try:
            # 查找对应的 PG company id
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
            log.error(f"  employee id={row['id']} 失败: {e}")
            log_migration(pg_cur, "employee", row["id"], None, "FAILED", str(e))

    pg_conn.commit()
    log.info(f"  ✓ 员工迁移完成: {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


# ════════════════════════════════════════════════════════════════
#  PHASE 3: 合同迁移（最复杂，含委托链重建）
# ════════════════════════════════════════════════════════════════
def migrate_contracts():
    log.info("=== PHASE 3: 迁移合同 (contract → contracts) ===")
    mysql_conn = get_mysql()
    pg_conn = get_pg()
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    # 注意：parent 字段是委托链，必须先插入父合同再插子合同
    # 用拓扑排序处理
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
    log.info(f"  读取 {len(rows)} 条合同记录（含委托链）")

    # 拓扑排序：没有 parent 的先插
    rows_dict = {r["id"]: r for r in rows}
    ordered = _topo_sort_contracts(rows)
    log.info(f"  拓扑排序完成，共 {len(ordered)} 条")

    success = 0
    for row in ordered:
        try:
            # 查 company
            pg_cur.execute("SELECT id FROM companies WHERE legacy_id=%s",
                           (row["company_id"],))
            r = pg_cur.fetchone()
            pg_company_id = r[0] if r else None

            # 查 employee
            pg_cur.execute("SELECT id FROM employees WHERE legacy_id=%s",
                           (row["employee_id"],))
            r = pg_cur.fetchone()
            pg_employee_id = r[0] if r else None

            # 查父合同（已在前面插入）
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
            log.error(f"  contract id={row['id']} 失败: {e}")
            pg_conn.rollback()
            log_migration(pg_cur, "contract", row["id"], None, "FAILED", str(e))
            pg_conn.commit()
            continue

        if success % BATCH_SIZE == 0:
            pg_conn.commit()
            log.info(f"    已提交 {success} 条...")

    pg_conn.commit()
    log.info(f"  ✓ 合同迁移完成: {success}/{len(ordered)}")

    # 委托链深度统计
    pg_cur.execute("""
        WITH RECURSIVE chain AS (
            SELECT id, parent_id, 1 AS depth FROM contracts WHERE parent_id IS NULL
            UNION ALL
            SELECT c.id, c.parent_id, ch.depth+1
            FROM contracts c JOIN chain ch ON c.parent_id = ch.id
        )
        SELECT depth, COUNT(*) FROM chain GROUP BY depth ORDER BY depth
    """)
    log.info("  委托链深度分布:")
    for r in pg_cur.fetchall():
        log.info(f"    深度 {r[0]}: {r[1]} 条合同")

    mysql_conn.close(); pg_conn.close()


def _topo_sort_contracts(rows):
    """拓扑排序：父合同在前，子合同在后"""
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


# ════════════════════════════════════════════════════════════════
#  PHASE 4: 财务数据迁移（收款/结算/发票）
# ════════════════════════════════════════════════════════════════
def migrate_finance():
    log.info("=== PHASE 4: 迁移财务数据 ===")
    _migrate_gatherings()
    _migrate_balances()
    _migrate_invoices()


def _migrate_gatherings():
    log.info("  4a. 收款单 (gathering → gatherings)")
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
    log.info(f"    ✓ 收款单: {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


def _migrate_balances():
    log.info("  4b. 结算单 (balance → balances)")
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
    log.info(f"    ✓ 结算单: {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


def _migrate_invoices():
    log.info("  4c. 发票 (invoice → invoices)")
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
    log.info(f"    ✓ 发票: {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


# ════════════════════════════════════════════════════════════════
#  PHASE 5: 图纸迁移
# ════════════════════════════════════════════════════════════════
def migrate_drawings():
    log.info("=== PHASE 5: 迁移图纸 (drawing → drawings) ===")
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
    log.info(f"  ✓ 图纸: {success}/{len(rows)}")
    mysql_conn.close(); pg_conn.close()


# ════════════════════════════════════════════════════════════════
#  PHASE 6: 校验
# ════════════════════════════════════════════════════════════════
def verify():
    log.info("=== PHASE 6: 数据完整性校验 ===")
    pg_conn = get_pg()
    pg_cur = pg_conn.cursor()

    checks = [
        ("companies",  "SELECT COUNT(*) FROM companies"),
        ("employees",  "SELECT COUNT(*) FROM employees"),
        ("contracts",  "SELECT COUNT(*) FROM contracts"),
        ("gatherings", "SELECT COUNT(*) FROM gatherings"),
        ("balances",   "SELECT COUNT(*) FROM balances"),
        ("invoices",   "SELECT COUNT(*) FROM invoices"),
        ("drawings",   "SELECT COUNT(*) FROM drawings"),
    ]
    for name, sql in checks:
        pg_cur.execute(sql); count = pg_cur.fetchone()[0]
        log.info(f"  {name}: {count} 条")

    # 失败记录
    pg_cur.execute("""
        SELECT table_name, COUNT(*) as failed
        FROM migration_log WHERE status='FAILED'
        GROUP BY table_name
    """)
    failed = pg_cur.fetchall()
    if failed:
        log.warning("  ⚠ 迁移失败记录:")
        for r in failed: log.warning(f"    {r[0]}: {r[1]} 条失败")
    else:
        log.info("  ✓ 无失败记录")

    # 委托链完整性
    pg_cur.execute("""
        SELECT COUNT(*) FROM contracts
        WHERE parent_id IS NOT NULL
          AND parent_id NOT IN (SELECT id FROM contracts)
    """)
    orphans = pg_cur.fetchone()[0]
    if orphans:
        log.warning(f"  ⚠ 孤儿合同（父合同未迁移）: {orphans} 条")
    else:
        log.info("  ✓ 委托链完整")

    # 金额一致性抽样
    pg_cur.execute("""
        SELECT SUM(contract_balance) FROM contracts
        WHERE migrate_status='LEGACY' AND deleted=FALSE
    """)
    total = pg_cur.fetchone()[0]
    log.info(f"  合同总金额: ¥{total:,.2f}" if total else "  合同总金额: N/A")

    pg_conn.close()


# ════════════════════════════════════════════════════════════════
#  主入口
# ════════════════════════════════════════════════════════════════
PHASES = {
    "company":  migrate_companies,
    "employee": migrate_employees,
    "contract": migrate_contracts,
    "finance":  migrate_finance,
    "drawing":  migrate_drawings,
    "verify":   verify,
}

def main():
    parser = argparse.ArgumentParser(description="iCRM → CoordOS 迁移脚本")
    parser.add_argument("--phase", required=True,
                        choices=list(PHASES.keys()) + ["all"],
                        help="迁移阶段")
    args = parser.parse_args()

    log.info(f"开始迁移: phase={args.phase}")
    start = datetime.now()

    if args.phase == "all":
        for name, fn in PHASES.items():
            fn()
    else:
        PHASES[args.phase]()

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"迁移完成，耗时 {elapsed:.1f}s")


if __name__ == "__main__":
    main()
