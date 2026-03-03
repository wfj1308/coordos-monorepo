import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "../bridge/common";

// ── finance/invoice@v1 skills ─────────────────────────────────
const FINANCE_INVOICE_SPU = "v://zhongbei/spu/finance/invoice@v1";
const FINANCE_INVOICE_SKILL_NAMES = [
  "tax_compliance",
  "invoice_issue",
  "financial_reconciliation",
];

export const FINANCE_INVOICE_SKILLS: Record<string, Skill> = createSkillRegistry(
  FINANCE_INVOICE_SPU,
  FINANCE_INVOICE_SKILL_NAMES,
);

// ── finance/collection@v1 skills ──────────────────────────────
const FINANCE_COLLECTION_SPU = "v://zhongbei/spu/finance/collection@v1";
const FINANCE_COLLECTION_SKILL_NAMES = [
  "financial_reconciliation",
  "collection_record",
  "settlement_split_trigger",
];

export const FINANCE_COLLECTION_SKILLS: Record<string, Skill> = createSkillRegistry(
  FINANCE_COLLECTION_SPU,
  FINANCE_COLLECTION_SKILL_NAMES,
);

// ── finance/tax_return@v1 skills ──────────────────────────────
const FINANCE_TAX_RETURN_SPU = "v://zhongbei/spu/finance/tax_return@v1";
const FINANCE_TAX_RETURN_SKILL_NAMES = [
  "tax_compliance",
  "tax_filing",
  "tax_payment_confirm",
];

export const FINANCE_TAX_RETURN_SKILLS: Record<string, Skill> = createSkillRegistry(
  FINANCE_TAX_RETURN_SPU,
  FINANCE_TAX_RETURN_SKILL_NAMES,
);
