import type { Skill } from "../../packages/spu-runtime/types";

// ── Bridge domain (design chain) ──────────────────────────────
import { BRIDGE_SKILL_MODULES, findBridgeSkill } from "./bridge/index";

// ── Bid domain (bidding chain) ────────────────────────────────
import {
  BID_PREPARATION_SKILLS,
  BID_SUBMISSION_SKILLS,
  BID_AWARD_SKILLS,
} from "./bid/bid_skills";

// ── Contract domain (contract chain) ─────────────────────────
import {
  CONTRACT_REVIEW_SKILLS,
  CONTRACT_SIGN_SKILLS,
  CONTRACT_ARCHIVE_SKILLS,
} from "./contract/contract_skills";

// ── Finance domain (finance chain) ────────────────────────────
import {
  FINANCE_INVOICE_SKILLS,
  FINANCE_COLLECTION_SKILLS,
  FINANCE_TAX_RETURN_SKILLS,
} from "./finance/finance_skills";

export * from "./bridge/index";
export * from "./bid/bid_skills";
export * from "./contract/contract_skills";
export * from "./finance/finance_skills";

/**
 * Global SPU skill registry keyed by spu_ref.
 * Covers all three company trip chains:
 *   BIDDING chain:  bid/preparation → bid/submission → bid/award
 *   DESIGN chain:   bridge/* (10 SPUs)
 *   FINANCE chain:  finance/invoice → finance/collection → finance/tax_return
 * Plus the CONTRACT chain that bridges BIDDING → DESIGN/FINANCE:
 *   contract/review → contract/sign → contract/archive
 */
export const ALL_SKILL_MODULES: Record<string, Record<string, Skill>> = {
  // Bidding chain
  "v://zhongbei/spu/bid/preparation@v1": BID_PREPARATION_SKILLS,
  "v://zhongbei/spu/bid/submission@v1": BID_SUBMISSION_SKILLS,
  "v://zhongbei/spu/bid/award@v1": BID_AWARD_SKILLS,

  // Contract chain
  "v://zhongbei/spu/contract/review@v1": CONTRACT_REVIEW_SKILLS,
  "v://zhongbei/spu/contract/sign@v1": CONTRACT_SIGN_SKILLS,
  "v://zhongbei/spu/contract/archive@v1": CONTRACT_ARCHIVE_SKILLS,

  // Design chain (bridge)
  ...BRIDGE_SKILL_MODULES,

  // Finance chain
  "v://zhongbei/spu/finance/invoice@v1": FINANCE_INVOICE_SKILLS,
  "v://zhongbei/spu/finance/collection@v1": FINANCE_COLLECTION_SKILLS,
  "v://zhongbei/spu/finance/tax_return@v1": FINANCE_TAX_RETURN_SKILLS,
};

/**
 * Resolve a skill by spu_ref + skill name across all registered domains.
 */
export function findSkill(spuRef: string, skillName: string): Skill | null {
  const module = ALL_SKILL_MODULES[spuRef];
  if (!module) return null;
  return module[skillName] ?? null;
}

// Re-export findBridgeSkill for backward compatibility.
export { findBridgeSkill };
