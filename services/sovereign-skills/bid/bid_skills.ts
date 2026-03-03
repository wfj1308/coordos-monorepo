import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "../bridge/common";

// ── bid/preparation@v1 skills ─────────────────────────────────
const BID_PREPARATION_SPU = "v://zhongbei/spu/bid/preparation@v1";
const BID_PREPARATION_SKILL_NAMES = [
  "market_analysis",
  "qualification_packaging",
  "bid_document",
  "compliance_review",
];

export const BID_PREPARATION_SKILLS: Record<string, Skill> = createSkillRegistry(
  BID_PREPARATION_SPU,
  BID_PREPARATION_SKILL_NAMES,
);

// ── bid/submission@v1 skills ──────────────────────────────────
const BID_SUBMISSION_SPU = "v://zhongbei/spu/bid/submission@v1";
const BID_SUBMISSION_SKILL_NAMES = [
  "compliance_review",
  "bid_submission",
  "client_coordination",
];

export const BID_SUBMISSION_SKILLS: Record<string, Skill> = createSkillRegistry(
  BID_SUBMISSION_SPU,
  BID_SUBMISSION_SKILL_NAMES,
);

// ── bid/award@v1 skills ───────────────────────────────────────
const BID_AWARD_SPU = "v://zhongbei/spu/bid/award@v1";
const BID_AWARD_SKILL_NAMES = [
  "award_followup",
  "contract_handover",
  "project_genesis_trigger",
];

export const BID_AWARD_SKILLS: Record<string, Skill> = createSkillRegistry(
  BID_AWARD_SPU,
  BID_AWARD_SKILL_NAMES,
);
