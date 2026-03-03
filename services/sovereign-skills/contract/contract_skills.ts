import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "../bridge/common";

// ── contract/review@v1 skills ─────────────────────────────────
const CONTRACT_REVIEW_SPU = "v://zhongbei/spu/contract/review@v1";
const CONTRACT_REVIEW_SKILL_NAMES = [
  "risk_control",
  "contract_review",
  "collection_tracking",
];

export const CONTRACT_REVIEW_SKILLS: Record<string, Skill> = createSkillRegistry(
  CONTRACT_REVIEW_SPU,
  CONTRACT_REVIEW_SKILL_NAMES,
);

// ── contract/sign@v1 skills ───────────────────────────────────
const CONTRACT_SIGN_SPU = "v://zhongbei/spu/contract/sign@v1";
const CONTRACT_SIGN_SKILL_NAMES = [
  "compliance_review",
  "contract_sign",
  "trip_trigger",
];

export const CONTRACT_SIGN_SKILLS: Record<string, Skill> = createSkillRegistry(
  CONTRACT_SIGN_SPU,
  CONTRACT_SIGN_SKILL_NAMES,
);

// ── contract/archive@v1 skills ────────────────────────────────
const CONTRACT_ARCHIVE_SPU = "v://zhongbei/spu/contract/archive@v1";
const CONTRACT_ARCHIVE_SKILL_NAMES = [
  "contract_archive",
  "document_traceability",
];

export const CONTRACT_ARCHIVE_SKILLS: Record<string, Skill> = createSkillRegistry(
  CONTRACT_ARCHIVE_SPU,
  CONTRACT_ARCHIVE_SKILL_NAMES,
);
