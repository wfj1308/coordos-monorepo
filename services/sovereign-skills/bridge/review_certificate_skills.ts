import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "./common";

const SPU_REF = "v://zhongbei/spu/bridge/review_certificate@v1";
const SKILL_NAMES = [
  "drawing_completeness_check",
  "technical_review",
  "comment_resolution_check",
  "cert_issuance",
];

export const REVIEW_CERTIFICATE_SKILLS: Record<string, Skill> = createSkillRegistry(
  SPU_REF,
  SKILL_NAMES,
);
