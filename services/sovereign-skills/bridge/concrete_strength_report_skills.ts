import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "./common";

const SPU_REF = "v://zhongbei/spu/bridge/concrete_strength_report@v1";
const SKILL_NAMES = [
  "specimen_creation_record",
  "strength_test_record",
  "strength_evaluation",
];

export const CONCRETE_STRENGTH_REPORT_SKILLS: Record<string, Skill> = createSkillRegistry(
  SPU_REF,
  SKILL_NAMES,
);
