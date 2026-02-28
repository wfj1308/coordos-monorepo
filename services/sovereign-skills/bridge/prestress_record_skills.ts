import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "./common";

const SPU_REF = "v://zhongbei/spu/bridge/prestress_record@v1";
const SKILL_NAMES = [
  "pretension_pre_check",
  "tension_data_record",
  "tension_deviation_check",
];

export const PRESTRESS_RECORD_SKILLS: Record<string, Skill> = createSkillRegistry(
  SPU_REF,
  SKILL_NAMES,
);
