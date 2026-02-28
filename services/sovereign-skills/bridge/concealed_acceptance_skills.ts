import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "./common";

const SPU_REF = "v://zhongbei/spu/bridge/concealed_acceptance@v1";
const SKILL_NAMES = [
  "field_measurement",
  "tolerance_check",
  "multi_party_sign",
];

export const CONCEALED_ACCEPTANCE_SKILLS: Record<string, Skill> = createSkillRegistry(
  SPU_REF,
  SKILL_NAMES,
);
