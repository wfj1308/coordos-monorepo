import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "./common";

const SPU_REF = "v://zhongbei/spu/bridge/pile_cap_drawing@v1";
const SKILL_NAMES = [
  "cap_dimension_calc",
  "cap_rebar_calc",
  "cap_drawing_gen",
  "spu_self_validate",
];

export const PILE_CAP_SKILLS: Record<string, Skill> = createSkillRegistry(
  SPU_REF,
  SKILL_NAMES,
);
