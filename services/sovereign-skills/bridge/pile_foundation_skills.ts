import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "./common";

const SPU_REF = "v://zhongbei/spu/bridge/pile_foundation_drawing@v1";
const SKILL_NAMES = [
  "geology_review",
  "pile_dimension_calc",
  "pile_rebar_calc",
  "pile_drawing_gen",
  "spu_self_validate",
];

export const PILE_FOUNDATION_SKILLS: Record<string, Skill> = createSkillRegistry(
  SPU_REF,
  SKILL_NAMES,
);
