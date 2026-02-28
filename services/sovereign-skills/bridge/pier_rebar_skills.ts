import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "./common";

const SPU_REF = "v://zhongbei/spu/bridge/pier_rebar_drawing@v1";
const SKILL_NAMES = [
  "pier_section_design",
  "pier_rebar_calc",
  "pier_drawing_gen",
  "spu_self_validate",
];

export const PIER_REBAR_SKILLS: Record<string, Skill> = createSkillRegistry(
  SPU_REF,
  SKILL_NAMES,
);
