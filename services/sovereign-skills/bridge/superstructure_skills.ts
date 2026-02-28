import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "./common";

const SPU_REF = "v://zhongbei/spu/bridge/superstructure_drawing@v1";
const SKILL_NAMES = [
  "prestress_system_design",
  "girder_section_design",
  "girder_rebar_design",
  "superstructure_drawing_gen",
  "spu_self_validate",
];

export const SUPERSTRUCTURE_SKILLS: Record<string, Skill> = createSkillRegistry(
  SPU_REF,
  SKILL_NAMES,
);
