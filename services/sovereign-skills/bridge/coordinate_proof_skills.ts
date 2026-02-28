import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "./common";

const SPU_REF = "v://zhongbei/spu/bridge/coordinate_proof@v1";
const SKILL_NAMES = [
  "control_point_check",
  "coordinate_measurement",
  "deviation_evaluation",
  "coordinate_report_gen",
];

export const COORDINATE_PROOF_SKILLS: Record<string, Skill> = createSkillRegistry(
  SPU_REF,
  SKILL_NAMES,
);
