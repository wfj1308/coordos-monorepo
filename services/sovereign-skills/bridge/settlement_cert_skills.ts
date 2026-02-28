import type { Skill } from "../../../packages/spu-runtime/types";
import { createSkillRegistry } from "./common";

const SPU_REF = "v://zhongbei/spu/bridge/settlement_cert@v1";
const SKILL_NAMES = [
  "utxo_chain_verify",
  "payment_condition_check",
  "split_calculation",
  "settlement_confirm",
];

export const SETTLEMENT_CERT_SKILLS: Record<string, Skill> = createSkillRegistry(
  SPU_REF,
  SKILL_NAMES,
);
