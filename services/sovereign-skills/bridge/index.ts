import type { Skill } from "../../../packages/spu-runtime/types";

import { CONCEALED_ACCEPTANCE_SKILLS } from "./concealed_acceptance_skills";
import { CONCRETE_STRENGTH_REPORT_SKILLS } from "./concrete_strength_report_skills";
import { COORDINATE_PROOF_SKILLS } from "./coordinate_proof_skills";
import { PIER_REBAR_SKILLS } from "./pier_rebar_skills";
import { PILE_CAP_SKILLS } from "./pile_cap_skills";
import { PILE_FOUNDATION_SKILLS } from "./pile_foundation_skills";
import { PRESTRESS_RECORD_SKILLS } from "./prestress_record_skills";
import { REVIEW_CERTIFICATE_SKILLS } from "./review_certificate_skills";
import { SETTLEMENT_CERT_SKILLS } from "./settlement_cert_skills";
import { SUPERSTRUCTURE_SKILLS } from "./superstructure_skills";

export * from "./common";
export * from "./concealed_acceptance_skills";
export * from "./concrete_strength_report_skills";
export * from "./coordinate_proof_skills";
export * from "./pier_rebar_skills";
export * from "./pile_cap_skills";
export * from "./pile_foundation_skills";
export * from "./prestress_record_skills";
export * from "./review_certificate_skills";
export * from "./settlement_cert_skills";
export * from "./superstructure_skills";

// Keyed by spu_ref to avoid collisions for generic skill names such as
// `spu_self_validate`.
export const BRIDGE_SKILL_MODULES: Record<string, Record<string, Skill>> = {
  "v://zhongbei/spu/bridge/pile_foundation_drawing@v1": PILE_FOUNDATION_SKILLS,
  "v://zhongbei/spu/bridge/pile_cap_drawing@v1": PILE_CAP_SKILLS,
  "v://zhongbei/spu/bridge/pier_rebar_drawing@v1": PIER_REBAR_SKILLS,
  "v://zhongbei/spu/bridge/superstructure_drawing@v1": SUPERSTRUCTURE_SKILLS,
  "v://zhongbei/spu/bridge/concealed_acceptance@v1": CONCEALED_ACCEPTANCE_SKILLS,
  "v://zhongbei/spu/bridge/prestress_record@v1": PRESTRESS_RECORD_SKILLS,
  "v://zhongbei/spu/bridge/concrete_strength_report@v1": CONCRETE_STRENGTH_REPORT_SKILLS,
  "v://zhongbei/spu/bridge/coordinate_proof@v1": COORDINATE_PROOF_SKILLS,
  "v://zhongbei/spu/bridge/review_certificate@v1": REVIEW_CERTIFICATE_SKILLS,
  "v://zhongbei/spu/bridge/settlement_cert@v1": SETTLEMENT_CERT_SKILLS,
};

export function findBridgeSkill(spuRef: string, skillName: string): Skill | null {
  const module = BRIDGE_SKILL_MODULES[spuRef];
  if (!module) return null;
  return module[skillName] ?? null;
}
