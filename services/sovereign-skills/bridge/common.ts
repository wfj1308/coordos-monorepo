import type { ExecutionContext, Executor, Skill, UTXO, VRef } from "../../../packages/spu-runtime/types";

type PayloadFactory = (params: {
  skillName: string;
  inputs: VRef[];
  executor: Executor;
  auto: boolean;
  ctx: ExecutionContext;
}) => Record<string, unknown>;

export function createStepSkill(
  spuRef: VRef,
  skillName: string,
  baseConfidence = 0.9,
  payloadFactory?: PayloadFactory,
): Skill {
  return {
    name: skillName,
    async execute({ inputs, executor, auto, ctx }) {
      const payload = payloadFactory
        ? payloadFactory({ skillName, inputs, executor, auto, ctx })
        : defaultPayload(skillName, inputs, executor, auto);

      const utxo = await ctx.utxoStore.create({
        spu_ref: spuRef,
        input_refs: inputs,
        executor_ref: executor.ref,
        step_utxos: {},
        execution_mode: resolveMode(executor, auto),
        status: "DRAFT",
        tenant_id: ctx.tenantId,
        proof_hash: "",
        created_at: new Date().toISOString(),
        payload,
      });

      return {
        utxoRef: utxo.ref,
        confidence: auto ? Math.max(baseConfidence, 0.9) : Math.min(baseConfidence, 0.88),
      };
    },
  };
}

export function createSkillRegistry(
  spuRef: VRef,
  skillNames: string[],
): Record<string, Skill> {
  const out: Record<string, Skill> = {};
  for (const skillName of skillNames) {
    out[skillName] = createStepSkill(spuRef, skillName);
  }
  return out;
}

function resolveMode(
  executor: Executor,
  auto: boolean,
): UTXO["execution_mode"] {
  if (auto) return "FULL_AUTO";
  if (executor.type === "HUMAN") return "HUMAN";
  return "AI_ASSISTED";
}

function defaultPayload(
  skillName: string,
  inputs: VRef[],
  executor: Executor,
  auto: boolean,
): Record<string, unknown> {
  return {
    skill: skillName,
    auto,
    input_refs: inputs,
    executor_ref: executor.ref,
    produced_at: new Date().toISOString(),
  };
}
