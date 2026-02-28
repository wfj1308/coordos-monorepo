/**
 * SPU Runtime
 *
 * 核心逻辑：
 *   取 SPU 规格 → 校验输入 → 校验执行体 → 按 process_unit 逐步执行
 *   → 产出 UTXO → 验收 → 状态转移
 *
 * 这是连接 SPU 规格（specs/spu/）和 技能实现（sovereign-skills/）的桥梁
 */

import type { VRef, UTXO, Executor, SPU, StepResult, ExecutionContext } from './types'

// ══════════════════════════════════════════════════════════════
//  核心执行函数
// ══════════════════════════════════════════════════════════════

export async function executeSPU(
  spuRef: VRef,
  inputs: Record<string, VRef>,   // { geology_utxo: "v://...", load_utxo: "v://..." }
  executor: Executor,
  ctx: ExecutionContext
): Promise<UTXO> {

  // 1. 加载 SPU 规格
  const spu = await ctx.spuStore.get(spuRef)
  if (!spu) throw new SPUNotFoundError(spuRef)

  // 2. 校验输入资源（parameter_unit.input_resource_refs）
  const inputValidation = validateInputs(spu, inputs)
  if (!inputValidation.ok) {
    throw new InputValidationError(spuRef, inputValidation.errors)
  }

  // 3. 校验执行体能力（executor_requirement）
  const executorValidation = validateExecutor(spu, executor)
  if (!executorValidation.ok) {
    throw new ExecutorMismatchError(spuRef, executorValidation.errors)
  }

  // 4. 计算执行模式（全自动/辅助/人工主导）
  const mode = resolveExecutionMode(spu, inputs, executor)

  // 5. 按 process_unit 逐步执行
  const stepUTXOs: Record<string, VRef> = {}
  for (const step of spu.process_unit.steps) {
    const stepResult = await executeStep(step, inputs, stepUTXOs, executor, ctx)
    stepUTXOs[step.output_kind.toLowerCase()] = stepResult.utxoRef

    // 记录步骤存证
    await ctx.auditStore.record({
      event: 'SPU_STEP_COMPLETED',
      spu_ref: spuRef,
      step_seq: step.seq,
      step_name: step.name,
      output_utxo: stepResult.utxoRef,
      executor_ref: executor.ref,
      auto: step.auto_executable && mode !== 'HUMAN',
      timestamp: new Date().toISOString()
    })
  }

  // 6. 产出最终 UTXO（实例）
  const utxo = await ctx.utxoStore.create({
    spu_ref: spuRef,
    input_refs: Object.values(inputs),
    executor_ref: executor.ref,
    step_utxos: stepUTXOs,
    execution_mode: mode,
    status: 'DRAFT',
    tenant_id: ctx.tenantId,
    proof_hash: '',              // 下面计算
    created_at: new Date().toISOString()
  })

  // 7. 自动验收（acceptance_spec）
  const acceptResult = await validateAcceptance(spu, utxo, stepUTXOs, ctx)

  // 8. 状态转移（由 SPU.utxo_state_machine 驱动）
  const finalStatus = await transitionUTXOStatus(spu, utxo, acceptResult, ctx)
  utxo.status = finalStatus

  await ctx.utxoStore.update(utxo)

  return utxo
}

// ══════════════════════════════════════════════════════════════
//  执行单个步骤
// ══════════════════════════════════════════════════════════════

async function executeStep(
  step: SPU['process_unit']['steps'][0],
  baseInputs: Record<string, VRef>,
  previousStepUTXOs: Record<string, VRef>,
  executor: Executor,
  ctx: ExecutionContext
): Promise<StepResult> {

  // 解析本步骤的输入（来自 baseInputs 或上游步骤产出）
  const resolvedInputs: VRef[] = step.inputs.map(inputKey => {
    const fromBase = baseInputs[inputKey]
    const fromPrev = previousStepUTXOs[inputKey.toLowerCase()]
    if (!fromBase && !fromPrev) {
      throw new MissingStepInputError(step.name, inputKey)
    }
    return fromBase || fromPrev
  })

  // 加载技能实现
  const skill = await ctx.skillLoader.load(step.skill)

  // 判断是否可以自动执行
  const canAuto = step.auto_executable &&
    executor.type !== 'HUMAN' &&
    (step.confidence_threshold === undefined ||
     await ctx.confidenceStore.get(step.skill) >= step.confidence_threshold)

  // 执行技能
  const result = await skill.execute({
    inputs: resolvedInputs,
    executor,
    auto: canAuto,
    ctx
  })

  return {
    stepSeq: step.seq,
    utxoRef: result.utxoRef,
    auto: canAuto,
    confidence: result.confidence
  }
}

// ══════════════════════════════════════════════════════════════
//  输入校验（parameter_unit 驱动）
// ══════════════════════════════════════════════════════════════

function validateInputs(
  spu: SPU,
  inputs: Record<string, VRef>
): { ok: boolean; errors: string[] } {
  const errors: string[] = []
  const required = spu.parameter_unit.input_resource_refs

  for (const [key, spec] of Object.entries(required)) {
    if (spec.required && !inputs[key]) {
      errors.push(`缺少必要输入: ${key}（${spec.label}）`)
    }
    if (spec.condition && !inputs[key]) {
      // 条件性输入，跳过
      continue
    }
  }

  return { ok: errors.length === 0, errors }
}

// ══════════════════════════════════════════════════════════════
//  执行体校验（executor_requirement 驱动）
// ══════════════════════════════════════════════════════════════

function validateExecutor(
  spu: SPU,
  executor: Executor
): { ok: boolean; errors: string[] } {
  const errors: string[] = []
  const req = spu.executor_requirement

  // 检查 Skills
  for (const skill of req.required_skills) {
    if (!executor.skills.includes(skill)) {
      errors.push(`执行体缺少技能: ${skill}`)
    }
  }

  // AI 执行体的额外检查
  if (executor.type === 'AI') {
    const aiReq = req.ai_executable
    if (aiReq?.human_required_condition) {
      // 如果满足"必须人工"条件，AI 不能执行
      // （实际判断需要从 inputs 里取地质条件等）
    }
  }

  return { ok: errors.length === 0, errors }
}

// ══════════════════════════════════════════════════════════════
//  执行模式判断
// ══════════════════════════════════════════════════════════════

function resolveExecutionMode(
  spu: SPU,
  inputs: Record<string, VRef>,
  executor: Executor
): 'FULL_AUTO' | 'AI_ASSISTED' | 'HUMAN' {

  if (executor.type === 'HUMAN') return 'HUMAN'

  const aiReq = spu.executor_requirement.ai_executable
  if (!aiReq) return 'HUMAN'

  // 简化：根据执行体类型判断
  // 实际需要从 inputs 解析地质条件、抗震等级等
  if (executor.type === 'AI' && executor.capability_level === 'FULL_AUTO') {
    return 'FULL_AUTO'
  }

  return 'AI_ASSISTED'
}

// ══════════════════════════════════════════════════════════════
//  验收校验（acceptance_spec 驱动）
// ══════════════════════════════════════════════════════════════

async function validateAcceptance(
  spu: SPU,
  utxo: UTXO,
  stepUTXOs: Record<string, VRef>,
  ctx: ExecutionContext
): Promise<{ passed: boolean; blocking_failures: string[]; warnings: string[] }> {

  const blockingFailures: string[] = []
  const warnings: string[] = []

  // 1. 参数范围检查
  for (const check of spu.acceptance_spec.parameter_checks) {
    const passed = await ctx.ruleEvaluator.evaluate(check.check, utxo)
    if (!passed) {
      if (check.blocking) {
        blockingFailures.push(`参数校验失败 [${check.spec_ref}]: ${check.check}`)
      } else {
        warnings.push(`参数警告 [${check.spec_ref}]: ${check.check}`)
      }
    }
  }

  // 2. 完整性检查
  for (const check of spu.acceptance_spec.completeness_checks) {
    const passed = await ctx.ruleEvaluator.evaluate(check, utxo)
    if (!passed) {
      blockingFailures.push(`完整性校验失败: ${check}`)
    }
  }

  // 3. 证据检查
  for (const evidence of spu.acceptance_spec.evidence_required) {
    if (evidence.condition && !evalCondition(evidence.condition, utxo)) {
      continue  // 条件不满足，跳过
    }
    const hasEvidence = !!stepUTXOs[evidence.kind.toLowerCase()]
    if (!hasEvidence && evidence.blocking) {
      blockingFailures.push(`缺少必要证据: ${evidence.description}（${evidence.kind}）`)
    }
  }

  return {
    passed: blockingFailures.length === 0,
    blocking_failures: blockingFailures,
    warnings
  }
}

// ══════════════════════════════════════════════════════════════
//  UTXO 状态转移（utxo_state_machine 驱动）
// ══════════════════════════════════════════════════════════════

async function transitionUTXOStatus(
  spu: SPU,
  utxo: UTXO,
  acceptResult: Awaited<ReturnType<typeof validateAcceptance>>,
  ctx: ExecutionContext
): Promise<string> {

  let status = 'DRAFT'

  // DRAFT → REVIEWED：所有证据齐备
  if (acceptResult.passed) {
    status = 'REVIEWED'
  }

  // REVIEWED → APPROVED：所有验收条件通过
  if (status === 'REVIEWED' && acceptResult.blocking_failures.length === 0) {
    status = 'APPROVED'
  }

  // 记录验收结果
  await ctx.auditStore.record({
    event: 'SPU_ACCEPTANCE_RESULT',
    spu_ref: spu.spu_ref,
    utxo_ref: utxo.ref,
    final_status: status,
    passed: acceptResult.passed,
    blocking_failures: acceptResult.blocking_failures,
    warnings: acceptResult.warnings,
    timestamp: new Date().toISOString()
  })

  return status
}

// ══════════════════════════════════════════════════════════════
//  置信度查询（历史 UTXO 统计）
// ══════════════════════════════════════════════════════════════

export async function getConfidence(
  spuRef: VRef,
  skillName: string,
  ctx: ExecutionContext
): Promise<number> {
  // 查询历史：同 SPU + 同 skill 的 APPROVED 比例
  const history = await ctx.utxoStore.query({
    spu_ref: spuRef,
    step_skill: skillName,
    limit: 1000
  })

  if (history.length < 10) return 0  // 样本不足，不自动执行

  const approved = history.filter(u => u.status === 'APPROVED').length
  return approved / history.length
}

// ══════════════════════════════════════════════════════════════
//  工具函数
// ══════════════════════════════════════════════════════════════

function evalCondition(condition: string, utxo: UTXO): boolean {
  // 简化实现：实际需要表达式引擎
  return true
}

// ── 错误类型 ─────────────────────────────────────────────────

export class SPUNotFoundError extends Error {
  constructor(ref: VRef) { super(`SPU not found: ${ref}`) }
}

export class InputValidationError extends Error {
  constructor(spuRef: VRef, errors: string[]) {
    super(`Input validation failed for ${spuRef}:\n${errors.join('\n')}`)
  }
}

export class ExecutorMismatchError extends Error {
  constructor(spuRef: VRef, errors: string[]) {
    super(`Executor mismatch for ${spuRef}:\n${errors.join('\n')}`)
  }
}

export class MissingStepInputError extends Error {
  constructor(step: string, input: string) {
    super(`Step "${step}" missing input: ${input}`)
  }
}
