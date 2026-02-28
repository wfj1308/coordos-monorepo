// CoordOS SPU Runtime 类型定义

export type VRef = string   // v://{tenant}/{kind}/{path}

// ── SPU 规格（从 JSON 文件加载） ──────────────────────────────

export interface SPU {
  spu_ref: VRef
  name: string
  version: string
  domain: string
  kind: 'DRAWING' | 'REPORT' | 'CERT' | 'DATA' | 'RECORD'

  product_unit: {
    deliverables: Array<{
      name: string
      format: string[]
      required: boolean
      completeness_check: string[]
    }>
  }

  parameter_unit: {
    input_parameters: Record<string, ParameterSpec>
    derived_parameters: Record<string, DerivedParamSpec>
    input_resource_refs: Record<string, ResourceRefSpec>
  }

  process_unit: {
    total_steps: number
    steps: Array<{
      seq: number
      name: string
      skill: string
      inputs: string[]
      output_kind: string
      output_fields: string[]
      executor_skill_required: string
      auto_executable: boolean
      confidence_threshold?: number
      spec_ref?: string
      note?: string
    }>
  }

  executor_requirement: {
    capability_level: string
    min_experience_years?: number
    required_skills: string[]
    qualification?: string
    energy: {
      kind: 'LABOR' | 'COMPUTE' | 'CAPITAL'
      base_days: number
      range_days: [number, number]
    }
    ai_executable?: {
      full_auto_condition: string
      assisted_condition: string
      human_required_condition: string
    }
  }

  acceptance_spec: {
    parameter_checks: Array<{
      check: string
      spec_ref: string
      blocking: boolean
    }>
    completeness_checks: string[]
    evidence_required: Array<{
      kind: string
      description: string
      blocking: boolean
      condition?: string
    }>
  }

  utxo_state_machine: {
    states: string[]
    transitions: Array<{
      from: string
      to: string
      condition: string
      auto: boolean
      note?: string
    }>
  }

  downstream_spu_refs?: VRef[]

  ai_training_spec?: {
    input_features: string[]
    output_targets: string[]
    label_source: string
    confidence_calc: string
    min_samples_for_auto: number
  }
}

// ── UTXO 实例 ─────────────────────────────────────────────────

export interface UTXO {
  ref: VRef
  spu_ref: VRef
  input_refs: VRef[]
  executor_ref: VRef
  step_utxos: Record<string, VRef>
  execution_mode: 'FULL_AUTO' | 'AI_ASSISTED' | 'HUMAN'
  status: 'DRAFT' | 'REVIEWED' | 'APPROVED' | 'SEALED' | 'DELIVERABLE'
  tenant_id: string
  proof_hash: string
  prev_hash?: string
  created_at: string
  payload?: Record<string, unknown>
}

// ── 执行体 ────────────────────────────────────────────────────

export interface Executor {
  ref: VRef
  type: 'HUMAN' | 'AI' | 'PROGRAM' | 'INSTITUTION'
  capability_level: string
  skills: string[]
  energy_profile: {
    kind: string
    rate: number
    unit: string
  }
}

// ── 技能（执行逻辑的实现） ────────────────────────────────────

export interface Skill {
  name: string
  execute(params: {
    inputs: VRef[]
    executor: Executor
    auto: boolean
    ctx: ExecutionContext
  }): Promise<{ utxoRef: VRef; confidence: number }>
}

// ── 执行上下文（依赖注入） ────────────────────────────────────

export interface ExecutionContext {
  tenantId: string
  spuStore: {
    get(ref: VRef): Promise<SPU | null>
    list(domain?: string): Promise<SPU[]>
  }
  utxoStore: {
    create(utxo: Omit<UTXO, 'ref'>): Promise<UTXO>
    update(utxo: UTXO): Promise<void>
    get(ref: VRef): Promise<UTXO | null>
    query(filter: {
      spu_ref?: VRef
      step_skill?: string
      status?: string
      limit?: number
    }): Promise<UTXO[]>
  }
  skillLoader: {
    load(skillName: string): Promise<Skill>
  }
  confidenceStore: {
    get(skillName: string): Promise<number>
  }
  ruleEvaluator: {
    evaluate(expression: string, utxo: UTXO): Promise<boolean>
  }
  auditStore: {
    record(event: Record<string, unknown>): Promise<void>
  }
}

// ── Step 执行结果 ─────────────────────────────────────────────

export interface StepResult {
  stepSeq: number
  utxoRef: VRef
  auto: boolean
  confidence: number
}

// ── 参数规格类型 ──────────────────────────────────────────────

export interface ParameterSpec {
  label: string
  type: 'float' | 'integer' | 'enum' | 'string' | 'boolean'
  unit?: string
  range?: [number, number]
  values?: (string | number)[]
  default?: string | number
  spec_ref?: string
  depends_on?: string[]
  note?: string
}

export interface DerivedParamSpec {
  label: string
  type: string
  unit?: string
  formula: string
  constraint?: string
  spec_ref?: string
}

export interface ResourceRefSpec {
  label: string
  kind: string
  required: boolean
  condition?: string
  fields_needed: string[]
}
