import { useState } from "react";
import { apiRequest, asArray, normalizeListData, pickField, toInt, trimTrailingSlash } from "../components/app/utils";

const flowBlueprint = [
  { key: "phase0", title: "Phase 0 注册入网", detail: "检查/初始化命名空间、资质容器、工程师基础数据" },
  { key: "phase1", title: "Phase 1 招标发布", detail: "创建 Tender Genesis UTXO（含入口调用）" },
  { key: "phase2", title: "Phase 2 编标投标", detail: "创建并提交 Bid，引用资质/工程师/业绩资源" },
  { key: "phase3", title: "Phase 3 中标", detail: "触发 fn_bid_awarded，自动生成项目与合同锚点" },
  { key: "phase4", title: "Phase 4 执行履约", detail: "写入并签发 Step Achievement（DRAFT->SIGNED）" },
  { key: "phase5", title: "Phase 5 审图验收", detail: "调用 review/seal 产出 Review Certificate UTXO" },
  { key: "phase6", title: "Phase 6 结算产出业绩", detail: "检查结算触发与 settlement 步骤状态" },
  { key: "phase7", title: "Phase 7 业绩入池", detail: "验证 achievement_utxos 在 bid pool 可检索" },
];

function buildFlowSteps() {
  return flowBlueprint.map((it) => ({
    ...it,
    status: "pending",
    elapsedMs: null,
    result: null,
    error: "",
  }));
}

export default function useMainFlow({ diBase, useAuth, token, onResponse }) {
  const [flowSteps, setFlowSteps] = useState(buildFlowSteps());
  const [flowRunning, setFlowRunning] = useState(false);
  const [flowSummary, setFlowSummary] = useState(null);

  const resetFlow = () => {
    setFlowSteps(buildFlowSteps());
    setFlowSummary(null);
  };

  const runMainFlow = async () => {
    if (flowRunning) return;

    const di = trimTrailingSlash(diBase.trim());
    if (!di) {
      onResponse?.(JSON.stringify({ error: "Design-Ins Base URL 不能为空" }, null, 2));
      return;
    }

    const authToken = useAuth ? token : "";
    const req = (input) => apiRequest({ ...input, token: authToken });
    const runCode = `P7-${Date.now()}`;
    const isoNow = new Date().toISOString();

    const ctx = {
      runCode,
      startedAt: isoNow,
      namespaceRef: "",
      namespaceSlug: "",
      ownedGenesis: [],
      reviewBatchRef: "",
      publishRightRef: "",
      companyQualRefs: [],
      engineerCount: 0,
      personQualRef: "",
      executorRef: "",
      chiefEngineerRef: "",
      tenderRef: "",
      bidID: 0,
      bidRef: "",
      projectRef: "",
      contractID: 0,
      stepRefs: [],
      reviewCertUTXORef: "",
      reviewProofHash: "",
      settlementStepRef: "",
      poolSettlementUTXO: "",
      poolReviewUTXO: "",
    };

    setFlowSteps(buildFlowSteps());
    setFlowSummary(null);
    setFlowRunning(true);

    const markStep = (idx, patch) => {
      setFlowSteps((prev) => prev.map((s, i) => (i === idx ? { ...s, ...patch } : s)));
    };

    const runStep = async (idx, task) => {
      markStep(idx, { status: "running", error: "", result: null, elapsedMs: null });
      const started = performance.now();
      try {
        const result = await task();
        markStep(idx, {
          status: "done",
          result,
          elapsedMs: Math.round(performance.now() - started),
        });
      } catch (err) {
        markStep(idx, {
          status: "failed",
          error: String(err),
          elapsedMs: Math.round(performance.now() - started),
        });
        throw err;
      }
    };

    try {
      await runStep(0, async () => {
        const health = await req({ method: "GET", url: `${di}/health` });

        const tryGetNamespace = async (ref) => {
          try {
            const out = await req({
              method: "GET",
              url: `${di}/api/v1/namespaces/get?ref=${encodeURIComponent(ref)}`,
            });
            return out.data;
          } catch {
            return null;
          }
        };

        let namespace = await tryGetNamespace("v://zhongbei");
        if (!namespace) {
          namespace = await tryGetNamespace("v://cn.zhongbei");
        }

        if (!namespace) {
          const registerPayload = {
            short_code: "cnzhongbei",
            namespace_ref: "v://cnzhongbei",
            company_name: "中北设计院",
            credit_code: "91110108MA01ZHONGBEI",
            cert_no: "A142000000-19/6",
            cert_valid_until: "2030-12-31",
            org_type: "HEAD_OFFICE",
            qualifications: [
              { resource_type: "QUAL_HIGHWAY_INDUSTRY_A", name: "Highway Industry Grade A" },
              { resource_type: "QUAL_MUNICIPAL_INDUSTRY_A", name: "Municipal Industry Grade A" },
              { resource_type: "QUAL_ARCH_COMPREHENSIVE_A", name: "Architecture Industry Grade A" },
              { resource_type: "QUAL_LANDSCAPE_SPECIAL_A", name: "Landscape Design Special Grade A" },
              { resource_type: "QUAL_WATER_INDUSTRY_B", name: "Water Industry Grade B" },
            ],
          };
          const created = await req({
            method: "POST",
            url: `${di}/api/v1/register/org`,
            body: registerPayload,
          });
          const nsRef = String(pickField(created.data, ["namespace_ref", "NamespaceRef"], ""));
          if (!nsRef) {
            throw new Error("register/org 已返回成功，但未返回 namespace_ref");
          }
          namespace = await tryGetNamespace(nsRef);
        }

        if (!namespace) {
          throw new Error("Phase 0: 未能读取或创建命名空间");
        }

        ctx.namespaceRef = String(pickField(namespace, ["ref", "Ref"], "")).trim();
        if (!ctx.namespaceRef) {
          throw new Error("命名空间记录缺少 ref");
        }
        ctx.namespaceSlug = ctx.namespaceRef.replace(/^v:\/\//, "").split("/")[0];
        ctx.ownedGenesis = asArray(pickField(namespace, ["owned_genesis", "OwnedGenesis"], []));
        ctx.companyQualRefs = ctx.ownedGenesis.filter((it) => String(it).includes("/genesis/qual/"));
        ctx.reviewBatchRef =
          ctx.ownedGenesis.find((it) => String(it).endsWith("/genesis/right/review_stamp")) ||
          `${ctx.namespaceRef}/genesis/right/review_stamp`;
        ctx.publishRightRef =
          ctx.ownedGenesis.find((it) => String(it).endsWith("/genesis/right/publish")) ||
          `${ctx.namespaceRef}/genesis/right/publish`;

        const [qualRes, empRes] = await Promise.all([
          req({
            method: "GET",
            url: `${di}/api/v1/qualifications?holder_type=PERSON&status=VALID&limit=200&offset=0`,
          }),
          req({
            method: "GET",
            url: `${di}/api/v1/employees?limit=300&offset=0`,
          }),
        ]);

        const personQualRows = normalizeListData(qualRes.data).filter((row) => {
          const exRef = String(pickField(row, ["executor_ref", "ExecutorRef"], ""));
          return exRef !== "";
        });

        if (personQualRows.length > 0) {
          const firstRow = personQualRows[0];
          ctx.personQualRef = String(pickField(firstRow, ["ref", "Ref", "cert_no", "CertNo"], ""));
          ctx.executorRef = String(pickField(firstRow, ["executor_ref", "ExecutorRef"], ""));
          ctx.chiefEngineerRef = String(
            pickField(personQualRows[Math.min(1, personQualRows.length - 1)], ["executor_ref", "ExecutorRef"], ""),
          );
        }

        const employeeRows = normalizeListData(empRes.data);
        ctx.engineerCount = employeeRows.filter((row) => {
          const exRef = String(pickField(row, ["executor_ref", "ExecutorRef"], ""));
          return exRef !== "";
        }).length;

        if (!ctx.executorRef) {
          throw new Error("Phase 0: 未找到可用 executor_ref（缺少有效人员资质）");
        }
        if (!ctx.chiefEngineerRef) {
          ctx.chiefEngineerRef = ctx.executorRef;
        }

        return {
          health: pickField(health.data, ["service"], "design-institute"),
          namespace_ref: ctx.namespaceRef,
          namespace_slug: ctx.namespaceSlug,
          qualification_genesis_count: ctx.companyQualRefs.length,
          engineer_count: ctx.engineerCount,
          review_batch_ref: ctx.reviewBatchRef,
        };
      });

      await runStep(1, async () => {
        const payload = {
          project_name: `中北桥梁示例-${runCode}`,
          project_type: "BRIDGE",
          owner_name: "业主单位A",
          estimated_amount: 12800,
          bid_deadline: new Date(Date.now() + 14 * 24 * 3600 * 1000).toISOString(),
          rule_binding: ["RULE-002", "RULE-003"],
        };
        const out = await req({
          method: "POST",
          url: `${di}/api/v1/tender/${encodeURIComponent(ctx.namespaceSlug)}`,
          body: payload,
        });
        ctx.tenderRef = String(pickField(out.data, ["ref", "Ref"], ""));
        if (!ctx.tenderRef) {
          throw new Error("Phase 1: 招标发布成功但未返回 tender ref");
        }
        return {
          tender_ref: ctx.tenderRef,
          namespace: ctx.namespaceSlug,
          project_type: "BRIDGE",
        };
      });

      await runStep(2, async () => {
        const validateOut = await req({
          method: "POST",
          url: `${di}/api/v1/bid/validate`,
          body: {
            namespace_ref: ctx.namespaceRef,
            project_type: "BRIDGE",
            target_spu: "v://zhongbei/spu/bid/submission@v1",
            executor_ref: ctx.executorRef,
            need_person_quals: 1,
            need_achievements: 1,
            achievement_years: 3,
          },
        });

        const poolBefore = await req({
          method: "GET",
          url: `${di}/api/v1/bid/pool/${encodeURIComponent(ctx.namespaceSlug)}?limit=10&offset=0`,
        });
        const poolRows = normalizeListData(poolBefore.data);
        const achRef = String(pickField(poolRows[0] || {}, ["utxo_ref", "UTXORef"], ""));

        const bidCreatePayload = {
          ProjectName: `中北桥梁投标包-${runCode}`,
          ProjectType: "BRIDGE",
          NamespaceRef: ctx.namespaceRef,
          TenderGenesisRef: ctx.tenderRef,
          CompanyQualRefs: ctx.companyQualRefs.slice(0, 5),
          PersonQualRefs: ctx.personQualRef ? [ctx.personQualRef] : [],
          AchievementRefs: achRef ? [achRef] : [],
        };
        const created = await req({
          method: "POST",
          url: `${di}/api/v1/bid`,
          body: bidCreatePayload,
        });
        const bidDoc = pickField(created.data, ["bid", "Bid"], {});
        ctx.bidID = toInt(pickField(bidDoc, ["id", "ID"], 0));
        ctx.bidRef = String(pickField(bidDoc, ["bid_ref", "BidRef"], ""));
        if (!ctx.bidID) {
          throw new Error("Phase 2: 创建 Bid 成功但未返回 bid.id");
        }

        await req({
          method: "POST",
          url: `${di}/api/v1/bid/${ctx.bidID}/submit`,
          body: {},
        });
        return {
          bid_id: ctx.bidID,
          bid_ref: ctx.bidRef,
          can_bid: !!pickField(validateOut.data, ["can_bid", "CanBid"], false),
          pool_candidates: poolRows.length,
        };
      });

      await runStep(3, async () => {
        const award = await req({
          method: "POST",
          url: `${di}/api/v1/bid/${ctx.bidID}/award`,
          body: {},
        });
        ctx.projectRef = String(pickField(award.data, ["project_ref", "ProjectRef"], ""));
        ctx.contractID = toInt(pickField(award.data, ["contract_id", "ContractID"], 0));
        if (!ctx.projectRef || !ctx.contractID) {
          throw new Error("Phase 3: 中标后未拿到 project_ref/contract_id，fn_bid_awarded 可能未生效");
        }
        return {
          bid_id: ctx.bidID,
          project_ref: ctx.projectRef,
          contract_id: ctx.contractID,
        };
      });

      await runStep(4, async () => {
        const containerRef = ctx.personQualRef || `${ctx.namespaceRef}/container/cert/reg-structure/default`;
        const step1 = await req({
          method: "POST",
          url: `${di}/api/v1/step-achievements`,
          body: {
            namespace_ref: ctx.namespaceRef,
            project_ref: ctx.projectRef,
            step_seq: 1,
            executor_ref: ctx.executorRef,
            container_ref: containerRef,
            output_type: "DESIGN_DOC",
            output_name: `方案设计-${runCode}`,
            source: "TRIP_DERIVED",
          },
        });
        const step1Ref = String(pickField(step1.data, ["ref", "Ref"], ""));
        if (!step1Ref) throw new Error("Phase 4: 第一步 StepAchievement 缺少 ref");

        await req({
          method: "POST",
          url: `${di}/api/v1/step-achievements/sign`,
          body: {
            ref: step1Ref,
            signed_by: ctx.executorRef,
            is_final_step: false,
            project_ref: ctx.projectRef,
          },
        });

        const step2 = await req({
          method: "POST",
          url: `${di}/api/v1/step-achievements`,
          body: {
            namespace_ref: ctx.namespaceRef,
            project_ref: ctx.projectRef,
            step_seq: 2,
            executor_ref: ctx.chiefEngineerRef,
            container_ref: containerRef,
            output_type: "REVIEW_CERT",
            output_name: `审图确认-${runCode}`,
            spu_ref: "v://coordos/spu/review/review_certificate@v1",
            source: "TRIP_DERIVED",
          },
        });
        const step2Ref = String(pickField(step2.data, ["ref", "Ref"], ""));
        if (!step2Ref) throw new Error("Phase 4: REVIEW_CERT StepAchievement 缺少 ref");

        await req({
          method: "POST",
          url: `${di}/api/v1/step-achievements/sign`,
          body: {
            ref: step2Ref,
            signed_by: ctx.chiefEngineerRef,
            is_final_step: true,
            project_ref: ctx.projectRef,
          },
        });

        ctx.stepRefs = [step1Ref, step2Ref];
        const progress = await req({
          method: "GET",
          url: `${di}/api/v1/step-achievements/progress?project_ref=${encodeURIComponent(ctx.projectRef)}`,
        });
        return {
          step_refs: ctx.stepRefs,
          signed_steps: toInt(pickField(progress.data, ["signed_steps", "SignedSteps"], 0)),
          total_steps: toInt(pickField(progress.data, ["total_steps", "TotalSteps"], 0)),
          ready_to_settle: !!pickField(progress.data, ["ready_to_settle", "ReadyToSettle"], false),
        };
      });

      await runStep(5, async () => {
        const fallbackBatchRef = `${ctx.namespaceRef}/genesis/right/review_stamp`;
        const sealBodyBase = {
          project_ref: ctx.projectRef,
          executor_ref: ctx.executorRef,
          chief_engineer_ref: ctx.chiefEngineerRef,
          review_comments_utxo: `${ctx.projectRef}/utxo/review-comments/${runCode}`,
          resolution_rate: 100,
        };

        const trySeal = async (batchRef) =>
          req({
            method: "POST",
            url: `${di}/api/v1/review/seal`,
            body: { batch_ref: batchRef, ...sealBodyBase },
          });

        let sealed;
        try {
          sealed = await trySeal(ctx.reviewBatchRef);
        } catch (err) {
          if (ctx.reviewBatchRef === fallbackBatchRef) {
            throw err;
          }
          sealed = await trySeal(fallbackBatchRef);
          ctx.reviewBatchRef = fallbackBatchRef;
        }

        const consumption = pickField(sealed.data, ["consumption", "Consumption"], {});
        ctx.reviewCertUTXORef = String(pickField(consumption, ["utxo_ref", "UTXORef"], ""));
        ctx.reviewProofHash = String(pickField(consumption, ["proof_hash", "ProofHash"], ""));
        if (!ctx.reviewCertUTXORef) {
          throw new Error("Phase 5: 审图签发成功但未返回 review certificate utxo_ref");
        }
        return {
          batch_ref: ctx.reviewBatchRef,
          review_cert_utxo_ref: ctx.reviewCertUTXORef,
          proof_hash: ctx.reviewProofHash,
        };
      });

      await runStep(6, async () => {
        const listRes = await req({
          method: "GET",
          url: `${di}/api/v1/step-achievements?project_ref=${encodeURIComponent(ctx.projectRef)}`,
        });
        const steps = normalizeListData(listRes.data);
        const settlementStep = steps.find((row) => {
          const outputType = String(pickField(row, ["output_type", "OutputType"], ""));
          const status = String(pickField(row, ["status", "Status"], ""));
          return outputType === "SETTLEMENT" && (status === "SETTLED" || status === "SIGNED");
        });
        if (!settlementStep) {
          throw new Error("Phase 6: 未发现 SETTLEMENT step，fn_project_settled 触发链路未生效");
        }
        ctx.settlementStepRef = String(pickField(settlementStep, ["ref", "Ref"], ""));
        return {
          settlement_step_ref: ctx.settlementStepRef,
          total_steps: steps.length,
        };
      });

      await runStep(7, async () => {
        const pool = await req({
          method: "GET",
          url: `${di}/api/v1/bid/pool/${encodeURIComponent(ctx.namespaceSlug)}?limit=200&offset=0`,
        });
        const items = normalizeListData(pool.data);
        const settlementHit = items.find((row) => {
          const projectRef = String(pickField(row, ["project_ref", "ProjectRef"], ""));
          const spuRef = String(pickField(row, ["spu_ref", "SPURef", "SpuRef"], ""));
          return projectRef === ctx.projectRef && spuRef.includes("settlement_cert");
        });
        const reviewHit = items.find((row) => {
          const projectRef = String(pickField(row, ["project_ref", "ProjectRef"], ""));
          const spuRef = String(pickField(row, ["spu_ref", "SPURef", "SpuRef"], ""));
          return projectRef === ctx.projectRef && spuRef.includes("review_certificate");
        });
        if (!settlementHit) {
          throw new Error("Phase 7: bid pool 未检索到 settlement_cert 业绩，achievement_utxos 入池未完成");
        }
        ctx.poolSettlementUTXO = String(pickField(settlementHit, ["utxo_ref", "UTXORef"], ""));
        ctx.poolReviewUTXO = String(pickField(reviewHit || {}, ["utxo_ref", "UTXORef"], ""));
        return {
          project_ref: ctx.projectRef,
          settlement_utxo_ref: ctx.poolSettlementUTXO,
          review_utxo_ref: ctx.poolReviewUTXO,
          pool_total: toInt(pickField(pool.data, ["total", "Total"], items.length)),
        };
      });

      const doneAt = new Date().toISOString();
      setFlowSummary({ ...ctx, doneAt, status: "success" });
      onResponse?.(JSON.stringify({ scenario: "Phase0-7 七步闭环", status: "success", context: ctx }, null, 2));
    } catch (err) {
      setFlowSummary({ ...ctx, doneAt: new Date().toISOString(), status: "failed", error: String(err) });
      onResponse?.(
        JSON.stringify({ scenario: "Phase0-7 七步闭环", status: "failed", error: String(err), context: ctx }, null, 2),
      );
    } finally {
      setFlowRunning(false);
    }
  };

  return {
    flowSteps,
    flowRunning,
    flowSummary,
    runMainFlow,
    resetFlow,
  };
}
