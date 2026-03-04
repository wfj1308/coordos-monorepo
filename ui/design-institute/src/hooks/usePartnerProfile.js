import { useState } from "react";
import {
  asArray,
  apiRequest,
  getIn,
  normalizeNamespaceCode,
  readLocal,
  saveLocal,
  trimTrailingSlash,
} from "../components/app/utils";

export default function usePartnerProfile({ diBase, useAuth, token }) {
  const [partnerProfile, setPartnerProfile] = useState(null);
  const [partnerProfileLoading, setPartnerProfileLoading] = useState(false);
  const [partnerProfileError, setPartnerProfileError] = useState("");
  const [partnerProfileNamespace, setPartnerProfileNamespace] = useState(
    readLocal("coordos.partner.namespace", "cn.zhongbei"),
  );

  const [verifyInput, setVerifyInput] = useState(readLocal("coordos.partner.verify.input", ""));
  const [verifyLoading, setVerifyLoading] = useState(false);
  const [verifyError, setVerifyError] = useState("");
  const [verifyResult, setVerifyResult] = useState(null);

  const pickFirstProofHash = (profile) => {
    const items = asArray(getIn(profile, ["achievement_layer", "items"], []));
    for (const item of items) {
      const hashes = asArray(item?.proof_hashes);
      const first = String(hashes[0] || "").trim();
      if (first) return first;
    }
    return "";
  };

  const loadPartnerProfile = async () => {
    const di = trimTrailingSlash(diBase.trim());
    const namespaceCode = normalizeNamespaceCode(partnerProfileNamespace);
    if (!di) {
      setPartnerProfileError("Design-Ins Base URL 不能为空");
      return;
    }
    if (!namespaceCode) {
      setPartnerProfileError("命名空间不能为空，例如 cn.zhongbei");
      return;
    }
    setPartnerProfileLoading(true);
    setPartnerProfileError("");
    try {
      const res = await apiRequest({
        method: "GET",
        url: `${di}/public/v1/partner-profile/${encodeURIComponent(namespaceCode)}`,
        token: useAuth ? token : "",
      });
      setPartnerProfile(res.data);

      const firstProofHash = pickFirstProofHash(res.data);
      if (firstProofHash) {
        setVerifyInput(firstProofHash);
        saveLocal("coordos.partner.verify.input", firstProofHash);
      }
    } catch (err) {
      setPartnerProfile(null);
      setPartnerProfileError(String(err));
    } finally {
      setPartnerProfileLoading(false);
    }
  };

  const handlePartnerProfileNamespaceChange = (v) => {
    setPartnerProfileNamespace(v);
    saveLocal("coordos.partner.namespace", v);
  };

  const handleVerifyInputChange = (v) => {
    setVerifyInput(v);
    saveLocal("coordos.partner.verify.input", v);
  };

  const useFirstProofHash = () => {
    const first = pickFirstProofHash(partnerProfile);
    if (!first) {
      setVerifyError("当前能力声明没有可用 proof_hash");
      return;
    }
    setVerifyError("");
    handleVerifyInputChange(first);
  };

  const verifyAchievement = async () => {
    const di = trimTrailingSlash(diBase.trim());
    const raw = String(verifyInput || "").trim();
    if (!di) {
      setVerifyError("Design-Ins Base URL 不能为空");
      return;
    }
    if (!raw) {
      setVerifyError("请输入 ref 或 proof_hash");
      return;
    }
    setVerifyLoading(true);
    setVerifyError("");
    setVerifyResult(null);
    try {
      const isHash = raw.startsWith("sha256:") || /^[a-fA-F0-9]{64}$/.test(raw);
      const url = isHash
        ? `${di}/public/v1/verify/achievement/${encodeURIComponent(raw)}`
        : `${di}/api/v1/achievement/verify?ref=${encodeURIComponent(raw)}`;
      const res = await apiRequest({
        method: "GET",
        url,
        token: useAuth ? token : "",
      });
      setVerifyResult(res.data);
    } catch (err) {
      setVerifyError(String(err));
    } finally {
      setVerifyLoading(false);
    }
  };

  return {
    partnerProfile,
    partnerProfileLoading,
    partnerProfileError,
    partnerProfileNamespace,
    loadPartnerProfile,
    handlePartnerProfileNamespaceChange,
    verifyInput,
    verifyLoading,
    verifyError,
    verifyResult,
    handleVerifyInputChange,
    verifyAchievement,
    useFirstProofHash,
  };
}
