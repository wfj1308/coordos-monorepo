import { useState } from "react";
import {
  apiRequest,
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

  return {
    partnerProfile,
    partnerProfileLoading,
    partnerProfileError,
    partnerProfileNamespace,
    loadPartnerProfile,
    handlePartnerProfileNamespaceChange,
  };
}
