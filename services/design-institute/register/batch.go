//go:build archive
// +build archive

// ============================================================
//  register/batch.go
//  批量导入服务
//
//  一次调用 = 一个节点完整上线
//  支持 JSON 和 Excel（.xlsx）两种格式
//
//  导入顺序（严格按依赖关系）：
//    1. 公司基础信息    → namespace + org Executor
//    2. 企业资质证书    → CERT容器(企业) + Genesis UTXO
//    3. 工程师列表      → person Executor × N
//    4. 工程师证书列表  → CERT容器(个人) + 挂载到Executor
//
//  max_parallel 规则（系统自动设定，用户无需填写）：
//    工程师证书（REG_*）= 4（行业确认值）
//    企业资质（QUAL_*） = 999（引用型）
//
//  幂等性：重复导入只更新变化字段，不报错
//  错误处理：单条失败不中断批次，收集所有错误统一返回
// ============================================================

package register

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// BatchInput 完整节点注册包（JSON格式）
type BatchInput struct {
	Org       BatchOrg        `json:"org"`
	Quals     []BatchQual     `json:"quals"`
	Engineers []BatchEngineer `json:"engineers"`
	TenantID  int             `json:"tenant_id"`
}

type BatchOrg struct {
	Name          string  `json:"name"`
	ShortCode     string  `json:"short_code"`
	CreditCode    string  `json:"credit_code"`
	LegalRep      string  `json:"legal_rep"`
	TechDirector  string  `json:"tech_director"`
	Address       string  `json:"address,omitempty"`
	RegCapital    float64 `json:"reg_capital"`
	EstablishedAt string  `json:"established_at,omitempty"`
	ParentRef     string  `json:"parent_ref,omitempty"`
}

type BatchQual struct {
	CertType   string `json:"cert_type"`
	CertNo     string `json:"cert_no"`
	ValidUntil string `json:"valid_until"`
	Issuer     string `json:"issuer,omitempty"`
}

type BatchEngineer struct {
	Name     string      `json:"name"`
	IDSuffix string      `json:"id_suffix"`
	Dept     string      `json:"dept,omitempty"`
	Title    string      `json:"title,omitempty"`
	Certs    []BatchCert `json:"certs"`
}

type BatchCert struct {
	CertType   string `json:"cert_type"`
	CertNo     string `json:"cert_no"`
	RegNumber  string `json:"reg_number,omitempty"`
	ValidUntil string `json:"valid_until"`
}

type BatchResult struct {
	NamespaceRef   string       `json:"namespace_ref"`
	OrgName        string       `json:"org_name"`
	QualsTotal     int          `json:"quals_total"`
	QualsOK        int          `json:"quals_ok"`
	EngineersTotal int          `json:"engineers_total"`
	EngineersOK    int          `json:"engineers_ok"`
	CertsTotal     int          `json:"certs_total"`
	CertsOK        int          `json:"certs_ok"`
	QualContainers []string     `json:"qual_containers"`
	CertContainers []string     `json:"cert_containers"`
	Errors         []BatchError `json:"errors,omitempty"`
	ReadyForBid    bool         `json:"ready_for_bid"`
	ReadyReason    []string     `json:"ready_reason"`
	DurationMs     int64        `json:"duration_ms"`
	ImportedAt     string       `json:"imported_at"`
}

type BatchError struct {
	Item    string `json:"item"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ImportBatch 批量导入核心入口
func (s *Service) ImportBatch(ctx context.Context, in BatchInput) (*BatchResult, error) {
	start := time.Now()
	result := &BatchResult{OrgName: in.Org.Name, ImportedAt: start.Format(time.RFC3339)}
	if in.TenantID == 0 {
		in.TenantID = s.tenantID
	}

	// Step1: 公司入网（失败则中断）
	orgResult, err := s.RegisterOrg(ctx, Step1Input{
		Name: in.Org.Name, ShortCode: in.Org.ShortCode,
		CreditCode: in.Org.CreditCode, LegalRep: in.Org.LegalRep,
		TechDirector: in.Org.TechDirector, Address: in.Org.Address,
		RegCapital: in.Org.RegCapital, EstablishedAt: in.Org.EstablishedAt,
		ParentRef: in.Org.ParentRef, TenantID: in.TenantID,
	})
	if err != nil {
		return nil, fmt.Errorf("批量导入失败：公司注册错误：%w", err)
	}
	result.NamespaceRef = orgResult.NamespaceRef

	// Step2: 企业资质
	result.QualsTotal = len(in.Quals)
	for i, q := range in.Quals {
		if berr := validateBatchQual(q, i); berr != nil {
			result.Errors = append(result.Errors, *berr)
			continue
		}
		qr, err := s.RegisterQual(ctx, Step2Input{
			NamespaceRef: orgResult.NamespaceRef,
			CertType:     q.CertType, CertNo: q.CertNo,
			ValidUntil: q.ValidUntil, Issuer: q.Issuer,
			TenantID: in.TenantID,
		})
		if err != nil {
			result.Errors = append(result.Errors, BatchError{
				Item: fmt.Sprintf("quals[%d] %s", i, q.CertType), Field: "cert_type", Message: err.Error(),
			})
			continue
		}
		result.QualsOK++
		result.QualContainers = append(result.QualContainers, qr.ContainerRef)
	}

	// Step3: 工程师 + 证书（单条失败不中断）
	result.EngineersTotal = len(in.Engineers)
	for i, eng := range in.Engineers {
		if berr := validateBatchEngineer(eng, i); berr != nil {
			result.Errors = append(result.Errors, *berr)
			continue
		}
		er, err := s.RegisterEngineer(ctx, Step3aInput{
			NamespaceRef: orgResult.NamespaceRef,
			Name:         eng.Name, IDSuffix: eng.IDSuffix,
			Dept: eng.Dept, Title: eng.Title, TenantID: in.TenantID,
		})
		if err != nil {
			result.Errors = append(result.Errors, BatchError{
				Item: fmt.Sprintf("engineers[%d] %s", i, eng.Name), Field: "name", Message: err.Error(),
			})
			continue
		}
		result.EngineersOK++

		result.CertsTotal += len(eng.Certs)
		for j, cert := range eng.Certs {
			if berr := validateBatchCert(cert, i, j); berr != nil {
				result.Errors = append(result.Errors, *berr)
				continue
			}
			cr, err := s.RegisterCert(ctx, Step3bInput{
				NamespaceRef: orgResult.NamespaceRef,
				EngineerID:   er.EngineerID,
				CertType:     cert.CertType, CertNo: cert.CertNo,
				RegNumber: cert.RegNumber, ValidUntil: cert.ValidUntil,
				TenantID: in.TenantID,
			})
			if err != nil {
				result.Errors = append(result.Errors, BatchError{
					Item:  fmt.Sprintf("engineers[%d].certs[%d] %s·%s", i, j, eng.Name, cert.CertType),
					Field: "cert_type", Message: err.Error(),
				})
				continue
			}
			result.CertsOK++
			result.CertContainers = append(result.CertContainers, cr.ContainerRef)
		}
	}

	// 检查节点就绪状态
	if st, err := s.GetOrgStatus(ctx, orgResult.NamespaceRef); err == nil {
		result.ReadyForBid = st.ReadyForBid
		result.ReadyReason = st.ReadyReason
	}

	result.DurationMs = time.Since(start).Milliseconds()
	return result, nil
}

// ExcelTemplate 返回导入模板描述（前端据此生成 Excel 下载）
func ExcelTemplate() map[string]any {
	return map[string]any{
		"sheets": []map[string]any{
			{
				"name": "Sheet1_公司信息",
				"columns": []string{
					"name（公司名称）", "short_code（命名空间，英文小写）",
					"credit_code（统一社会信用代码）", "legal_rep（法定代表人）",
					"tech_director（技术负责人）", "address（地址，可选）",
					"reg_capital（注册资本，万元）", "established_at（成立日期 YYYY-MM-DD，可选）",
				},
				"example": []string{
					"中北工程设计咨询有限公司", "cn.zhongbei",
					"91610000661186666D", "石玉山", "任祥", "陕西省西安市", "5000", "2005-03-15",
				},
			},
			{
				"name": "Sheet2_企业资质",
				"note": "max_parallel 由系统自动设定（企业资质=999），无需填写",
				"columns": []string{
					"cert_type（资质类型）", "cert_no（证书编号）",
					"valid_until（有效期 YYYY-MM-DD）", "issuer（发证机关，可选）",
				},
				"valid_cert_types": certTypesForTemplate("QUAL"),
				"example": [][]string{
					{"QUAL_HIGHWAY_A", "A161003712-10/1", "2030-02-14", "住房和城乡建设部"},
					{"QUAL_MUNICIPAL_A", "A161003712-10/1", "2030-02-14", "住房和城乡建设部"},
				},
			},
			{
				"name": "Sheet3_工程师与证书",
				"note": "一人多证时重复填写姓名和id_suffix，每行一个证书。max_parallel 由系统自动设定（工程师证书=4）",
				"columns": []string{
					"name（姓名）", "id_suffix（身份证后4位）",
					"dept（部门，可选）", "title（职称，可选）",
					"cert_type（证书类型）", "cert_no（证书编号）",
					"reg_number（注册号，可选）", "valid_until（有效期 YYYY-MM-DD）",
				},
				"valid_cert_types": certTypesForTemplate("REG"),
				"example": [][]string{
					{"陈勇攀", "4310", "结构设计部", "高级工程师", "REG_STRUCTURE", "6100371-S018", "", "2029-12-31"},
					{"李准", "0012", "结构设计部", "工程师", "REG_STRUCTURE", "6100371-S015", "", "2029-12-31"},
					{"李准", "0012", "", "", "REG_CIVIL_GEOTEC", "6100371-AY009", "", "2029-12-31"},
				},
			},
		},
		"auto_fields": map[string]any{
			"max_parallel_engineer": 4,
			"max_parallel_qual":     999,
			"note":                  "这两个字段由系统自动设定，导入时无需填写",
		},
		"cert_type_reference": certTypeDescriptions(),
	}
}

// SampleBatchJSON 返回标准导入示例（中北工程）
func SampleBatchJSON() *BatchInput {
	return &BatchInput{
		Org: BatchOrg{
			Name: "中北工程设计咨询有限公司", ShortCode: "cn.zhongbei",
			CreditCode: "91610000661186666D", LegalRep: "石玉山",
			TechDirector: "任祥", Address: "陕西省西安市", RegCapital: 5000,
		},
		Quals: []BatchQual{
			{CertType: "QUAL_HIGHWAY_A", CertNo: "A161003712-10/1", ValidUntil: "2030-02-14"},
			{CertType: "QUAL_MUNICIPAL_A", CertNo: "A161003712-10/1", ValidUntil: "2030-02-14"},
			{CertType: "QUAL_ARCH_A", CertNo: "A161003712-10/1", ValidUntil: "2030-02-14"},
			{CertType: "QUAL_LANDSCAPE_A", CertNo: "A161003712-10/1", ValidUntil: "2030-02-14"},
			{CertType: "QUAL_WATER_B", CertNo: "A161003712-10/1", ValidUntil: "2030-02-14"},
		},
		Engineers: []BatchEngineer{
			{
				Name: "陈勇攀", IDSuffix: "4310", Title: "高级工程师",
				Certs: []BatchCert{
					{CertType: "REG_STRUCTURE", CertNo: "6100371-S018", ValidUntil: "2029-12-31"},
				},
			},
			{
				Name: "戴永常", IDSuffix: "4019", Title: "高级工程师",
				Certs: []BatchCert{
					{CertType: "REG_STRUCTURE", CertNo: "6100371-S009", ValidUntil: "2029-12-31"},
					{CertType: "REG_COST", CertNo: "6100371-ZZ001", ValidUntil: "2028-12-31"},
				},
			},
			{
				Name: "李准", IDSuffix: "0012", Title: "工程师",
				Certs: []BatchCert{
					{CertType: "REG_STRUCTURE", CertNo: "6100371-S015", ValidUntil: "2029-12-31"},
					{CertType: "REG_CIVIL_GEOTEC", CertNo: "6100371-AY009", ValidUntil: "2029-12-31"},
				},
			},
		},
		TenantID: 1,
	}
}

// ── 校验函数 ──────────────────────────────────────────────────

func validateBatchQual(q BatchQual, idx int) *BatchError {
	if q.CertType == "" {
		return &BatchError{Item: fmt.Sprintf("quals[%d]", idx), Field: "cert_type", Message: "不能为空"}
	}
	if _, ok := certTypeMap[q.CertType]; !ok {
		return &BatchError{Item: fmt.Sprintf("quals[%d]", idx), Field: "cert_type",
			Message: fmt.Sprintf("不支持的资质类型 %q，有效类型：%s", q.CertType, validCertTypes())}
	}
	if q.CertNo == "" {
		return &BatchError{Item: fmt.Sprintf("quals[%d]", idx), Field: "cert_no", Message: "不能为空"}
	}
	if _, err := time.Parse("2006-01-02", q.ValidUntil); err != nil {
		return &BatchError{Item: fmt.Sprintf("quals[%d]", idx), Field: "valid_until", Message: "格式应为 YYYY-MM-DD"}
	}
	return nil
}

func validateBatchEngineer(eng BatchEngineer, idx int) *BatchError {
	if eng.Name == "" {
		return &BatchError{Item: fmt.Sprintf("engineers[%d]", idx), Field: "name", Message: "姓名不能为空"}
	}
	if eng.IDSuffix == "" {
		return &BatchError{Item: fmt.Sprintf("engineers[%d] %s", idx, eng.Name), Field: "id_suffix", Message: "身份证后4位不能为空"}
	}
	if len(eng.Certs) == 0 {
		return &BatchError{Item: fmt.Sprintf("engineers[%d] %s", idx, eng.Name), Field: "certs", Message: "至少需要一个证书"}
	}
	return nil
}

func validateBatchCert(cert BatchCert, engIdx, certIdx int) *BatchError {
	if cert.CertType == "" {
		return &BatchError{Item: fmt.Sprintf("engineers[%d].certs[%d]", engIdx, certIdx), Field: "cert_type", Message: "不能为空"}
	}
	if _, ok := certTypeMap[cert.CertType]; !ok {
		return &BatchError{Item: fmt.Sprintf("engineers[%d].certs[%d]", engIdx, certIdx), Field: "cert_type",
			Message: fmt.Sprintf("不支持的证书类型 %q", cert.CertType)}
	}
	if cert.CertNo == "" {
		return &BatchError{Item: fmt.Sprintf("engineers[%d].certs[%d]", engIdx, certIdx), Field: "cert_no", Message: "不能为空"}
	}
	if _, err := time.Parse("2006-01-02", cert.ValidUntil); err != nil {
		return &BatchError{Item: fmt.Sprintf("engineers[%d].certs[%d]", engIdx, certIdx), Field: "valid_until", Message: "格式应为 YYYY-MM-DD"}
	}
	return nil
}

func certTypesForTemplate(prefix string) []string {
	var types []string
	for k := range certTypeMap {
		if strings.HasPrefix(k, prefix) {
			types = append(types, k)
		}
	}
	return types
}

func certTypeDescriptions() []map[string]any {
	descs := []map[string]any{}
	for k, p := range certTypeMap {
		descs = append(descs, map[string]any{
			"cert_type": k, "name": certTypeName(k),
			"max_parallel": p.MaxParallel, "cap_tags": p.CapTags,
		})
	}
	return descs
}

func (r *BatchResult) Summary() string {
	b, _ := json.MarshalIndent(r, "", "  ")
	return string(b)
}
