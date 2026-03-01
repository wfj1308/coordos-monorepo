package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// This command generates a minimal DOCX report from qualification JSON data.
// It is dependency-free (std lib only) so it works in offline environments.
//
// Usage:
//
//	go run ./services/design-institute/cmd/qual-report --in data.json --out report.docx
//	go run ./services/design-institute/cmd/qual-report data.json report.docx
func main() {
	inFlag := flag.String("in", "", "input report json path")
	outFlag := flag.String("out", "", "output docx path")
	flag.Parse()

	inPath, outPath := resolvePaths(*inFlag, *outFlag, flag.Args())
	if inPath == "" {
		fatalf("missing input json path")
	}
	if outPath == "" {
		outPath = "qualification_report.docx"
	}

	raw, err := os.ReadFile(inPath)
	if err != nil {
		fatalf("read input failed: %v", err)
	}
	// PowerShell's UTF8 Set-Content may prepend BOM; trim it for robust JSON parsing.
	raw = bytes.TrimPrefix(raw, []byte{0xEF, 0xBB, 0xBF})

	payload := map[string]any{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		fatalf("parse input json failed: %v", err)
	}

	lines := buildReportLines(payload)
	documentXML := buildDocumentXML(lines)
	if err := writeDocx(outPath, documentXML); err != nil {
		fatalf("write docx failed: %v", err)
	}

	fmt.Printf("[PASS] Generated report: %s\n", outPath)
}

func resolvePaths(inFlag, outFlag string, args []string) (string, string) {
	inPath := strings.TrimSpace(inFlag)
	outPath := strings.TrimSpace(outFlag)
	if inPath == "" && len(args) >= 1 {
		inPath = strings.TrimSpace(args[0])
	}
	if outPath == "" && len(args) >= 2 {
		outPath = strings.TrimSpace(args[1])
	}
	return inPath, outPath
}

func buildReportLines(data map[string]any) []string {
	year := asInt(data["report_year"])
	if year <= 0 {
		year = time.Now().Year()
	}

	generatedAt := asString(data["generated_at"])
	if generatedAt == "" {
		generatedAt = time.Now().Format(time.RFC3339)
	}

	company := asMap(data["company"])
	companyName := asString(company["name"])
	if companyName == "" {
		companyName = "N/A"
	}

	lines := []string{
		fmt.Sprintf("Qualification Report %d", year),
		companyName,
		fmt.Sprintf("Generated At: %s", generatedAt),
		"",
		"1. Company Information",
		fmt.Sprintf("Name: %s", companyName),
		fmt.Sprintf("Unified Code: %s", fallback(asString(company["unified_code"]), "N/A")),
		fmt.Sprintf("Legal Representative: %s", fallback(asString(company["legal_rep"]), "N/A")),
		fmt.Sprintf("Technical Director: %s", fallback(asString(company["tech_director"]), "N/A")),
		fmt.Sprintf("Phone: %s", fallback(asString(company["phone"]), "N/A")),
		fmt.Sprintf("Address: %s", fallback(asString(company["address"]), "N/A")),
		fmt.Sprintf("Established Year: %s", fallback(numberOrString(company["established_year"]), "N/A")),
		"",
		"2. Company Certificates",
	}

	certs := asSlice(data["company_certs"])
	if len(certs) == 0 {
		lines = append(lines, "- None")
	} else {
		for i, item := range certs {
			c := asMap(item)
			lines = append(lines,
				fmt.Sprintf("- [%d] %s | %s | issuer=%s | valid=%s ~ %s | status=%s | level=%s | specialty=%s",
					i+1,
					fallback(asString(c["cert_type"]), "N/A"),
					fallback(asString(c["cert_no"]), "N/A"),
					fallback(asString(c["issued_by"]), "N/A"),
					fallback(trimDate(asString(c["valid_from"])), "N/A"),
					fallback(trimDate(asString(c["valid_until"])), "N/A"),
					fallback(asString(c["status"]), "N/A"),
					fallback(asString(c["level"]), "N/A"),
					fallback(asString(c["specialty"]), "N/A"),
				),
			)
		}
	}

	lines = append(lines, "", "3. Registered Persons")
	persons := asSlice(data["registered_persons"])
	if len(persons) == 0 {
		lines = append(lines, "- None")
	} else {
		for i, item := range persons {
			p := asMap(item)
			lines = append(lines,
				fmt.Sprintf("- [%d] %s | cert=%s | no=%s | valid_until=%s | specialty=%s",
					i+1,
					fallback(asString(p["name"]), "N/A"),
					fallback(asString(p["cert_type"]), "N/A"),
					fallback(asString(p["cert_no"]), "N/A"),
					fallback(trimDate(asString(p["valid_until"])), "N/A"),
					fallback(asString(p["specialty"]), "N/A"),
				),
			)
			projects := asSlice(p["recent_projects"])
			for _, prj := range projects {
				rp := asMap(prj)
				lines = append(lines, fmt.Sprintf("  * %s | role=%s | year=%s | spu=%s",
					fallback(asString(rp["project_name"]), "N/A"),
					fallback(asString(rp["role"]), "N/A"),
					fallback(numberOrString(rp["year"]), "N/A"),
					fallback(asString(rp["spu_ref"]), "N/A"),
				))
			}
		}
	}

	lines = append(lines, "", "4. Project Records (Recent 3 Years)")
	records := asSlice(data["project_records"])
	if len(records) == 0 {
		lines = append(lines, "- None")
	} else {
		for i, item := range records {
			r := asMap(item)
			lines = append(lines, fmt.Sprintf("- [%d] %s | contract=%s | amount=%s | owner=%s | year=%s | type=%s | proof=%s",
				i+1,
				fallback(asString(r["project_name"]), "N/A"),
				fallback(asString(r["contract_no"]), "N/A"),
				formatAmountWan(r["contract_amount"]),
				fallback(asString(r["owner_name"]), "N/A"),
				fallback(numberOrString(r["completed_year"]), "N/A"),
				fallback(asString(r["project_type"]), "N/A"),
				fallback(asString(r["proof_utxo_ref"]), "manual"),
			))
		}
	}

	finance := asMap(data["finance"])
	lines = append(lines, "", "5. Finance (Recent 3 Years)")
	addFinanceLine := func(yearKey, amountKey string) {
		y := numberOrString(finance[yearKey])
		if y == "" {
			return
		}
		lines = append(lines, fmt.Sprintf("- %s : %s", y, formatAmountWan(finance[amountKey])))
	}
	addFinanceLine("year1", "year1_gathering")
	addFinanceLine("year2", "year2_gathering")
	addFinanceLine("year3", "year3_gathering")
	if lines[len(lines)-1] == "5. Finance (Recent 3 Years)" {
		lines = append(lines, "- None")
	}

	lines = append(lines, "", "6. Expiry Warnings (Within 90 days)")
	warnings := asSlice(data["expiry_warnings"])
	if len(warnings) == 0 {
		lines = append(lines, "- None")
	} else {
		for i, item := range warnings {
			w := asMap(item)
			lines = append(lines, fmt.Sprintf("- [%d] %s | %s | %s | valid_until=%s | days_left=%s",
				i+1,
				fallback(asString(w["holder_name"]), "N/A"),
				fallback(asString(w["cert_type"]), "N/A"),
				fallback(asString(w["cert_no"]), "N/A"),
				fallback(trimDate(asString(w["valid_until"])), "N/A"),
				fallback(numberOrString(w["days_left"]), "N/A"),
			))
		}
	}

	return lines
}

func buildDocumentXML(lines []string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	b.WriteString(`<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">`)
	b.WriteString(`<w:body>`)
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			b.WriteString(`<w:p/>`)
			continue
		}
		b.WriteString(`<w:p><w:r><w:t xml:space="preserve">`)
		b.WriteString(xmlEscape(line))
		b.WriteString(`</w:t></w:r></w:p>`)
	}
	// Keep a minimal section definition so generated files open correctly in Word.
	b.WriteString(`<w:sectPr><w:pgSz w:w="12240" w:h="15840"/><w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" w:header="720" w:footer="720" w:gutter="0"/></w:sectPr>`)
	b.WriteString(`</w:body></w:document>`)
	return b.String()
}

func writeDocx(outPath string, documentXML string) error {
	outDir := filepath.Dir(outPath)
	if outDir != "" && outDir != "." {
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			return err
		}
	}

	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	zw := zip.NewWriter(f)
	defer zw.Close()

	now := time.Now().UTC().Format(time.RFC3339)

	files := map[string]string{
		"[Content_Types].xml": contentTypesXML(),
		"_rels/.rels":         relsXML(),
		"docProps/core.xml":   corePropsXML(now),
		"docProps/app.xml":    appPropsXML(),
		"word/document.xml":   documentXML,
		"word/_rels/document.xml.rels": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"></Relationships>`,
	}

	for name, content := range files {
		if err := writeZipFile(zw, name, content); err != nil {
			return err
		}
	}

	return nil
}

func writeZipFile(zw *zip.Writer, name, content string) error {
	h := &zip.FileHeader{
		Name:   name,
		Method: zip.Deflate,
	}
	h.SetMode(0o644)
	w, err := zw.CreateHeader(h)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(content))
	return err
}

func contentTypesXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>`
}

func relsXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>`
}

func corePropsXML(ts string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>Qualification Report</dc:title>
  <dc:creator>CoordOS</dc:creator>
  <cp:lastModifiedBy>CoordOS</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">%s</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">%s</dcterms:modified>
</cp:coreProperties>`, ts, ts)
}

func appPropsXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>CoordOS</Application>
</Properties>`
}

func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func asSlice(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case fmt.Stringer:
		return strings.TrimSpace(x.String())
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', 2, 64)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case json.Number:
		return x.String()
	default:
		return ""
	}
}

func asInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case string:
		i, _ := strconv.Atoi(strings.TrimSpace(x))
		return i
	case json.Number:
		i, _ := x.Int64()
		return int(i)
	default:
		return 0
	}
}

func numberOrString(v any) string {
	s := asString(v)
	if s != "" {
		return s
	}
	return ""
}

func trimDate(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 10 {
		return v[:10]
	}
	return v
}

func formatAmountWan(v any) string {
	switch x := v.(type) {
	case float64:
		return fmt.Sprintf("%.2f wan", x/10000.0)
	case int:
		return fmt.Sprintf("%.2f wan", float64(x)/10000.0)
	case int64:
		return fmt.Sprintf("%.2f wan", float64(x)/10000.0)
	case string:
		if strings.TrimSpace(x) == "" {
			return "N/A"
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return x
		}
		return fmt.Sprintf("%.2f wan", f/10000.0)
	default:
		return "N/A"
	}
}

func fallback(v, d string) string {
	if strings.TrimSpace(v) == "" {
		return d
	}
	return v
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[FAIL] "+format+"\n", args...)
	os.Exit(1)
}
