#!/usr/bin/env node
"use strict";

/**
 * Generate a qualification report DOCX from report API JSON.
 *
 * Usage:
 *   node scripts/gen_qual_report.js <input.json> <output.docx>
 *
 * Example:
 *   curl "http://127.0.0.1:8090/api/v1/reports/qualification?year=2026" > data.json
 *   node scripts/gen_qual_report.js data.json qualification_report_2026.docx
 */

const fs = require("fs");
const path = require("path");

let docx;
try {
  docx = require("docx");
} catch (err) {
  console.error("Missing dependency: docx");
  console.error("Please install it first, for example:");
  console.error("  npm install docx");
  process.exit(1);
}

const {
  Document,
  Packer,
  Paragraph,
  TextRun,
  Table,
  TableCell,
  TableRow,
  AlignmentType,
  BorderStyle,
  Header,
  Footer,
  PageNumber,
  WidthType,
} = docx;

const inputPath = process.argv[2] || "qualification_report_data.json";
const outputPath = process.argv[3] || "qualification_report.docx";

if (!fs.existsSync(inputPath)) {
  console.error(`Input file not found: ${inputPath}`);
  process.exit(1);
}

let data;
try {
  data = JSON.parse(fs.readFileSync(inputPath, "utf8"));
} catch (err) {
  console.error(`Failed to parse JSON: ${inputPath}`);
  console.error(err.message);
  process.exit(1);
}

const BLACK = "000000";
const BLUE = "1F4E79";
const LIGHT_BLUE = "D6E4F0";
const RED = "B00020";
const FONT_MAIN = "SimSun";
const FONT_TITLE = "Microsoft YaHei";

const border = { style: BorderStyle.SINGLE, size: 1, color: "A0A0A0" };
const tableBorders = { top: border, bottom: border, left: border, right: border };

function safeText(v, fallback = "-") {
  if (v === null || v === undefined) return fallback;
  const s = String(v).trim();
  return s.length > 0 ? s : fallback;
}

function safeDate(v) {
  if (!v) return "-";
  const s = String(v);
  return s.length >= 10 ? s.slice(0, 10) : s;
}

function formatAmountWan(v) {
  if (typeof v !== "number" || Number.isNaN(v)) return "-";
  return `${(v / 10000).toFixed(2)} 万元`;
}

function p(text, opts = {}) {
  return new Paragraph({
    alignment: opts.center ? AlignmentType.CENTER : AlignmentType.LEFT,
    spacing: { before: opts.before || 0, after: opts.after || 120 },
    children: [
      new TextRun({
        text: safeText(text, ""),
        font: opts.title ? FONT_TITLE : FONT_MAIN,
        size: opts.size || 22,
        bold: !!opts.bold,
        color: opts.color || BLACK,
      }),
    ],
  });
}

function heading(text) {
  return p(text, { bold: true, title: true, size: 30, color: BLUE, before: 220, after: 160 });
}

function cell(text, opts = {}) {
  return new TableCell({
    borders: tableBorders,
    width: opts.width ? { size: opts.width, type: WidthType.DXA } : undefined,
    shading: opts.header ? { fill: LIGHT_BLUE, color: "auto" } : undefined,
    children: [
      new Paragraph({
        alignment: opts.center ? AlignmentType.CENTER : AlignmentType.LEFT,
        spacing: { before: 60, after: 60 },
        children: [
          new TextRun({
            text: safeText(text),
            font: FONT_MAIN,
            size: 20,
            bold: !!opts.header,
            color: opts.color || BLACK,
          }),
        ],
      }),
    ],
  });
}

function makeTable(headers, rows, widths) {
  const headerRow = new TableRow({
    children: headers.map((h, i) => cell(h, { header: true, center: true, width: widths?.[i] })),
  });

  const bodyRows = rows.length
    ? rows.map((r) =>
        new TableRow({
          children: r.map((v, i) => cell(v, { width: widths?.[i] })),
        })
      )
    : [
        new TableRow({
          children: [cell("暂无数据", { center: true })],
        }),
      ];

  return new Table({
    width: { size: 100, type: WidthType.PERCENTAGE },
    rows: [headerRow, ...bodyRows],
  });
}

function sectionCompanyInfo(d) {
  const c = d.company || {};
  const rows = [
    ["企业名称", safeText(c.name)],
    ["统一社会信用代码", safeText(c.unified_code)],
    ["法定代表人", safeText(c.legal_rep)],
    ["技术负责人", safeText(c.tech_director)],
    ["联系电话", safeText(c.phone)],
    ["地址", safeText(c.address)],
    ["成立年份", safeText(c.established_year)],
  ];

  return [
    heading("一、企业基本信息"),
    makeTable(["字段", "内容"], rows, [2600, 6200]),
  ];
}

function sectionCompanyCerts(d) {
  const rows = (d.company_certs || []).map((c) => [
    safeText(c.cert_type),
    safeText(c.cert_no),
    safeText(c.issued_by),
    safeDate(c.valid_from),
    safeDate(c.valid_until),
    safeText(c.status),
    safeText(c.level),
    safeText(c.specialty),
  ]);

  return [
    heading("二、企业资质证书"),
    makeTable(
      ["证书类型", "证书编号", "发证机关", "生效日期", "到期日期", "状态", "等级", "专业"],
      rows,
      [1400, 1500, 1200, 1100, 1100, 900, 900, 1100]
    ),
  ];
}

function sectionPersons(d) {
  const rows = (d.registered_persons || []).map((pItem) => [
    safeText(pItem.name),
    safeText(pItem.cert_type),
    safeText(pItem.cert_no),
    safeDate(pItem.valid_until),
    safeText(pItem.specialty),
    safeText((pItem.recent_projects || []).length),
  ]);

  const elements = [
    heading("三、注册人员"),
    makeTable(["姓名", "证书类型", "证书编号", "到期日期", "专业", "近三年项目数"], rows, [1200, 1400, 1600, 1200, 1200, 1200]),
  ];

  const withProjects = (d.registered_persons || []).filter((it) => (it.recent_projects || []).length > 0);
  for (const person of withProjects) {
    elements.push(p(`- ${safeText(person.name)} (${safeText(person.cert_type)})`, { bold: true, after: 80 }));
    const pRows = (person.recent_projects || []).map((proj) => [
      safeText(proj.project_name),
      safeText(proj.role),
      safeText(proj.year),
      safeText(proj.spu_ref),
    ]);
    elements.push(makeTable(["项目名称", "角色", "年份", "SPU"], pRows, [3000, 1500, 800, 2500]));
  }

  return elements;
}

function sectionProjectRecords(d) {
  const rows = (d.project_records || []).map((r) => [
    safeText(r.project_name),
    safeText(r.contract_no),
    formatAmountWan(r.contract_amount),
    safeText(r.owner_name),
    safeText(r.completed_year),
    safeText(r.project_type),
    safeText(r.proof_utxo_ref, "manual"),
  ]);

  return [
    heading("四、近三年工程业绩"),
    makeTable(
      ["项目名称", "合同编号", "合同额", "业主", "竣工年", "项目类型", "存证引用"],
      rows,
      [2200, 1400, 1200, 1400, 900, 900, 1600]
    ),
  ];
}

function sectionFinance(d) {
  const f = d.finance || {};
  const rows = [
    [safeText(f.year1), formatAmountWan(f.year1_gathering)],
    [safeText(f.year2), formatAmountWan(f.year2_gathering)],
    [safeText(f.year3), formatAmountWan(f.year3_gathering)],
  ].filter((r) => r[0] !== "-");

  return [
    heading("五、近三年财务收款"),
    makeTable(["年份", "收款金额"], rows, [2000, 4000]),
  ];
}

function sectionWarnings(d) {
  const rows = (d.expiry_warnings || []).map((w) => [
    safeText(w.holder_name),
    safeText(w.cert_type),
    safeText(w.cert_no),
    safeDate(w.valid_until),
    `${safeText(w.days_left)} 天`,
  ]);

  return [
    heading("六、证书到期预警（90天内）"),
    makeTable(["持有人", "证书类型", "证书编号", "到期日期", "剩余天数"], rows, [1700, 1500, 1900, 1200, 1000]),
  ];
}

function makeDocument(d) {
  const reportYear = d.report_year || new Date().getFullYear();
  const generatedAt = safeDate(d.generated_at || new Date().toISOString());
  const companyName = safeText(d.company?.name, "CoordOS");

  const children = [
    p(`${reportYear} 年度资质申报报告`, { center: true, bold: true, title: true, size: 44, color: BLUE, before: 800, after: 220 }),
    p(companyName, { center: true, title: true, size: 30, after: 140 }),
    p(`生成日期：${generatedAt}`, { center: true, size: 20, color: "666666", after: 300 }),
    p("本报告由 CoordOS 自动生成，数据来源于业务系统存证记录。", { center: true, size: 18, color: "777777", after: 400 }),
    p("", { after: 300 }),
    ...sectionCompanyInfo(d),
    ...sectionCompanyCerts(d),
    ...sectionPersons(d),
    ...sectionProjectRecords(d),
    ...sectionFinance(d),
    ...sectionWarnings(d),
  ];

  return new Document({
    styles: {
      default: {
        document: {
          run: {
            font: FONT_MAIN,
            size: 21,
            color: BLACK,
          },
        },
      },
    },
    sections: [
      {
        headers: {
          default: new Header({
            children: [
              p(`${companyName} - ${reportYear} 资质申报报告`, { size: 16, color: "666666" }),
            ],
          }),
        },
        footers: {
          default: new Footer({
            children: [
              new Paragraph({
                alignment: AlignmentType.CENTER,
                children: [
                  new TextRun({ text: "第 ", font: FONT_MAIN, size: 16, color: "888888" }),
                  new TextRun({ children: [PageNumber.CURRENT], font: FONT_MAIN, size: 16, color: "888888" }),
                  new TextRun({ text: " / ", font: FONT_MAIN, size: 16, color: "888888" }),
                  new TextRun({ children: [PageNumber.TOTAL_PAGES], font: FONT_MAIN, size: 16, color: "888888" }),
                  new TextRun({ text: " 页", font: FONT_MAIN, size: 16, color: "888888" }),
                ],
              }),
            ],
          }),
        },
        children,
      },
    ],
  });
}

async function main() {
  const doc = makeDocument(data);
  const outBuffer = await Packer.toBuffer(doc);
  fs.writeFileSync(outputPath, outBuffer);

  console.log("Qualification report generated.");
  console.log(`Input : ${path.resolve(inputPath)}`);
  console.log(`Output: ${path.resolve(outputPath)}`);
  console.log(`Company: ${safeText(data.company?.name)}`);
  console.log(`Year   : ${safeText(data.report_year)}`);
  console.log(`Certs  : ${(data.company_certs || []).length}`);
  console.log(`Persons: ${(data.registered_persons || []).length}`);
  console.log(`Projects: ${(data.project_records || []).length}`);
  console.log(`Warnings: ${(data.expiry_warnings || []).length}`);
}

main().catch((err) => {
  console.error("Failed to generate qualification report.");
  console.error(err && err.stack ? err.stack : err);
  process.exit(1);
});

