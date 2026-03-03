#!/usr/bin/env node
// ============================================================
//  scripts/gen_qual_report.js
//  资质申报报告 Word 文件生成器
//
//  用法：
//    node scripts/gen_qual_report.js <data.json> <output.docx>
//
//  data.json 来自：
//    curl "http://localhost:8081/api/v1/reports/qualification?year=2026" > data.json
// ============================================================

"use strict";

const fs = require("fs");
const path = require("path");

const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, HeadingLevel, BorderStyle,
  WidthType, ShadingType, VerticalAlign, PageNumber,
  LevelFormat, PageBreak,
} = require("docx");

// ── 参数 ───────────────────────────────────────────────────────
const dataFile  = process.argv[2] || "qual_report_data.json";
const outFile   = process.argv[3] || "资质申报材料.docx";

if (!fs.existsSync(dataFile)) {
  console.error("找不到数据文件：", dataFile);
  process.exit(1);
}

const D = JSON.parse(fs.readFileSync(dataFile, "utf8"));
const year = D.report_year || new Date().getFullYear();

// ── 样式常量 ──────────────────────────────────────────────────
const PAGE_WIDTH    = 11906; // A4
const PAGE_HEIGHT   = 16838;
const MARGIN        = 1440;  // 2.54cm
const CONTENT_WIDTH = PAGE_WIDTH - MARGIN * 2; // 9026

const BLUE    = "1F4E79";
const LBLUE   = "D6E4F0";
const GRAY    = "F2F2F2";
const BLACK   = "000000";
const RED     = "C00000";

const border1 = { style: BorderStyle.SINGLE, size: 1, color: "AAAAAA" };
const borders  = { top: border1, bottom: border1, left: border1, right: border1 };
const noBorder = { style: BorderStyle.NONE, size: 0, color: "FFFFFF" };
const noBorders = { top: noBorder, bottom: noBorder, left: noBorder, right: noBorder };

// ── 工具函数 ──────────────────────────────────────────────────
const fmt = (n) =>
  typeof n === "number"
    ? (n / 10000).toFixed(1) + "万元"
    : (n || "—");

const cell = (text, opts = {}) =>
  new TableCell({
    borders,
    width: { size: opts.width || 2000, type: WidthType.DXA },
    shading: opts.shade
      ? { fill: opts.shade, type: ShadingType.CLEAR }
      : undefined,
    verticalAlign: VerticalAlign.CENTER,
    margins: { top: 60, bottom: 60, left: 100, right: 100 },
    children: [
      new Paragraph({
        alignment: opts.center ? AlignmentType.CENTER : AlignmentType.LEFT,
        children: [
          new TextRun({
            text: String(text ?? "—"),
            size: opts.size || 18,
            bold: opts.bold || false,
            color: opts.color || BLACK,
            font: "仿宋",
          }),
        ],
      }),
    ],
  });

const headerCell = (text, width) =>
  cell(text, { width, shade: LBLUE, bold: true, center: true, size: 18 });

const h1 = (text) =>
  new Paragraph({
    heading: HeadingLevel.HEADING_1,
    children: [new TextRun({ text, font: "黑体", size: 32, bold: true, color: BLUE })],
    spacing: { before: 360, after: 180 },
    border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: BLUE, space: 4 } },
  });

const h2 = (text) =>
  new Paragraph({
    heading: HeadingLevel.HEADING_2,
    children: [new TextRun({ text: "▌ " + text, font: "黑体", size: 24, bold: true, color: BLUE })],
    spacing: { before: 240, after: 120 },
  });

const body = (text, opts = {}) =>
  new Paragraph({
    alignment: opts.center ? AlignmentType.CENTER : AlignmentType.LEFT,
    spacing: { after: 80 },
    children: [
      new TextRun({
        text,
        font: "仿宋",
        size: opts.size || 20,
        bold: opts.bold || false,
        color: opts.color || BLACK,
      }),
    ],
  });

const warn = (text) =>
  new Paragraph({
    spacing: { after: 80 },
    children: [
      new TextRun({ text: "⚠ " + text, font: "仿宋", size: 18, color: RED }),
    ],
  });

const blank = (n = 1) =>
  Array.from({ length: n }, () =>
    new Paragraph({ children: [new TextRun("")], spacing: { after: 60 } })
  );

const pageBreak = () =>
  new Paragraph({ children: [new PageBreak()] });

// ── 封面 ──────────────────────────────────────────────────────
function makeCover() {
  const co = D.company || {};
  return [
    ...blank(8),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new TextRun({
        text: `${year} 年度工程设计资质申报材料`,
        font: "黑体", size: 52, bold: true, color: BLUE,
      })],
      spacing: { after: 200 },
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new TextRun({
        text: co.name || "（企业名称）",
        font: "黑体", size: 36, color: BLACK,
      })],
      spacing: { after: 120 },
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new TextRun({
        text: `生成日期：${D.generated_at ? D.generated_at.slice(0,10) : new Date().toISOString().slice(0,10)}`,
        font: "仿宋", size: 22, color: "666666",
      })],
    }),
    ...blank(4),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new TextRun({
        text: "本材料由 CoordOS 系统自动生成，数据来源于业务运行过程中的存证记录",
        font: "仿宋", size: 18, color: "888888",
      })],
    }),
    pageBreak(),
  ];
}

// ── 一、企业基本信息 ───────────────────────────────────────────
function makeCompanyInfo() {
  const co = D.company || {};
  const fields = [
    ["企业名称",   co.name        || "—"],
    ["法定代表人", co.legal_rep   || "—"],
    ["技术负责人", co.tech_director || "—"],
    ["联系电话",   co.phone       || "—"],
    ["统一社会信用代码", co.unified_code || "—"],
    ["注册地址",   co.address     || "—"],
  ];

  const rows = fields.map(([label, value]) =>
    new TableRow({
      children: [
        headerCell(label, 2400),
        cell(value, { width: CONTENT_WIDTH - 2400 }),
      ],
    })
  );

  return [
    h1("一、企业基本信息"),
    new Table({
      width: { size: CONTENT_WIDTH, type: WidthType.DXA },
      columnWidths: [2400, CONTENT_WIDTH - 2400],
      rows,
    }),
    ...blank(),
  ];
}

// ── 二、企业资质证书 ───────────────────────────────────────────
function makeCompanyCerts() {
  const certs = D.company_certs || [];
  const colW = [2200, 2200, 1400, 1400, 1000, 826];
  const header = new TableRow({
    children: [
      headerCell("资质类型",   colW[0]),
      headerCell("证书编号",   colW[1]),
      headerCell("发证机关",   colW[2]),
      headerCell("有效期至",   colW[3]),
      headerCell("状态",       colW[4]),
      headerCell("专业",       colW[5]),
    ],
  });

  const rows = certs.length > 0
    ? certs.map(c => new TableRow({
        children: [
          cell(c.cert_type,   { width: colW[0] }),
          cell(c.cert_no,     { width: colW[1] }),
          cell(c.issued_by,   { width: colW[2] }),
          cell(c.valid_until, { width: colW[3] }),
          cell(c.status,      { width: colW[4], color: c.status === "VALID" ? "00AA00" : RED }),
          cell(c.specialty,   { width: colW[5] }),
        ],
      }))
    : [new TableRow({ children: [
        new TableCell({
          columnSpan: 6, borders, width: { size: CONTENT_WIDTH, type: WidthType.DXA },
          children: [body("暂无企业资质记录", { center: true })],
        }),
      ]})];

  return [
    h1("二、企业资质证书"),
    new Table({
      width: { size: CONTENT_WIDTH, type: WidthType.DXA },
      columnWidths: colW,
      rows: [header, ...rows],
    }),
    ...blank(),
  ];
}

// ── 三、注册人员名单 ───────────────────────────────────────────
function makePersonnel() {
  const persons = D.registered_persons || [];
  const sections = [h1("三、注册人员名单")];

  if (persons.length === 0) {
    sections.push(body("暂无注册人员记录"));
    return sections;
  }

  const colW = [1200, 2400, 2000, 1800, 1626];
  const header = new TableRow({
    children: [
      headerCell("序号",     colW[0]),
      headerCell("姓名",     colW[1]),
      headerCell("证书类型", colW[2]),
      headerCell("证书编号", colW[3]),
      headerCell("有效期至", colW[4]),
    ],
  });

  const rows = persons.map((p, i) =>
    new TableRow({
      children: [
        cell(String(i + 1),    { width: colW[0], center: true }),
        cell(p.name,           { width: colW[1] }),
        cell(p.cert_type,      { width: colW[2] }),
        cell(p.cert_no,        { width: colW[3] }),
        cell(p.valid_until,    { width: colW[4] }),
      ],
    })
  );

  sections.push(
    new Table({
      width: { size: CONTENT_WIDTH, type: WidthType.DXA },
      columnWidths: colW,
      rows: [header, ...rows],
    })
  );

  // 近三年执业记录（展开有记录的人员）
  const withProjects = persons.filter(p => p.recent_projects && p.recent_projects.length > 0);
  if (withProjects.length > 0) {
    sections.push(...blank(), h2("3.1 近三年执业项目记录"));
    for (const p of withProjects) {
      sections.push(body(`${p.name}（${p.cert_type}）`, { bold: true }));
      const pColW = [600, 3600, 2000, 826];
      const pHeader = new TableRow({
        children: [
          headerCell("序",    pColW[0]),
          headerCell("项目名称", pColW[1]),
          headerCell("角色",  pColW[2]),
          headerCell("年度",  pColW[3]),
        ],
      });
      const pRows = p.recent_projects.map((proj, j) =>
        new TableRow({
          children: [
            cell(String(j+1),        { width: pColW[0], center: true }),
            cell(proj.project_name,  { width: pColW[1] }),
            cell(proj.role,          { width: pColW[2] }),
            cell(String(proj.year),  { width: pColW[3], center: true }),
          ],
        })
      );
      sections.push(
        new Table({
          width: { size: CONTENT_WIDTH, type: WidthType.DXA },
          columnWidths: pColW,
          rows: [pHeader, ...pRows],
        }),
        ...blank(1)
      );
    }
  }

  sections.push(...blank());
  return sections;
}

// ── 四、近三年工程业绩 ─────────────────────────────────────────
function makeProjects() {
  const projs = D.project_records || [];
  const sections = [h1("四、近三年工程业绩")];

  if (projs.length === 0) {
    sections.push(body("暂无符合条件的工程业绩记录（合同额≥50万）"));
    return sections;
  }

  const colW = [600, 2600, 1600, 1400, 1000, 600, 1226];
  const header = new TableRow({
    children: [
      headerCell("序",         colW[0]),
      headerCell("项目名称",   colW[1]),
      headerCell("合同额",     colW[2]),
      headerCell("委托方",     colW[3]),
      headerCell("项目类型",   colW[4]),
      headerCell("竣工年",     colW[5]),
      headerCell("存证引用",   colW[6]),
    ],
  });

  const rows = projs.map((p, i) =>
    new TableRow({
      children: [
        cell(String(i+1),      { width: colW[0], center: true }),
        cell(p.project_name,   { width: colW[1] }),
        cell(fmt(p.contract_amount), { width: colW[2], center: true }),
        cell(p.owner_name,     { width: colW[3] }),
        cell(p.project_type,   { width: colW[4], center: true }),
        cell(String(p.completed_year || "—"), { width: colW[5], center: true }),
        cell(p.proof_utxo_ref ? "有存证" : "手动录入",
          { width: colW[6], center: true, color: p.proof_utxo_ref ? "00AA00" : "888888" }),
      ],
    })
  );

  sections.push(
    new Table({
      width: { size: CONTENT_WIDTH, type: WidthType.DXA },
      columnWidths: colW,
      rows: [header, ...rows],
    }),
    ...blank()
  );
  return sections;
}

// ── 五、财务状况 ───────────────────────────────────────────────
function makeFinance() {
  const f = D.finance || {};
  const years = [
    [f.year1, f.year1_gathering],
    [f.year2, f.year2_gathering],
    [f.year3, f.year3_gathering],
  ].filter(([y]) => y);

  const colW = [2000, CONTENT_WIDTH - 2000];
  const rows = years.map(([yr, amt]) =>
    new TableRow({
      children: [
        headerCell(`${yr} 年`, colW[0]),
        cell(fmt(amt) + "（已到账收款合计）", { width: colW[1] }),
      ],
    })
  );

  return [
    h1("五、财务状况"),
    rows.length > 0
      ? new Table({
          width: { size: CONTENT_WIDTH, type: WidthType.DXA },
          columnWidths: colW,
          rows,
        })
      : body("暂无财务数据"),
    ...blank(),
  ];
}

// ── 六、到期预警 ───────────────────────────────────────────────
function makeWarnings() {
  const ws = D.expiry_warnings || [];
  const sections = [h1("六、证书到期预警（90天内）")];

  if (ws.length === 0) {
    sections.push(body("✓ 当前无证书在90天内到期", { color: "00AA00" }));
    return [...sections, ...blank()];
  }

  const colW = [2000, 2200, 2000, 1600, 1226];
  const header = new TableRow({
    children: [
      headerCell("持有人",   colW[0]),
      headerCell("证书类型", colW[1]),
      headerCell("证书编号", colW[2]),
      headerCell("到期日",   colW[3]),
      headerCell("剩余天数", colW[4]),
    ],
  });

  const rows = ws.map(w =>
    new TableRow({
      children: [
        cell(w.holder_name,  { width: colW[0] }),
        cell(w.cert_type,    { width: colW[1] }),
        cell(w.cert_no,      { width: colW[2] }),
        cell(w.valid_until,  { width: colW[3] }),
        cell(String(w.days_left) + " 天",
          { width: colW[4], center: true, color: w.days_left <= 30 ? RED : "DD7700" }),
      ],
    })
  );

  sections.push(
    new Table({
      width: { size: CONTENT_WIDTH, type: WidthType.DXA },
      columnWidths: colW,
      rows: [header, ...rows],
    }),
    ...blank()
  );
  return sections;
}

// ── 页眉页脚 ───────────────────────────────────────────────────
function makeHeader() {
  const co = D.company || {};
  return new Header({
    children: [
      new Paragraph({
        alignment: AlignmentType.RIGHT,
        border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: BLUE, space: 4 } },
        children: [
          new TextRun({ text: (co.name || "企业") + `  ${year}年度工程设计资质申报材料`, font: "仿宋", size: 16, color: "666666" }),
        ],
      }),
    ],
  });
}

function makeFooter() {
  return new Footer({
    children: [
      new Paragraph({
        alignment: AlignmentType.CENTER,
        border: { top: { style: BorderStyle.SINGLE, size: 2, color: "CCCCCC", space: 4 } },
        children: [
          new TextRun({ text: "第 ", font: "仿宋", size: 16, color: "888888" }),
          new TextRun({ children: [PageNumber.CURRENT], font: "仿宋", size: 16, color: "888888" }),
          new TextRun({ text: " 页 / 共 ", font: "仿宋", size: 16, color: "888888" }),
          new TextRun({ children: [PageNumber.TOTAL_PAGES], font: "仿宋", size: 16, color: "888888" }),
          new TextRun({ text: " 页    CoordOS 自动生成", font: "仿宋", size: 16, color: "888888" }),
        ],
      }),
    ],
  });
}

// ── 组装文档 ──────────────────────────────────────────────────
const children = [
  ...makeCover(),
  ...makeCompanyInfo(),
  ...makeCompanyCerts(),
  ...makePersonnel(),
  ...makeProjects(),
  ...makeFinance(),
  ...makeWarnings(),
];

const doc = new Document({
  styles: {
    default: {
      document: { run: { font: "仿宋", size: 20, color: BLACK } },
    },
    paragraphStyles: [
      {
        id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 32, bold: true, font: "黑体", color: BLUE },
        paragraph: { spacing: { before: 360, after: 180 }, outlineLevel: 0 },
      },
      {
        id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 24, bold: true, font: "黑体", color: BLUE },
        paragraph: { spacing: { before: 240, after: 120 }, outlineLevel: 1 },
      },
    ],
  },
  sections: [{
    properties: {
      page: {
        size: { width: PAGE_WIDTH, height: PAGE_HEIGHT },
        margin: { top: MARGIN, right: MARGIN, bottom: MARGIN, left: MARGIN },
      },
    },
    headers: { default: makeHeader() },
    footers: { default: makeFooter() },
    children,
  }],
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync(outFile, buf);
  console.log(`✓ 生成完成：${outFile}`);
  console.log(`  企业：${D.company?.name || "—"}`);
  console.log(`  年度：${year}`);
  console.log(`  企业资质：${(D.company_certs || []).length} 条`);
  console.log(`  注册人员：${(D.registered_persons || []).length} 人`);
  console.log(`  工程业绩：${(D.project_records || []).length} 条`);
  console.log(`  到期预警：${(D.expiry_warnings || []).length} 条`);
}).catch(err => {
  console.error("生成失败：", err.message);
  process.exit(1);
});
