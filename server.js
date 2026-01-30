const path = require('path');
const fs = require('fs');
const express = require('express');
const PDFDocument = require('pdfkit');
const QRCode = require('qrcode');
const multer = require('multer');
const XLSX = require('xlsx');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 5 * 1024 * 1024 } });

const PAPER_PRESETS = {
  A4: { widthMm: 210, heightMm: 297 },
  Letter: { widthMm: 215.9, heightMm: 279.4 },
  Label3x5: { widthMm: 76.2, heightMm: 127 }
};

const DEFAULT_LAYOUT = {
  labelWidthMm: 60,
  labelHeightMm: 30,
  marginsMm: { top: 8, right: 8, bottom: 8, left: 8 },
  gapsMm: { x: 2, y: 2 },
  safeMarginMm: 6,
  layoutMode: 'sheet',
  includeBleed: false
};

const DEFAULT_QR = {
  mode: 'id',
  baseUrl: 'https://fgn.app'
};

const STRUCTURES = {
  S1: { code: 'S1', hasRows: false, hasPlants: true }, // Plot + Plant
  S2: { code: 'S2', hasRows: true, hasPlants: false }, // Plot + Row
  S3: { code: 'S3', hasRows: true, hasPlants: true }, // Plot + Row + Plant
  S4: { code: 'S4', hasRows: false, hasPlants: false } // Plot only
};

const MODES = {
  STANDARD: 'Standard',
  RESEARCH: 'Research',
  FULL: 'Full'
};

const DATA_DIR = path.join(__dirname, 'data');
const SAMPLING_FILE = path.join(DATA_DIR, 'sampling-plans.json');
const IMPORTS_FILE = path.join(DATA_DIR, 'import-history.json');
const CURRENT_IMPORT_FILE = path.join(DATA_DIR, 'current-import.json');

function mmToPt(mm) {
  return (mm * 72) / 25.4;
}

function pad(num, len) {
  return String(num).padStart(len, '0');
}

function normalizeCode(text) {
  return text
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function parsePositiveInt(value) {
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) return null;
  return num;
}

function numberOrDefault(value, fallback) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function loadSamplingPlans() {
  try {
    if (!fs.existsSync(SAMPLING_FILE)) return [];
    const raw = fs.readFileSync(SAMPLING_FILE, 'utf-8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    return [];
  }
}

function saveSamplingPlan(plan) {
  ensureDataDir();
  const plans = loadSamplingPlans();
  plans.push(plan);
  fs.writeFileSync(SAMPLING_FILE, JSON.stringify(plans, null, 2));
}

function loadImportHistory() {
  try {
    if (!fs.existsSync(IMPORTS_FILE)) return [];
    const raw = fs.readFileSync(IMPORTS_FILE, 'utf-8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    return [];
  }
}

function saveImportHistory(record) {
  ensureDataDir();
  const history = loadImportHistory();
  history.unshift(record);
  const trimmed = history.slice(0, 5);
  fs.writeFileSync(IMPORTS_FILE, JSON.stringify(trimmed, null, 2));
}

function saveCurrentImport(data) {
  ensureDataDir();
  fs.writeFileSync(CURRENT_IMPORT_FILE, JSON.stringify(data, null, 2));
}

function getStructure(code) {
  return STRUCTURES[code] || STRUCTURES.S3;
}

function hasPlantTracking(structureCode, mode) {
  const structure = getStructure(structureCode);
  if (!structure.hasPlants) return false;
  return mode === MODES.RESEARCH || mode === MODES.FULL;
}

function buildPlantIdShort(plotNo, rowNo, plantNo) {
  return `P${pad(plotNo, 2)}-R${pad(rowNo, 2)}-T${pad(plantNo, 3)}`;
}

function buildPlotIdShort(plotNo) {
  return `P${pad(plotNo, 2)}`;
}

function buildRowIdShort(plotNo, rowNo) {
  return `P${pad(plotNo, 2)}-R${pad(rowNo, 2)}`;
}

function buildPlantIdShortFlexible(structureCode, plotNo, rowNo, plantNo) {
  if (structureCode === 'S1') {
    return `P${pad(plotNo, 2)}-T${pad(plantNo, 3)}`;
  }
  return buildPlantIdShort(plotNo, rowNo, plantNo);
}

function buildPlantIdFull(siteName, cropType, plotNo, rowNo, plantNo) {
  const siteCode = normalizeCode(siteName);
  const cropCode = normalizeCode(cropType);
  return `${siteCode}-${cropCode}-${buildPlantIdShort(plotNo, rowNo, plantNo)}`;
}

function buildPlantIdFullFlexible(siteName, cropType, structureCode, plotNo, rowNo, plantNo) {
  const siteCode = normalizeCode(siteName);
  const cropCode = normalizeCode(cropType);
  return `${siteCode}-${cropCode}-${buildPlantIdShortFlexible(structureCode, plotNo, rowNo, plantNo)}`;
}

function buildPlotIdFull(siteName, cropType, plotNo) {
  const siteCode = normalizeCode(siteName);
  const cropCode = normalizeCode(cropType);
  return `${siteCode}-${cropCode}-${buildPlotIdShort(plotNo)}`;
}

function buildRowIdFull(siteName, cropType, plotNo, rowNo) {
  const siteCode = normalizeCode(siteName);
  const cropCode = normalizeCode(cropType);
  return `${siteCode}-${cropCode}-${buildRowIdShort(plotNo, rowNo)}`;
}

function parsePlots(body, structureCode) {
  const structure = getStructure(structureCode);
  if (Array.isArray(body.plots) && body.plots.length) {
    return body.plots.map((plot, plotIndex) => {
      const plotNo = parsePositiveInt(plot.plot_no ?? plot.plotNo ?? plot.plot) || plotIndex + 1;
      if (!structure.hasRows && structure.hasPlants) {
        const plantCount = parsePositiveInt(plot.plant_count ?? plot.plantCount ?? plot.plants);
        return { plotNo, plantCount };
      }
      if (!structure.hasRows && !structure.hasPlants) {
        return { plotNo };
      }
      const rows = Array.isArray(plot.rows) ? plot.rows : [];
      const normalizedRows = rows.map((row, rowIndex) => {
        const rowNo = parsePositiveInt(row.row_no ?? row.rowNo ?? row.row) || rowIndex + 1;
        const plantCount = parsePositiveInt(row.plant_count ?? row.plantCount ?? row.plants);
        return { rowNo, plantCount };
      });
      return { plotNo, rows: normalizedRows };
    });
  }

  const plotCount = parsePositiveInt(body.plotsCount ?? body.plots);
  if (!plotCount) return [];

  const rowsPerPlot = parsePositiveInt(body.rowsPerPlot);
  const plantsPerRow = parsePositiveInt(body.plantsPerRow);
  const plantsPerPlot = parsePositiveInt(body.plantsPerPlot ?? body.plantCountPerPlot);

  if (!structure.hasRows && structure.hasPlants && plantsPerPlot) {
    return Array.from({ length: plotCount }, (_, index) => ({
      plotNo: index + 1,
      plantCount: plantsPerPlot
    }));
  }

  if (!structure.hasRows && !structure.hasPlants) {
    return Array.from({ length: plotCount }, (_, index) => ({ plotNo: index + 1 }));
  }

  if (rowsPerPlot && plantsPerRow) {
    const plots = [];
    for (let p = 1; p <= plotCount; p += 1) {
      const rows = [];
      for (let r = 1; r <= rowsPerPlot; r += 1) {
        rows.push({ rowNo: r, plantCount: plantsPerRow });
      }
      plots.push({ plotNo: p, rows });
    }
    return plots;
  }

  return [];
}

function validatePayload(body) {
  const errors = [];
  const siteName = String(body.siteName || '').trim();
  const cropType = String(body.cropType || '').trim();
  const batchName = String(body.batchName || '').trim();
  const startDate = String(body.startDate || '').trim();
  const structureCode = String(body.structureCode || 'S3').toUpperCase();
  const mode = String(body.mode || MODES.STANDARD);
  const structure = getStructure(structureCode);

  if (!siteName) errors.push('Site name is required.');
  if (!cropType) errors.push('Crop type is required.');
  const plots = parsePlots(body, structureCode);
  if (!plots.length) errors.push('At least one plot is required.');
  if (!STRUCTURES[structureCode]) errors.push('Tracking structure is invalid.');
  if (![MODES.STANDARD, MODES.RESEARCH, MODES.FULL].includes(mode)) {
    errors.push('Tracking mode is invalid.');
  }
  if ((mode === MODES.RESEARCH || mode === MODES.FULL) && !structure.hasPlants) {
    errors.push('Selected tracking mode requires plant tracking.');
  }

  let totalRows = 0;
  let totalPlants = 0;
  plots.forEach((plot) => {
    if (structure.hasRows) {
      if (!plot.rows || plot.rows.length === 0) {
        errors.push(`Plot ${plot.plotNo} must have at least 1 row.`);
        return;
      }
      totalRows += plot.rows.length;
      plot.rows.forEach((row) => {
        if (!row.plantCount || row.plantCount < 1) {
          errors.push(`Plot ${plot.plotNo} Row ${row.rowNo} must have at least 1 plant.`);
        } else {
          totalPlants += row.plantCount;
        }
      });
      return;
    }

    if (structure.hasPlants) {
      if (!plot.plantCount || plot.plantCount < 1) {
        errors.push(`Plot ${plot.plotNo} must have at least 1 plant.`);
      } else {
        totalPlants += plot.plantCount;
      }
    }
  });

  const totalLabels = totalPlants;

  const warnings = [];
  if (totalLabels > 10000) {
    warnings.push('Total labels exceed 10,000. Consider exporting plot-wise.');
  }
  if (totalLabels > 100000) {
    errors.push('Total labels exceed the safe limit of 100,000.');
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    values: {
      siteName,
      cropType,
      batchName,
      startDate,
      structureCode,
      mode,
      plots,
      totalPlots: plots.length,
      totalRows,
      totalPlants,
      totalLabels,
      labels: {
        plot: plots.length,
        row: structure.hasRows ? totalRows : 0,
        plant: hasPlantTracking(structureCode, mode) ? totalPlants : 0
      }
    }
  };
}

function* iterateLabels({
  siteName,
  cropType,
  structureCode,
  plots,
  exportPlot,
  trackedPlants
}) {
  if (Array.isArray(trackedPlants) && trackedPlants.length) {
    const sortedTracked = [...trackedPlants].sort((a, b) => {
      const ap = parsePositiveInt(a.plot_no ?? a.plotNo ?? a.plot) || 0;
      const bp = parsePositiveInt(b.plot_no ?? b.plotNo ?? b.plot) || 0;
      if (ap !== bp) return ap - bp;
      const ar = parsePositiveInt(a.row_no ?? a.rowNo ?? a.row) || 0;
      const br = parsePositiveInt(b.row_no ?? b.rowNo ?? b.row) || 0;
      if (ar !== br) return ar - br;
      const at = parsePositiveInt(a.plant_no ?? a.plantNo ?? a.plant) || 0;
      const bt = parsePositiveInt(b.plant_no ?? b.plantNo ?? b.plant) || 0;
      return at - bt;
    });
    for (const tracked of sortedTracked) {
      const plotNo = parsePositiveInt(tracked.plot_no ?? tracked.plotNo ?? tracked.plot);
      const rowNo = parsePositiveInt(tracked.row_no ?? tracked.rowNo ?? tracked.row);
      const plantNo = parsePositiveInt(tracked.plant_no ?? tracked.plantNo ?? tracked.plant);
      if (!plotNo || !plantNo) continue;
      if (exportPlot && exportPlot !== plotNo) continue;
      const plantIdShort = buildPlantIdShortFlexible(structureCode, plotNo, rowNo, plantNo);
      const plantIdFull = buildPlantIdFullFlexible(siteName, cropType, structureCode, plotNo, rowNo, plantNo);
      yield {
        siteName,
        cropType,
        plotNo,
        rowNo: rowNo || null,
        plantNo,
        plantIdShort,
        plantIdFull
      };
    }
    return;
  }

  for (const plot of plots) {
    const plotNo = plot.plotNo;
    if (exportPlot && exportPlot !== plotNo) continue;

    if (structureCode === 'S1') {
      const plantCount = plot.plantCount || 0;
      for (let t = 1; t <= plantCount; t += 1) {
        const plantIdShort = buildPlantIdShortFlexible(structureCode, plotNo, null, t);
        const plantIdFull = buildPlantIdFullFlexible(siteName, cropType, structureCode, plotNo, null, t);
        yield {
          siteName,
          cropType,
          plotNo,
          rowNo: null,
          plantNo: t,
          plantIdShort,
          plantIdFull
        };
      }
      continue;
    }

    const rows = Array.isArray(plot.rows) ? plot.rows : [];
    for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
      const row = rows[rowIndex];
      const rowNo = row.rowNo || rowIndex + 1;
      const plantCount = row.plantCount || 0;
      for (let t = 1; t <= plantCount; t += 1) {
        const plantIdShort = buildPlantIdShortFlexible(structureCode, plotNo, rowNo, t);
        const plantIdFull = buildPlantIdFullFlexible(siteName, cropType, structureCode, plotNo, rowNo, t);
        yield {
          siteName,
          cropType,
          plotNo,
          rowNo,
          plantNo: t,
          plantIdShort,
          plantIdFull
        };
      }
    }
  }
}

function* iteratePlots({ siteName, cropType, plots, exportPlot }) {
  for (const plot of plots) {
    const plotNo = plot.plotNo;
    if (exportPlot && exportPlot !== plotNo) continue;
    const rows = Array.isArray(plot.rows) ? plot.rows : [];
    yield {
      siteName,
      cropType,
      plotNo,
      rowCount: rows.length,
      plotIdShort: buildPlotIdShort(plotNo),
      plotIdFull: buildPlotIdFull(siteName, cropType, plotNo)
    };
  }
}

function* iterateRows({ siteName, cropType, structureCode, plots, exportPlot }) {
  const structure = getStructure(structureCode);
  if (!structure.hasRows) return;

  for (const plot of plots) {
    const plotNo = plot.plotNo;
    if (exportPlot && exportPlot !== plotNo) continue;
    const rows = Array.isArray(plot.rows) ? plot.rows : [];
    for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
      const row = rows[rowIndex];
      const rowNo = row.rowNo || rowIndex + 1;
      const plantCount = row.plantCount || 0;
      yield {
        siteName,
        cropType,
        plotNo,
        rowNo,
        plantCount,
        rowIdShort: buildRowIdShort(plotNo, rowNo),
        rowIdFull: buildRowIdFull(siteName, cropType, plotNo, rowNo)
      };
    }
  }
}

function getLayoutOptions(body) {
  const preset = PAPER_PRESETS[body.paperPreset] || PAPER_PRESETS.A4;
  const requestedMode = body.layoutMode || DEFAULT_LAYOUT.layoutMode;
  const layoutMode = body.paperPreset === 'Label3x5' ? 'single' : requestedMode;
  const labelWidthMm = Number.isFinite(Number(body.labelWidthMm))
    ? Number(body.labelWidthMm)
    : DEFAULT_LAYOUT.labelWidthMm;
  const labelHeightMm = Number.isFinite(Number(body.labelHeightMm))
    ? Number(body.labelHeightMm)
    : DEFAULT_LAYOUT.labelHeightMm;
  const marginsMm = {
    top: numberOrDefault(body.marginsMm?.top, DEFAULT_LAYOUT.marginsMm.top),
    right: numberOrDefault(body.marginsMm?.right, DEFAULT_LAYOUT.marginsMm.right),
    bottom: numberOrDefault(body.marginsMm?.bottom, DEFAULT_LAYOUT.marginsMm.bottom),
    left: numberOrDefault(body.marginsMm?.left, DEFAULT_LAYOUT.marginsMm.left)
  };
  const gapsMm = {
    x: numberOrDefault(body.gapsMm?.x, DEFAULT_LAYOUT.gapsMm.x),
    y: numberOrDefault(body.gapsMm?.y, DEFAULT_LAYOUT.gapsMm.y)
  };
  const safeMarginMm = numberOrDefault(body.safeMarginMm, DEFAULT_LAYOUT.safeMarginMm);
  const includeBleed = Boolean(body.includeBleed);

  return {
    preset,
    layoutMode,
    labelWidthMm,
    labelHeightMm,
    marginsMm,
    gapsMm,
    safeMarginMm,
    includeBleed
  };
}

function formatMonthYear(date) {
  return date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
}

function drawDivider(doc, x1, x2, y) {
  doc.save();
  doc.strokeColor('#B0B0B0').lineWidth(0.5);
  doc.moveTo(x1, y).lineTo(x2, y).stroke();
  doc.restore();
}

function getQrPayload(fullId, body) {
  const mode = body.qrMode || DEFAULT_QR.mode;
  if (mode === 'url') {
    const baseUrl = String(body.qrBaseUrl || DEFAULT_QR.baseUrl).replace(/\/+$/, '');
    return `${baseUrl}/farm/${fullId}`;
  }
  return fullId;
}

function hashSeed(input) {
  const str = String(input);
  let hash = 2166136261;
  for (let i = 0; i < str.length; i += 1) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function createRng(seed) {
  let state = hashSeed(seed || Date.now().toString());
  return function rng() {
    state += 0x6D2B79F5;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function sampleUnique(rangeMax, count, rng) {
  if (count > rangeMax) {
    throw new Error('Sample size exceeds population.');
  }
  const arr = Array.from({ length: rangeMax }, (_, index) => index + 1);
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rng() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr.slice(0, count);
}

function parseRangeList(input, maxValue) {
  if (!input) return [];
  const cleaned = String(input).replace(/\\s+/g, '');
  if (!cleaned) return [];
  const segments = cleaned.split(',');
  const values = new Set();
  segments.forEach((segment) => {
    if (!segment) return;
    const parts = segment.split('-');
    if (parts.length === 1) {
      const num = parsePositiveInt(parts[0]);
      if (num && (!maxValue || num <= maxValue)) values.add(num);
      return;
    }
    const start = parsePositiveInt(parts[0]);
    const end = parsePositiveInt(parts[1]);
    if (!start || !end) return;
    const min = Math.min(start, end);
    const max = Math.max(start, end);
    for (let i = min; i <= max; i += 1) {
      if (!maxValue || i <= maxValue) values.add(i);
    }
  });
  return Array.from(values).sort((a, b) => a - b);
}

function buildSamplingPlan(values, planConfig) {
  const structure = getStructure(values.structureCode);
  const samplingType = planConfig.samplingType;
  const seed = planConfig.seed || Date.now().toString();
  const rng = createRng(seed);
  const trackedPlants = [];

  if (!structure.hasPlants) {
    throw new Error('Sampling requires plant tracking.');
  }

  if (samplingType === 'plot_based') {
    const samplesPerPlot = parsePositiveInt(planConfig.samplesPerPlot);
    if (!samplesPerPlot) throw new Error('Samples per plot is required.');
    const selectedPlots = parseRangeList(planConfig.plots);

    values.plots.forEach((plot) => {
      const plotNo = plot.plotNo;
      if (selectedPlots.length && !selectedPlots.includes(plotNo)) return;
      if (structure.code === 'S1') {
        const plantCount = plot.plantCount || 0;
        const picks = sampleUnique(plantCount, samplesPerPlot, rng);
        picks.forEach((plantNo) => {
          trackedPlants.push({ plot_no: plotNo, plant_no: plantNo });
        });
        return;
      }

      const rows = Array.isArray(plot.rows) ? plot.rows : [];
      const pool = [];
      rows.forEach((row, rowIndex) => {
        const rowNo = row.rowNo || rowIndex + 1;
        const plantCount = row.plantCount || 0;
        for (let t = 1; t <= plantCount; t += 1) {
          pool.push({ plot_no: plotNo, row_no: rowNo, plant_no: t });
        }
      });
      if (samplesPerPlot > pool.length) {
        throw new Error(`Plot ${plotNo} has fewer plants than requested samples.`);
      }
      const picks = sampleUnique(pool.length, samplesPerPlot, rng);
      picks.forEach((index) => {
        trackedPlants.push(pool[index - 1]);
      });
    });
  } else if (samplingType === 'row_based') {
    if (!structure.hasRows) {
      throw new Error('Row-based sampling requires rows.');
    }
    const samplesPerRow = parsePositiveInt(planConfig.samplesPerRow);
    if (!samplesPerRow) throw new Error('Samples per row is required.');
    const selectedPlots = parseRangeList(planConfig.plots);
    const selectedRows = parseRangeList(planConfig.rows);

    values.plots.forEach((plot) => {
      const plotNo = plot.plotNo;
      if (selectedPlots.length && !selectedPlots.includes(plotNo)) return;
      const rows = Array.isArray(plot.rows) ? plot.rows : [];
      rows.forEach((row, rowIndex) => {
        const rowNo = row.rowNo || rowIndex + 1;
        if (selectedRows.length && !selectedRows.includes(rowNo)) return;
        const plantCount = row.plantCount || 0;
        const picks = sampleUnique(plantCount, samplesPerRow, rng);
        picks.forEach((plantNo) => {
          trackedPlants.push({ plot_no: plotNo, row_no: rowNo, plant_no: plantNo });
        });
      });
    });
  } else {
    throw new Error('Sampling type is invalid.');
  }

  return { seed, trackedPlants, samplingType };
}

function normalizeHeader(header) {
  return String(header || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_');
}

function parseExcelRows(buffer) {
  const workbook = XLSX.read(buffer, { type: 'buffer' });
  const sheetNames = workbook.SheetNames || [];
  const targetName = sheetNames.find((name) => normalizeHeader(name) === 'rows');
  if (!targetName) {
    return { ok: false, errors: ['Missing required sheet: ROWS.'] };
  }

  const sheet = workbook.Sheets[targetName];
  const rawRows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
  if (!rawRows.length) {
    return { ok: false, errors: ['ROWS sheet is empty.'] };
  }

  const required = ['site_name', 'crop_type', 'plot_no', 'row_no', 'plant_count'];
  const errors = [];
  const mappedRows = [];
  const siteValues = new Set();
  const cropValues = new Set();
  const seenPairs = new Set();

  rawRows.forEach((row, index) => {
    const normalized = {};
    Object.entries(row).forEach(([key, value]) => {
      normalized[normalizeHeader(key)] = value;
    });

    const missing = required.filter((col) => normalized[col] === '' || normalized[col] === null || normalized[col] === undefined);
    if (missing.length) {
      errors.push(`Row ${index + 2}: Missing ${missing.join(', ')}.`);
      return;
    }

    const plotNo = parsePositiveInt(normalized.plot_no);
    const rowNo = parsePositiveInt(normalized.row_no);
    const plantCount = parsePositiveInt(normalized.plant_count);
    if (!plotNo) {
      errors.push(`Row ${index + 2}: plot_no must be a positive integer.`);
      return;
    }
    if (!rowNo) {
      errors.push(`Row ${index + 2}: row_no must be a positive integer.`);
      return;
    }
    if (!plantCount) {
      errors.push(`Row ${index + 2}: plant_count must be a positive integer.`);
      return;
    }

    const siteName = String(normalized.site_name).trim();
    const cropType = String(normalized.crop_type).trim();
    if (!siteName) {
      errors.push(`Row ${index + 2}: site_name is required.`);
      return;
    }
    if (!cropType) {
      errors.push(`Row ${index + 2}: crop_type is required.`);
      return;
    }

    const pairKey = `${plotNo}-${rowNo}`;
    if (seenPairs.has(pairKey)) {
      errors.push(`Row ${index + 2}: Duplicate plot_no + row_no (${plotNo}, ${rowNo}).`);
      return;
    }
    seenPairs.add(pairKey);
    siteValues.add(siteName);
    cropValues.add(cropType);
    mappedRows.push({ siteName, cropType, plotNo, rowNo, plantCount });
  });

  if (siteValues.size > 1) {
    errors.push('Multiple site_name values detected. Use a single site_name per import.');
  }
  if (cropValues.size > 1) {
    errors.push('Multiple crop_type values detected. Use a single crop_type per import.');
  }

  if (errors.length) {
    return { ok: false, errors };
  }

  const plotsMap = new Map();
  mappedRows.forEach((row) => {
    if (!plotsMap.has(row.plotNo)) {
      plotsMap.set(row.plotNo, []);
    }
    plotsMap.get(row.plotNo).push({ rowNo: row.rowNo, plantCount: row.plantCount });
  });

  const plots = Array.from(plotsMap.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([plotNo, rows]) => ({
      plotNo,
      rows: rows.sort((a, b) => a.rowNo - b.rowNo)
    }));

  const [siteName] = Array.from(siteValues);
  const [cropType] = Array.from(cropValues);

  return {
    ok: true,
    data: { siteName, cropType, plots },
    counts: {
      plots: plots.length,
      rows: mappedRows.length,
      plants: mappedRows.reduce((sum, row) => sum + row.plantCount, 0)
    }
  };
}

function drawTopSection(doc, record, options) {
  const { left, right } = options;
  let y = options.y;
  doc.fillColor('#000000');
  doc.font('Helvetica').fontSize(9).text('Site: ', left, y, { continued: true });
  doc.font('Helvetica-Bold').fontSize(10).text(record.siteName);
  y += mmToPt(5);
  drawDivider(doc, left, right, y);
  y += mmToPt(3);

  doc.font('Helvetica').fontSize(9).text('Crop: ', left, y, { continued: true });
  doc.font('Helvetica-Bold').fontSize(10).text(record.cropType);
  y += mmToPt(5);
  drawDivider(doc, left, right, y);
  y += mmToPt(4);

  return y;
}

function drawFrontPlantLabel(doc, record, options) {
  const { pageWidthPt, pageHeightPt, bleedMm, safeMarginMm } = options;
  const bleedPt = mmToPt(bleedMm);
  const safePt = mmToPt(safeMarginMm);
  const left = bleedPt + safePt;
  const right = pageWidthPt - bleedPt - safePt;
  const width = right - left;
  const topPadPt = mmToPt(8);
  let y = bleedPt + Math.max(topPadPt, safePt);

  y = drawTopSection(doc, record, { left, right, y });

  const writeRow = (label, value) => {
    doc.font('Helvetica').fontSize(9).text(`${label}: `, left, y, { continued: true });
    doc.font('Helvetica-Bold').fontSize(10).text(value);
    y += mmToPt(5);
  };

  writeRow('Plot', pad(record.plotNo, 2));
  if (record.rowNo !== null && record.rowNo !== undefined) {
    writeRow('Row', pad(record.rowNo, 2));
  }
  writeRow('Plant', pad(record.plantNo, 3));
  drawDivider(doc, left, right, y);
  y += mmToPt(6);

  const mainId = record.plantIdShort;
  let idFontSize = 20;
  doc.font('Helvetica-Bold').fontSize(idFontSize);
  while (doc.widthOfString(mainId) > width && idFontSize > 14) {
    idFontSize -= 1;
    doc.fontSize(idFontSize);
  }
  const idY = pageHeightPt / 2 - mmToPt(6);
  doc.text(mainId, left, idY, { width, align: 'center' });

  const dividerAfterIdY = idY + mmToPt(10);
  drawDivider(doc, left, right, dividerAfterIdY);

  const dateText = `Date: ${formatMonthYear(new Date())}`;
  const dateFontSize = 8;
  const bottomPadPt = Math.max(mmToPt(8), safePt);
  const dateTextY = pageHeightPt - bleedPt - bottomPadPt - dateFontSize;
  drawDivider(doc, left, right, dateTextY - mmToPt(3));
  doc.font('Helvetica').fontSize(dateFontSize).fillColor('#333333');
  doc.text(dateText, left, dateTextY, { width });
}

function drawFrontPlotLabel(doc, record, options) {
  const { pageWidthPt, pageHeightPt, bleedMm, safeMarginMm } = options;
  const bleedPt = mmToPt(bleedMm);
  const safePt = mmToPt(safeMarginMm);
  const left = bleedPt + safePt;
  const right = pageWidthPt - bleedPt - safePt;
  const width = right - left;
  const topPadPt = mmToPt(8);
  let y = bleedPt + Math.max(topPadPt, safePt);

  y = drawTopSection(doc, record, { left, right, y });

  const mainId = record.plotIdShort;
  let idFontSize = 44;
  doc.font('Helvetica-Bold').fontSize(idFontSize);
  while (doc.widthOfString(mainId) > width && idFontSize > 32) {
    idFontSize -= 1;
    doc.fontSize(idFontSize);
  }
  const idY = pageHeightPt / 2 - mmToPt(10);
  doc.text(mainId, left, idY, { width, align: 'center' });

  const bottomPadPt = Math.max(mmToPt(8), safePt);
  const rowsText = `Rows: ${record.rowCount}`;
  const rowsTextY = pageHeightPt - bleedPt - bottomPadPt - mmToPt(4);
  drawDivider(doc, left, right, rowsTextY - mmToPt(3));
  doc.font('Helvetica').fontSize(10).fillColor('#333333');
  doc.text(rowsText, left, rowsTextY, { width, align: 'left' });
}

function drawFrontRowLabel(doc, record, options) {
  const { pageWidthPt, pageHeightPt, bleedMm, safeMarginMm } = options;
  const bleedPt = mmToPt(bleedMm);
  const safePt = mmToPt(safeMarginMm);
  const left = bleedPt + safePt;
  const right = pageWidthPt - bleedPt - safePt;
  const width = right - left;
  const topPadPt = mmToPt(8);
  let y = bleedPt + Math.max(topPadPt, safePt);

  y = drawTopSection(doc, record, { left, right, y });

  doc.font('Helvetica').fontSize(9).text(`Plot: ${pad(record.plotNo, 2)}`, left, y);
  y += mmToPt(4);
  drawDivider(doc, left, right, y);
  y += mmToPt(6);

  const mainId = `R${pad(record.rowNo, 2)}`;
  let idFontSize = 34;
  doc.font('Helvetica-Bold').fontSize(idFontSize);
  while (doc.widthOfString(mainId) > width && idFontSize > 24) {
    idFontSize -= 1;
    doc.fontSize(idFontSize);
  }
  const idY = pageHeightPt / 2 - mmToPt(10);
  doc.text(mainId, left, idY, { width, align: 'center' });

  const bottomPadPt = Math.max(mmToPt(8), safePt);
  const plantsText = `Plants: ${record.plantCount}`;
  const plantsTextY = pageHeightPt - bleedPt - bottomPadPt - mmToPt(4);
  drawDivider(doc, left, right, plantsTextY - mmToPt(3));
  doc.font('Helvetica').fontSize(10).fillColor('#333333');
  doc.text(plantsText, left, plantsTextY, { width, align: 'left' });
}

async function drawBackLabel(doc, fullId, qrPayload, options) {
  const { pageWidthPt, pageHeightPt, bleedMm, safeMarginMm } = options;
  const bleedPt = mmToPt(bleedMm);
  const safePt = mmToPt(safeMarginMm);
  const contentWidthPt = pageWidthPt - 2 * (bleedPt + safePt);
  const contentHeightPt = pageHeightPt - 2 * (bleedPt + safePt);
  const quietZonePt = mmToPt(4);
  const minQrPt = mmToPt(38);
  const idealQrPt = mmToPt(45);
  let qrSizePt = Math.min(
    idealQrPt,
    contentWidthPt - quietZonePt * 2,
    contentHeightPt * 0.6 - quietZonePt * 2
  );
  if (qrSizePt < minQrPt) {
    qrSizePt = Math.min(minQrPt, contentWidthPt - quietZonePt * 2, contentHeightPt * 0.6 - quietZonePt * 2);
  }
  if (qrSizePt <= 0) {
    return;
  }
  const qrX = (pageWidthPt - qrSizePt) / 2;
  const qrY = pageHeightPt / 2 - qrSizePt / 2 - mmToPt(6);

  const qrDataUrl = await QRCode.toDataURL(qrPayload, {
    margin: 0,
    width: 256,
    errorCorrectionLevel: 'M'
  });
  doc.image(qrDataUrl, qrX, qrY, { width: qrSizePt, height: qrSizePt });

  const textY = qrY + qrSizePt + mmToPt(4);
  doc.font('Helvetica').fontSize(8).fillColor('#000000');
  doc.text(fullId, bleedPt + safePt, textY, { width: contentWidthPt, align: 'center' });

  const footerText = 'Scan for Info';
  const footerFontSize = 9;
  const bottomPadPt = Math.max(mmToPt(8), safePt);
  const footerTextY = pageHeightPt - bleedPt - bottomPadPt - footerFontSize;
  drawDivider(doc, bleedPt + safePt, pageWidthPt - bleedPt - safePt, footerTextY - mmToPt(4));
  doc.font('Helvetica-Bold').fontSize(footerFontSize);
  doc.text(footerText, bleedPt + safePt, footerTextY, { width: contentWidthPt, align: 'center' });
}

function csvEscapeRow(values) {
  return values
    .map((value) => {
      const text = String(value ?? '');
      if (/[",\n]/.test(text)) {
        return `"${text.replace(/"/g, '""')}"`;
      }
      return text;
    })
    .join(',');
}

function buildCsvPlant({ values, exportPlot, trackedPlants }) {
  const lines = ['site,crop,plot_no,row_no,plant_no,short_id,full_id'];
  for (const record of iterateLabels({ ...values, exportPlot, trackedPlants })) {
    lines.push(
      csvEscapeRow([
        record.siteName,
        record.cropType,
        record.plotNo,
        record.rowNo,
        record.plantNo,
        record.plantIdShort,
        record.plantIdFull
      ])
    );
  }
  return lines.join('\n');
}

function buildCsvRow({ values, exportPlot }) {
  const lines = ['site,crop,plot_no,row_no,plant_count,row_id'];
  for (const record of iterateRows({ ...values, exportPlot, structureCode: values.structureCode })) {
    lines.push(
      csvEscapeRow([
        record.siteName,
        record.cropType,
        record.plotNo,
        record.rowNo,
        record.plantCount,
        record.rowIdFull
      ])
    );
  }
  return lines.join('\n');
}

function buildCsvPlot({ values, exportPlot }) {
  const lines = ['site,crop,plot_no,row_count,plot_id'];
  for (const record of iteratePlots({ ...values, exportPlot })) {
    lines.push(
      csvEscapeRow([
        record.siteName,
        record.cropType,
        record.plotNo,
        record.rowCount,
        record.plotIdFull
      ])
    );
  }
  return lines.join('\n');
}

app.get('/api/import-template', (_req, res) => {
  const worksheet = XLSX.utils.aoa_to_sheet([
    ['site_name', 'crop_type', 'plot_no', 'row_no', 'plant_count'],
    ['TERLABAD', 'MARIGOLD', 1, 1, 25]
  ]);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, 'ROWS');
  const buffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'buffer' });
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', 'attachment; filename="farm-rows-template.xlsx"');
  res.send(buffer);
});

app.get('/api/imports', (_req, res) => {
  res.json({ history: loadImportHistory() });
});

app.post('/api/import', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ errors: ['Excel file is required.'], warnings: [] });
  }
  const parsed = parseExcelRows(req.file.buffer);
  if (!parsed.ok) {
    return res.status(400).json({ errors: parsed.errors, warnings: [] });
  }

  const importId = `import_${Date.now()}`;
  const record = {
    id: importId,
    fileName: req.file.originalname,
    uploadedAt: new Date().toISOString(),
    siteName: parsed.data.siteName,
    cropType: parsed.data.cropType,
    counts: parsed.counts
  };
  saveImportHistory(record);
  saveCurrentImport({
    ...parsed.data,
    structureCode: 'S3',
    mode: MODES.STANDARD,
    importId,
    uploadedAt: record.uploadedAt
  });

  res.json({
    importId,
    siteName: parsed.data.siteName,
    cropType: parsed.data.cropType,
    plots: parsed.data.plots,
    counts: parsed.counts,
    structureCode: 'S3',
    mode: MODES.STANDARD
  });
});

app.post('/api/csv', (req, res) => {
  const validation = validatePayload(req.body || {});
  if (!validation.ok) {
    return res.status(400).json({ errors: validation.errors, warnings: validation.warnings });
  }

  const exportPlot = req.body.exportPlot && req.body.exportPlot !== 'all'
    ? parsePositiveInt(req.body.exportPlot)
    : null;

  if (exportPlot && exportPlot > validation.values.totalPlots) {
    return res.status(400).json({ errors: ['Selected plot is outside the available range.'], warnings: [] });
  }

  const labelType = req.body.labelType || 'plant';
  if (labelType === 'all') {
    return res.status(400).json({ errors: ['CSV export does not support combined labels. Export each label type separately.'], warnings: [] });
  }
  if (labelType === 'plant' && !hasPlantTracking(validation.values.structureCode, validation.values.mode)) {
    return res.status(400).json({ errors: ['Plant labels are not enabled for this structure/mode.'], warnings: [] });
  }
  if (labelType === 'row' && !getStructure(validation.values.structureCode).hasRows) {
    return res.status(400).json({ errors: ['Row labels are not available for this structure.'], warnings: [] });
  }

  const trackedPlants = Array.isArray(req.body.trackedPlants) ? req.body.trackedPlants : null;
  if (labelType === 'plant' && validation.values.mode === MODES.RESEARCH && (!trackedPlants || trackedPlants.length === 0)) {
    return res.status(400).json({ errors: ['Generate a sampling plan before exporting plant labels.'], warnings: [] });
  }

  let csv = '';
  if (labelType === 'plot') {
    csv = buildCsvPlot({ values: validation.values, exportPlot });
  } else if (labelType === 'row') {
    csv = buildCsvRow({ values: validation.values, exportPlot });
  } else {
    csv = buildCsvPlant({ values: validation.values, exportPlot, trackedPlants });
  }
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="farm-${labelType}-labels.csv"`);
  res.send(csv);
});

app.post('/api/pdf', async (req, res) => {
  const validation = validatePayload(req.body || {});
  if (!validation.ok) {
    return res.status(400).json({ errors: validation.errors, warnings: validation.warnings });
  }

  const labelType = req.body.labelType || 'plant';
  const exportPlot = req.body.exportPlot && req.body.exportPlot !== 'all'
    ? parsePositiveInt(req.body.exportPlot)
    : null;

  if (exportPlot && exportPlot > validation.values.totalPlots) {
    return res.status(400).json({ errors: ['Selected plot is outside the available range.'], warnings: [] });
  }

  const layout = getLayoutOptions(req.body || {});
  const structure = getStructure(validation.values.structureCode);
  if (labelType === 'plant' && !hasPlantTracking(validation.values.structureCode, validation.values.mode)) {
    return res.status(400).json({ errors: ['Plant labels are not enabled for this structure/mode.'], warnings: [] });
  }
  if (labelType === 'row' && !structure.hasRows) {
    return res.status(400).json({ errors: ['Row labels are not available for this structure.'], warnings: [] });
  }
  if (layout.layoutMode === 'sheet' && labelType !== 'plant') {
    return res.status(400).json({ errors: ['Sheet mode is only supported for plant labels.'], warnings: [] });
  }
  if (layout.layoutMode === 'single') {
    if (layout.safeMarginMm < 0) {
      return res.status(400).json({ errors: ['Safe margin cannot be negative.'], warnings: [] });
    }
    const bleedMm = layout.includeBleed ? 3 : 0;
    if (layout.safeMarginMm * 2 >= layout.preset.widthMm || layout.safeMarginMm * 2 >= layout.preset.heightMm) {
      return res.status(400).json({ errors: ['Safe margin is too large for the label size.'], warnings: [] });
    }
    const pageWidthPt = mmToPt(layout.preset.widthMm + bleedMm * 2);
    const pageHeightPt = mmToPt(layout.preset.heightMm + bleedMm * 2);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="farm-${labelType}-labels.pdf"`);

    const doc = new PDFDocument({ size: [pageWidthPt, pageHeightPt], margin: 0 });
    doc.pipe(res);

    let index = 0;
    try {
      const trackedPlants = Array.isArray(req.body.trackedPlants) ? req.body.trackedPlants : null;
      if (labelType === 'plant' && validation.values.mode === MODES.RESEARCH && (!trackedPlants || trackedPlants.length === 0)) {
        return res.status(400).json({ errors: ['Generate a sampling plan before exporting plant labels.'], warnings: [] });
      }

      const labelTypes = labelType === 'all'
        ? ['plot', 'row', 'plant']
        : [labelType];

      for (const type of labelTypes) {
        if (type === 'row' && !structure.hasRows) continue;
        if (type === 'plant' && !hasPlantTracking(validation.values.structureCode, validation.values.mode)) continue;

        const iterator = type === 'plot'
          ? iteratePlots({ ...validation.values, exportPlot })
          : type === 'row'
            ? iterateRows({ ...validation.values, exportPlot, structureCode: validation.values.structureCode })
            : iterateLabels({
              ...validation.values,
              exportPlot,
              structureCode: validation.values.structureCode,
              trackedPlants
            });

        for (const record of iterator) {
          if (index > 0) doc.addPage();
          if (type === 'plot') {
            drawFrontPlotLabel(doc, record, {
              pageWidthPt,
              pageHeightPt,
              bleedMm,
              safeMarginMm: layout.safeMarginMm
            });
          } else if (type === 'row') {
            drawFrontRowLabel(doc, record, {
              pageWidthPt,
              pageHeightPt,
              bleedMm,
              safeMarginMm: layout.safeMarginMm
            });
          } else {
            drawFrontPlantLabel(doc, record, {
              pageWidthPt,
              pageHeightPt,
              bleedMm,
              safeMarginMm: layout.safeMarginMm
            });
          }

          const fullId = type === 'plot'
            ? record.plotIdFull
            : type === 'row'
              ? record.rowIdFull
              : record.plantIdFull;
          const qrPayload = getQrPayload(fullId, req.body || {});

          doc.addPage();
          await drawBackLabel(doc, fullId, qrPayload, {
            pageWidthPt,
            pageHeightPt,
            bleedMm,
            safeMarginMm: layout.safeMarginMm
          });
          index += 1;
        }
      }

      doc.end();
    } catch (error) {
      doc.end();
      res.status(500).end();
    }
    return;
  }

  if (layout.labelWidthMm <= 0 || layout.labelHeightMm <= 0) {
    return res.status(400).json({ errors: ['Label width and height must be greater than 0.'], warnings: [] });
  }
  if (Object.values(layout.marginsMm).some((value) => value < 0)) {
    return res.status(400).json({ errors: ['Margins cannot be negative.'], warnings: [] });
  }
  if (Object.values(layout.gapsMm).some((value) => value < 0)) {
    return res.status(400).json({ errors: ['Gaps cannot be negative.'], warnings: [] });
  }

  const pageWidthPt = mmToPt(layout.preset.widthMm);
  const pageHeightPt = mmToPt(layout.preset.heightMm);
  const labelWidthPt = mmToPt(layout.labelWidthMm);
  const labelHeightPt = mmToPt(layout.labelHeightMm);
  const marginLeft = mmToPt(layout.marginsMm.left);
  const marginRight = mmToPt(layout.marginsMm.right);
  const marginTop = mmToPt(layout.marginsMm.top);
  const marginBottom = mmToPt(layout.marginsMm.bottom);
  const gapX = mmToPt(layout.gapsMm.x);
  const gapY = mmToPt(layout.gapsMm.y);

  const usableWidth = pageWidthPt - marginLeft - marginRight;
  const usableHeight = pageHeightPt - marginTop - marginBottom;
  const columns = Math.floor((usableWidth + gapX) / (labelWidthPt + gapX));
  const rows = Math.floor((usableHeight + gapY) / (labelHeightPt + gapY));

  if (columns < 1 || rows < 1) {
    return res.status(400).json({
      errors: ['Label size or margins are too large for the selected paper size.'],
      warnings: validation.warnings
    });
  }

  const labelsPerPage = columns * rows;
  const includeQr = Boolean(req.body.includeQr);
  const dateText = new Date().toLocaleDateString('en-CA');
  const trackedPlants = Array.isArray(req.body.trackedPlants) ? req.body.trackedPlants : null;
  if (validation.values.mode === MODES.RESEARCH && (!trackedPlants || trackedPlants.length === 0)) {
    return res.status(400).json({ errors: ['Generate a sampling plan before exporting plant labels.'], warnings: [] });
  }

  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', 'attachment; filename="farm-plant-labels.pdf"');

  const doc = new PDFDocument({ size: [pageWidthPt, pageHeightPt], margin: 0 });
  doc.pipe(res);

  const padding = mmToPt(2.5);
  const maxQrSize = Math.max(0, Math.min(labelHeightPt - padding * 2, labelWidthPt * 0.38));
  const canRenderQr = includeQr && maxQrSize > 0;

  let index = 0;
  try {
    for (const record of iterateLabels({
      ...validation.values,
      exportPlot,
      structureCode: validation.values.structureCode,
      trackedPlants
    })) {
      if (index > 0 && index % labelsPerPage === 0) {
        doc.addPage();
      }
      const indexInPage = index % labelsPerPage;
      const row = Math.floor(indexInPage / columns);
      const col = indexInPage % columns;
      const x = marginLeft + col * (labelWidthPt + gapX);
      const y = marginTop + row * (labelHeightPt + gapY);

      const textAreaWidth = Math.max(10, labelWidthPt - padding * 2 - (canRenderQr ? maxQrSize + padding : 0));
      const textX = x + padding;
      const textY = y + padding;

      doc.font('Helvetica').fontSize(8).fillColor('black');
      doc.text(record.siteName, textX, textY, { width: textAreaWidth });
      doc.text(record.cropType, textX, textY + 10, { width: textAreaWidth });

      const idFontSize = Math.max(12, Math.min(20, labelHeightPt * 0.22));
      doc.font('Helvetica-Bold').fontSize(idFontSize);
      doc.text(record.plantIdShort, textX, textY + 22, { width: textAreaWidth });

      doc.font('Helvetica').fontSize(7);
      doc.text(`Generated ${dateText}`, textX, y + labelHeightPt - padding - 8, {
        width: textAreaWidth
      });

      if (canRenderQr) {
        const qrDataUrl = await QRCode.toDataURL(getQrPayload(record.plantIdFull, req.body || {}), {
          margin: 0,
          width: 256,
          errorCorrectionLevel: 'M'
        });
        const qrX = x + labelWidthPt - padding - maxQrSize;
        const qrY = y + (labelHeightPt - maxQrSize) / 2;
        doc.image(qrDataUrl, qrX, qrY, { width: maxQrSize, height: maxQrSize });
      }

      index += 1;
    }

    doc.end();
  } catch (error) {
    doc.end();
    res.status(500).end();
  }
});

app.get('/api/presets', (_req, res) => {
  res.json({ paperPresets: PAPER_PRESETS, defaults: DEFAULT_LAYOUT });
});

app.post('/api/sample', (req, res) => {
  const validation = validatePayload(req.body || {});
  if (!validation.ok) {
    return res.status(400).json({ errors: validation.errors, warnings: validation.warnings });
  }

  if (validation.values.mode !== MODES.RESEARCH) {
    return res.status(400).json({ errors: ['Sampling is only available in Research mode.'], warnings: [] });
  }

  const planConfig = req.body.samplingPlan || req.body.sampling || {};
  try {
    const planResult = buildSamplingPlan(validation.values, planConfig);
    const planId = `plan_${Date.now()}`;
    const planRecord = {
      id: planId,
      createdAt: new Date().toISOString(),
      siteName: validation.values.siteName,
      cropType: validation.values.cropType,
      batchName: validation.values.batchName,
      startDate: validation.values.startDate,
      structureCode: validation.values.structureCode,
      mode: validation.values.mode,
      samplingType: planResult.samplingType,
      seed: planResult.seed,
      config: planConfig,
      trackedPlants: planResult.trackedPlants
    };
    saveSamplingPlan(planRecord);
    res.json({
      samplingPlanId: planId,
      seed: planResult.seed,
      trackedPlants: planResult.trackedPlants,
      totalSamples: planResult.trackedPlants.length
    });
  } catch (error) {
    res.status(400).json({ errors: [error.message || 'Sampling failed.'], warnings: [] });
  }
});

app.listen(PORT, () => {
  console.log(`Farm Label Generator running on http://localhost:${PORT}`);
});
