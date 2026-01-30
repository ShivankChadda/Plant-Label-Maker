const form = document.getElementById('labelForm');
const stageA = document.getElementById('stageA');
const stageB = document.getElementById('stageB');
const continueButton = document.getElementById('continueButton');
const setupMessages = document.getElementById('setupMessages');
const previewButton = document.getElementById('previewButton');
const pdfButton = document.getElementById('pdfButton');
const csvButton = document.getElementById('csvButton');
const formatPreview = document.getElementById('formatPreview');
const totalPreview = document.getElementById('totalPreview');
const sampleGrid = document.getElementById('sampleGrid');
const formMessages = document.getElementById('formMessages');
const exportPlotSelect = document.getElementById('exportPlot');
const labelTypeSelect = document.getElementById('labelType');
const qrModeSelect = document.getElementById('qrMode');
const qrBaseUrlField = document.getElementById('qrBaseUrlField');
const qrBaseUrlInput = document.getElementById('qrBaseUrl');
const batchNameInput = document.getElementById('batchName');
const startDateInput = document.getElementById('startDate');
const structureSelect = document.getElementById('structureCode');
const modeSelect = document.getElementById('mode');
const inputModeSelect = document.getElementById('inputMode');
const layoutModeSelect = document.getElementById('layoutMode');
const paperPresetSelect = document.getElementById('paperPreset');
const includeQrField = document.getElementById('includeQrField');
const includeBackField = document.getElementById('includeBackField');
const plotsContainer = document.getElementById('plotsContainer');
const defaultRowsInput = document.getElementById('defaultRows');
const defaultPlantsInput = document.getElementById('defaultPlants');
const defaultPlantsPerPlotField = document.getElementById('defaultPlantsPerPlotField');
const defaultPlantsPerPlotInput = document.getElementById('defaultPlantsPerPlot');
const defaultsPanel = document.querySelector('.defaults-panel');
const applyDefaultsCheckbox = document.getElementById('applyDefaults');
const applyDefaultsButton = document.getElementById('applyDefaultsButton');
const copyPlotButton = document.getElementById('copyPlotButton');
const samplingPanel = document.getElementById('samplingPanel');
const samplingTypeSelect = document.getElementById('samplingType');
const samplesPerPlotField = document.getElementById('samplesPerPlotField');
const samplesPerRowField = document.getElementById('samplesPerRowField');
const samplingPlotsField = document.getElementById('samplingPlotsField');
const samplingRowsField = document.getElementById('samplingRowsField');
const samplingSeedInput = document.getElementById('samplingSeed');
const generateSamplingButton = document.getElementById('generateSamplingButton');
const samplingSummary = document.getElementById('samplingSummary');
const importPanel = document.getElementById('importPanel');
const importFileInput = document.getElementById('importFile');
const downloadTemplateButton = document.getElementById('downloadTemplate');
const importButton = document.getElementById('importButton');
const importMessages = document.getElementById('importMessages');
const importHistory = document.getElementById('importHistory');

const countPlots = document.getElementById('countPlots');
const countRows = document.getElementById('countRows');
const countPlants = document.getElementById('countPlants');
const countTotal = document.getElementById('countTotal');

let plotsData = [];
let activePlotIndex = 0;
let needsRebuild = false;
let samplingState = {
  planId: null,
  trackedPlants: [],
  totalSamples: 0,
  seed: null
};

const CROP_DEFAULTS = {
  'areca nut': { structure: 'S1', mode: 'Standard' },
  'radish': { structure: 'S2', mode: 'Standard' },
  'marigold': { structure: 'S3', mode: 'Research' }
};

function normalizeCode(text) {
  return text
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function pad(num, len) {
  return String(num).padStart(len, '0');
}

function parsePositiveInt(value) {
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) return null;
  return num;
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

function buildPlantIdFull(siteName, cropType, plotNo, rowNo, plantNo) {
  const siteCode = normalizeCode(siteName || 'SITE');
  const cropCode = normalizeCode(cropType || 'CROP');
  return `${siteCode}-${cropCode}-${buildPlantIdShort(plotNo, rowNo, plantNo)}`;
}

function buildPlotIdFull(siteName, cropType, plotNo) {
  const siteCode = normalizeCode(siteName || 'SITE');
  const cropCode = normalizeCode(cropType || 'CROP');
  return `${siteCode}-${cropCode}-${buildPlotIdShort(plotNo)}`;
}

function buildRowIdFull(siteName, cropType, plotNo, rowNo) {
  const siteCode = normalizeCode(siteName || 'SITE');
  const cropCode = normalizeCode(cropType || 'CROP');
  return `${siteCode}-${cropCode}-${buildRowIdShort(plotNo, rowNo)}`;
}

function buildPlantIdShortFlexible(structureCode, plotNo, rowNo, plantNo) {
  if (structureCode === 'S1') {
    return `P${pad(plotNo, 2)}-T${pad(plantNo, 3)}`;
  }
  return buildPlantIdShort(plotNo, rowNo, plantNo);
}

function buildPlantIdFullFlexible(siteName, cropType, structureCode, plotNo, rowNo, plantNo) {
  const siteCode = normalizeCode(siteName || 'SITE');
  const cropCode = normalizeCode(cropType || 'CROP');
  return `${siteCode}-${cropCode}-${buildPlantIdShortFlexible(structureCode, plotNo, rowNo, plantNo)}`;
}

function getValues() {
  const labelTypeValue = labelTypeSelect ? labelTypeSelect.value : 'plant';
  const structureCode = structureSelect ? structureSelect.value : 'S3';
  const mode = modeSelect ? modeSelect.value : 'Standard';
  const plotsPayload = structureCode === 'S1'
    ? plotsData.map((plot, index) => ({
      plot_no: plot.plotNo || index + 1,
      plant_count: plot.plantCount
    }))
    : structureCode === 'S4'
      ? plotsData.map((plot, index) => ({
        plot_no: plot.plotNo || index + 1
      }))
      : plotsData.map((plot, index) => ({
        plot_no: plot.plotNo || index + 1,
        rows: plot.rows.map((row, rowIndex) => ({
          row_no: rowIndex + 1,
          plant_count: row.plantCount
        }))
      }));
  return {
    siteName: form.siteName.value.trim(),
    cropType: form.cropType.value.trim(),
    batchName: batchNameInput ? batchNameInput.value.trim() : '',
    startDate: startDateInput ? startDateInput.value : '',
    structureCode,
    mode,
    plotsCount: parsePositiveInt(form.plotsCount.value),
    inputMode: inputModeSelect ? inputModeSelect.value : 'manual',
    plots: plotsPayload,
    layoutMode: labelTypeValue === 'plant' ? layoutModeSelect.value : 'single',
    labelType: labelTypeValue,
    qrMode: qrModeSelect ? qrModeSelect.value : 'id',
    qrBaseUrl: qrBaseUrlInput ? qrBaseUrlInput.value.trim() : '',
    trackedPlants: samplingState.trackedPlants,
    samplingPlanId: samplingState.planId,
    paperPreset: form.paperPreset.value,
    labelWidthMm: Number(form.labelWidth.value),
    labelHeightMm: Number(form.labelHeight.value),
    includeQr: form.includeQr?.checked,
    includeBack: form.includeBack?.checked,
    exportPlot: exportPlotSelect.value,
    marginsMm: {
      top: Number(document.getElementById('marginTop').value),
      right: Number(document.getElementById('marginRight').value),
      bottom: Number(document.getElementById('marginBottom').value),
      left: Number(document.getElementById('marginLeft').value)
    },
    gapsMm: {
      x: Number(document.getElementById('gapX').value),
      y: Number(document.getElementById('gapY').value)
    },
    safeMarginMm: Number(document.getElementById('safeMargin').value),
    includeBleed: document.getElementById('includeBleed').checked
  };
}

function validate(values) {
  const errors = [];
  const warnings = [];
  const structureCode = values.structureCode || 'S3';
  const mode = values.mode || 'Standard';
  const structureHasRows = structureCode === 'S2' || structureCode === 'S3';
  const structureHasPlants = structureCode === 'S1' || structureCode === 'S3';

  if (!values.siteName) errors.push('Site name is required.');
  if (!values.cropType) errors.push('Crop type is required.');
  if (values.inputMode !== 'excel' && values.plotsCount && values.plots.length !== values.plotsCount) {
    errors.push('Plot count changed. Click Continue to rebuild plots.');
  }
  if (!values.plots.length) errors.push('At least one plot is required.');
  if ((mode === 'Research' || mode === 'Full') && !structureHasPlants) {
    errors.push('Selected tracking mode requires plant tracking.');
  }

  let totalRows = 0;
  let totalPlants = 0;

  values.plots.forEach((plot) => {
    if (structureHasRows) {
      if (!plot.rows.length) {
        errors.push(`Plot ${plot.plot_no} must have at least 1 row.`);
        return;
      }
      totalRows += plot.rows.length;
      plot.rows.forEach((row) => {
        if (!row.plant_count || row.plant_count < 1) {
          errors.push(`Plot ${plot.plot_no} Row ${row.row_no} must have at least 1 plant.`);
        } else {
          totalPlants += row.plant_count;
        }
      });
      return;
    }

    if (structureHasPlants) {
      if (!plot.plant_count || plot.plant_count < 1) {
        errors.push(`Plot ${plot.plot_no} must have at least 1 plant.`);
      } else {
        totalPlants += plot.plant_count;
      }
    }
  });

  const total = totalPlants;
  if (total > 10000) warnings.push('Total labels exceed 10,000. Consider exporting plot-wise.');

  return {
    errors,
    warnings,
    total,
    totalPlots: values.plots.length,
    totalRows,
    totalPlants
  };
}

function updatePlotOptions(plotCount) {
  const current = exportPlotSelect.value;
  exportPlotSelect.innerHTML = '<option value="all">All plots</option>';
  if (plotCount && plotCount > 0) {
    for (let p = 1; p <= plotCount; p += 1) {
      const option = document.createElement('option');
      option.value = String(p);
      option.textContent = `Plot ${p}`;
      exportPlotSelect.appendChild(option);
    }
  }
  if ([...exportPlotSelect.options].some((opt) => opt.value === current)) {
    exportPlotSelect.value = current;
  }
}

function renderMessages({ errors, warnings }, target = formMessages) {
  const lines = [];
  errors.forEach((err) => lines.push(`• ${err}`));
  warnings.forEach((warn) => lines.push(`• ${warn}`));
  target.textContent = lines.join('\n');
}

function buildPlotData(plotNo, rowsCount, defaultPlants) {
  return {
    plotNo,
    rows: Array.from({ length: rowsCount }, (_, index) => ({
      plantCount: defaultPlants,
      rowNo: index + 1
    }))
  };
}

function buildPlotDataPlants(plotNo, plantCount) {
  return { plotNo, plantCount };
}

function buildPlotDataEmpty(plotNo) {
  return { plotNo };
}

function applyDefaultsToPlot(plotIndex, rowsCount, defaultPlants) {
  const targetPlot = plotsData[plotIndex];
  if (!targetPlot) return;
  targetPlot.rows = Array.from({ length: rowsCount }, (_, index) => ({
    rowNo: index + 1,
    plantCount: defaultPlants
  }));
}

function rebuildPlots(plotCount, defaultRows, defaultPlants) {
  const structureCode = structureSelect ? structureSelect.value : 'S3';
  if (structureCode === 'S4') {
    plotsData = Array.from({ length: plotCount }, (_, index) => buildPlotDataEmpty(index + 1));
  } else if (structureCode === 'S1') {
    const defaultPlantsPerPlot = parsePositiveInt(defaultPlantsPerPlotInput.value) || 1;
    plotsData = Array.from({ length: plotCount }, (_, index) =>
      buildPlotDataPlants(index + 1, defaultPlantsPerPlot)
    );
  } else {
    plotsData = Array.from({ length: plotCount }, (_, index) =>
      buildPlotData(index + 1, defaultRows, defaultPlants)
    );
  }
  activePlotIndex = 0;
  renderPlots();
}

function updatePlotRows(plotIndex, nextRows, defaultPlants) {
  const plot = plotsData[plotIndex];
  if (!plot) return;
  const currentCount = plot.rows.length;
  const rowsCount = Math.max(1, nextRows);

  if (rowsCount > currentCount) {
    for (let i = currentCount; i < rowsCount; i += 1) {
      plot.rows.push({ rowNo: i + 1, plantCount: defaultPlants });
    }
  } else if (rowsCount < currentCount) {
    plot.rows = plot.rows.slice(0, rowsCount);
  }
}

function sumPlants(rows) {
  return rows.reduce((sum, row) => sum + (row.plantCount || 0), 0);
}

function renderPlots() {
  plotsContainer.innerHTML = '';
  const structureCode = structureSelect ? structureSelect.value : 'S3';

  if (structureCode === 'S4') {
    plotsData.forEach((plot, plotIndex) => {
      const card = document.createElement('div');
      card.className = 'plot-card';
      card.innerHTML = `
        <div class="plot-meta">Plot ${plot.plotNo}</div>
      `;
      plotsContainer.appendChild(card);
    });
    updatePlotOptions(plotsData.length);
    copyPlotButton.disabled = true;
    return;
  }

  if (structureCode === 'S1') {
    plotsData.forEach((plot, plotIndex) => {
      const card = document.createElement('div');
      card.className = 'plot-card';
      card.innerHTML = `
        <div class="plot-meta">Plot ${plot.plotNo}</div>
        <div class="plot-controls">
          <div class="field">
            <label>Plants in Plot</label>
            <input type="number" min="1" step="1" value="${plot.plantCount || ''}" />
          </div>
        </div>
      `;
      const input = card.querySelector('input');
      input.addEventListener('input', (event) => {
        const value = parsePositiveInt(event.target.value) || 0;
        plotsData[plotIndex].plantCount = value;
        resetSamplingState('Sampling plan cleared. Regenerate after plant counts change.');
        updatePreview();
      });
      plotsContainer.appendChild(card);
    });
    updatePlotOptions(plotsData.length);
    copyPlotButton.disabled = plotsData.length < 2;
    return;
  }

  plotsData.forEach((plot, plotIndex) => {
    const details = document.createElement('details');
    details.className = 'plot-card';
    details.open = plotIndex === activePlotIndex;
    details.addEventListener('toggle', () => {
      if (details.open) {
        activePlotIndex = plotIndex;
      }
    });

    const summary = document.createElement('summary');
    summary.textContent = `Plot ${plot.plotNo}`;

    const meta = document.createElement('div');
    meta.className = 'plot-meta';
    meta.textContent = `${plot.rows.length} rows · ${sumPlants(plot.rows)} plants`;
    const updateMeta = () => {
      meta.textContent = `${plot.rows.length} rows · ${sumPlants(plot.rows)} plants`;
    };

    const controls = document.createElement('div');
    controls.className = 'plot-controls';
    const rowsField = document.createElement('div');
    rowsField.className = 'field';
    rowsField.innerHTML = `
      <label>Number of Rows</label>
      <input type="number" min="1" step="1" value="${plot.rows.length}" />
    `;
    const rowsInput = rowsField.querySelector('input');
    rowsInput.addEventListener('change', (event) => {
      const count = parsePositiveInt(event.target.value) || 1;
      const defaultPlants = parsePositiveInt(defaultPlantsInput.value) || 1;
      updatePlotRows(plotIndex, count, defaultPlants);
      resetSamplingState('Sampling plan cleared. Regenerate after structure changes.');
      renderPlots();
      updatePreview();
    });

    const addRowButton = document.createElement('button');
    addRowButton.type = 'button';
    addRowButton.className = 'ghost';
    addRowButton.textContent = 'Add Row';
    addRowButton.addEventListener('click', () => {
      const defaultPlants = parsePositiveInt(defaultPlantsInput.value) || 1;
      updatePlotRows(plotIndex, plot.rows.length + 1, defaultPlants);
      resetSamplingState('Sampling plan cleared. Regenerate after structure changes.');
      renderPlots();
      updatePreview();
    });

    const removeRowButton = document.createElement('button');
    removeRowButton.type = 'button';
    removeRowButton.className = 'ghost';
    removeRowButton.textContent = 'Remove Row';
    removeRowButton.disabled = plot.rows.length <= 1;
    removeRowButton.addEventListener('click', () => {
      updatePlotRows(plotIndex, plot.rows.length - 1, 1);
      resetSamplingState('Sampling plan cleared. Regenerate after structure changes.');
      renderPlots();
      updatePreview();
    });

    controls.append(rowsField, addRowButton, removeRowButton);

    const rowList = document.createElement('div');
    rowList.className = 'row-list';

    plot.rows.forEach((row, rowIndex) => {
      const rowItem = document.createElement('div');
      rowItem.className = 'row-item';
      rowItem.innerHTML = `
        <div class="row-label">Row ${pad(rowIndex + 1, 2)}</div>
        <input type="number" min="1" step="1" value="${row.plantCount || ''}" />
        <div class="row-actions"></div>
      `;
      const input = rowItem.querySelector('input');
      input.addEventListener('input', (event) => {
        const value = parsePositiveInt(event.target.value) || 0;
        plotsData[plotIndex].rows[rowIndex].plantCount = value;
        updateMeta();
        resetSamplingState('Sampling plan cleared. Regenerate after plant counts change.');
        updatePreview();
      });
      rowList.appendChild(rowItem);
    });

    details.append(summary, meta, controls, rowList);
    plotsContainer.appendChild(details);
  });

  updatePlotOptions(plotsData.length);
  copyPlotButton.disabled = plotsData.length < 2;
}

function toggleField(id, show) {
  const field = document.getElementById(id);
  if (!field) return;
  const wrapper = field.closest('.field');
  if (!wrapper) return;
  wrapper.classList.toggle('hidden', !show);
}

function updateLayoutVisibility() {
  const labelTypeValue = labelTypeSelect ? labelTypeSelect.value : 'plant';
  const structureCode = structureSelect ? structureSelect.value : 'S3';
  const mode = modeSelect ? modeSelect.value : 'Standard';
  const structureHasRows = structureCode === 'S2' || structureCode === 'S3';
  const structureHasPlants = structureCode === 'S1' || structureCode === 'S3';
  const plantLabelsEnabled = structureHasPlants && (mode === 'Research' || mode === 'Full');

  if (labelTypeSelect) {
    const plantOption = labelTypeSelect.querySelector('option[value="plant"]');
    const rowOption = labelTypeSelect.querySelector('option[value="row"]');
    const plotOption = labelTypeSelect.querySelector('option[value="plot"]');
    const allOption = labelTypeSelect.querySelector('option[value="all"]');
    if (plantOption) plantOption.disabled = !plantLabelsEnabled;
    if (rowOption) rowOption.disabled = !structureHasRows;
    if (plotOption) plotOption.disabled = false;
    if (allOption) {
      allOption.disabled = !(structureHasRows || plantLabelsEnabled);
    }

    if (labelTypeValue === 'plant' && !plantLabelsEnabled) {
      labelTypeSelect.value = structureHasRows ? 'row' : 'plot';
    }
    if (labelTypeValue === 'row' && !structureHasRows) {
      labelTypeSelect.value = 'plot';
    }
    if (labelTypeValue === 'all' && !(structureHasRows || plantLabelsEnabled)) {
      labelTypeSelect.value = 'plot';
    }
  }

  const finalLabelType = labelTypeSelect ? labelTypeSelect.value : labelTypeValue;

  if (finalLabelType !== 'plant') {
    layoutModeSelect.value = 'single';
  }
  const isSingle = layoutModeSelect.value === 'single';

  if (isSingle) {
    paperPresetSelect.value = 'Label3x5';
  } else if (paperPresetSelect.value === 'Label3x5') {
    paperPresetSelect.value = 'A4';
  }
  layoutModeSelect.disabled = finalLabelType !== 'plant';
  paperPresetSelect.disabled = isSingle;
  form.labelWidth.disabled = isSingle;
  form.labelHeight.disabled = isSingle;

  includeQrField.classList.toggle('hidden', isSingle);
  includeBackField.classList.add('hidden');

  toggleField('marginTop', !isSingle);
  toggleField('marginRight', !isSingle);
  toggleField('marginBottom', !isSingle);
  toggleField('marginLeft', !isSingle);
  toggleField('gapX', !isSingle);
  toggleField('gapY', !isSingle);
  toggleField('safeMargin', isSingle);
  toggleField('includeBleed', isSingle);

  if (qrBaseUrlField && qrModeSelect) {
    qrBaseUrlField.classList.toggle('hidden', qrModeSelect.value !== 'url');
  }

  if (samplingPanel) {
    samplingPanel.classList.toggle('hidden', !(mode === 'Research' && structureHasPlants));
  }

  if (defaultPlantsPerPlotField) {
    defaultPlantsPerPlotField.classList.toggle('hidden', structureCode !== 'S1');
  }
  defaultRowsInput.closest('.field').classList.toggle('hidden', structureCode === 'S1' || structureCode === 'S4');
  defaultPlantsInput.closest('.field').classList.toggle('hidden', structureCode === 'S1' || structureCode === 'S4');
  if (applyDefaultsButton) {
    applyDefaultsButton.disabled = structureCode === 'S4';
  }
}

function updateSamplingVisibility() {
  if (!samplingPanel) return;
  const structureCode = structureSelect ? structureSelect.value : 'S3';
  const mode = modeSelect ? modeSelect.value : 'Standard';
  const structureHasPlants = structureCode === 'S1' || structureCode === 'S3';
  const structureHasRows = structureCode === 'S2' || structureCode === 'S3';

  samplingPanel.classList.toggle('hidden', !(mode === 'Research' && structureHasPlants));

  if (samplingTypeSelect) {
    const rowBasedOption = samplingTypeSelect.querySelector('option[value="row_based"]');
    if (rowBasedOption) {
      rowBasedOption.disabled = !structureHasRows;
      if (!structureHasRows) {
        samplingTypeSelect.value = 'plot_based';
      }
    }
  }

  const isRowBased = samplingTypeSelect && samplingTypeSelect.value === 'row_based';
  samplesPerPlotField.classList.toggle('hidden', isRowBased);
  samplesPerRowField.classList.toggle('hidden', !isRowBased);
  samplingPlotsField.classList.toggle('hidden', false);
  samplingRowsField.classList.toggle('hidden', !isRowBased);
}

function resetSamplingState(message) {
  samplingState = { planId: null, trackedPlants: [], totalSamples: 0, seed: null };
  if (samplingSummary) {
    samplingSummary.textContent = message || '';
  }
}

function applyImportedData(payload) {
  const plots = Array.isArray(payload.plots) ? payload.plots : [];
  plotsData = plots.map((plot) => ({
    plotNo: plot.plotNo || plot.plot_no,
    rows: (plot.rows || []).map((row) => ({
      rowNo: row.rowNo || row.row_no,
      plantCount: row.plantCount || row.plant_count
    }))
  }));
  activePlotIndex = 0;
  needsRebuild = false;

  form.siteName.value = payload.siteName || '';
  form.cropType.value = payload.cropType || '';
  form.plotsCount.value = plots.length || 1;
  if (structureSelect) structureSelect.value = payload.structureCode || 'S3';
  if (modeSelect) modeSelect.value = payload.mode || 'Standard';

  renderPlots();
  updatePreview();
}

function updateInputMode() {
  const modeValue = inputModeSelect ? inputModeSelect.value : 'manual';
  if (importPanel) {
    importPanel.classList.toggle('hidden', modeValue !== 'excel');
  }
  const disableBuilder = modeValue === 'excel';
  if (defaultsPanel) defaultsPanel.classList.toggle('builder-disabled', disableBuilder);
  if (plotsContainer) plotsContainer.classList.toggle('builder-disabled', disableBuilder);
  if (samplingPanel) samplingPanel.classList.toggle('builder-disabled', disableBuilder);
}

async function loadImportHistory() {
  if (!importHistory) return;
  try {
    const response = await fetch('/api/imports');
    if (!response.ok) return;
    const data = await response.json();
    const history = data.history || [];
    if (!history.length) {
      importHistory.textContent = 'No imports yet.';
      return;
    }
    importHistory.textContent = history
      .map((entry) => `${entry.uploadedAt} · ${entry.fileName} · ${entry.counts?.plots || 0} plots`)
      .join('\\n');
  } catch (error) {
    importHistory.textContent = '';
  }
}

async function downloadTemplate() {
  try {
    const response = await fetch('/api/import-template');
    if (!response.ok) return;
    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'farm-rows-template.xlsx';
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  } catch (error) {
    // no-op
  }
}

async function importExcel() {
  if (!importFileInput || !importFileInput.files.length) {
    importMessages.textContent = 'Please select an Excel file.';
    return;
  }
  importMessages.textContent = 'Importing...';
  const formData = new FormData();
  formData.append('file', importFileInput.files[0]);
  try {
    const response = await fetch('/api/import', {
      method: 'POST',
      body: formData
    });
    const responseText = await response.text();
    if (!response.ok) {
      try {
        const data = JSON.parse(responseText);
        renderMessages({ errors: data.errors || ['Import failed.'], warnings: [] }, importMessages);
      } catch (error) {
        importMessages.textContent = responseText || 'Import failed.';
      }
      return;
    }
    const data = JSON.parse(responseText);
    importMessages.textContent = `Imported ${data.counts?.rows || 0} rows from ${data.counts?.plots || 0} plots.`;
    applyImportedData(data);
    loadImportHistory();
  } catch (error) {
    importMessages.textContent = 'Import failed. Please try again.';
  }
}

function applyCropDefaults() {
  const crop = form.cropType.value.trim().toLowerCase();
  const defaults = CROP_DEFAULTS[crop];
  if (!defaults) return;
  if (structureSelect) structureSelect.value = defaults.structure;
  if (modeSelect) modeSelect.value = defaults.mode;
  markNeedsRebuild();
  updatePreview();
}

function renderSamples(values) {
  sampleGrid.innerHTML = '';
  const structureCode = values.structureCode || 'S3';
  const firstPlot = values.plots[0];
  if (!firstPlot) {
    sampleGrid.innerHTML = '<div class="muted">Add plots to preview sample labels.</div>';
    return;
  }

  if (values.labelType === 'plot') {
    const shortId = buildPlotIdShort(1);
    const fullId = buildPlotIdFull(values.siteName, values.cropType, 1);
    const rowCount = firstPlot.rows ? firstPlot.rows.length : 0;
    const card = document.createElement('div');
    card.className = 'sample-card';
    card.innerHTML = `
      <div class="meta">${values.siteName || 'Site Name'} · ${values.cropType || 'Crop Type'}</div>
      <div class="plant-id">${shortId}</div>
      <div class="meta">${fullId}</div>
      <div class="meta">Rows: ${rowCount}</div>
    `;
    sampleGrid.appendChild(card);
    return;
  }

  if (values.labelType === 'row') {
    if (!firstPlot.rows || !firstPlot.rows.length) {
      sampleGrid.innerHTML = '<div class="muted">Add rows to preview row labels.</div>';
      return;
    }
    const firstRow = firstPlot.rows[0];
    const shortId = buildRowIdShort(1, 1);
    const fullId = buildRowIdFull(values.siteName, values.cropType, 1, 1);
    const card = document.createElement('div');
    card.className = 'sample-card';
    card.innerHTML = `
      <div class="meta">${values.siteName || 'Site Name'} · ${values.cropType || 'Crop Type'}</div>
      <div class="plant-id">${shortId}</div>
      <div class="meta">${fullId}</div>
      <div class="meta">Plants: ${firstRow.plant_count || 0}</div>
    `;
    sampleGrid.appendChild(card);
    return;
  }

  const plantCount = structureCode === 'S1'
    ? (firstPlot.plant_count || 0)
    : (firstPlot.rows && firstPlot.rows[0] ? firstPlot.rows[0].plant_count || 0 : 0);
  const sampleCount = Math.min(plantCount, 6);
  if (!sampleCount) {
    sampleGrid.innerHTML = '<div class="muted">Enter plant counts to preview sample labels.</div>';
    return;
  }

  for (let i = 1; i <= sampleCount; i += 1) {
    const shortId = buildPlantIdShortFlexible(structureCode, 1, 1, i);
    const fullId = buildPlantIdFullFlexible(values.siteName, values.cropType, structureCode, 1, 1, i);
    const card = document.createElement('div');
    card.className = 'sample-card';
    card.innerHTML = `
      <div class="meta">${values.siteName || 'Site Name'} · ${values.cropType || 'Crop Type'}</div>
      <div class="plant-id">${shortId}</div>
      <div class="meta">${fullId}</div>
      <div class="meta">${structureCode === 'S1' ? `Plot 1 · Plant ${pad(i, 3)}` : `Plot 1 · Row 1 · Plant ${pad(i, 3)}`}</div>
    `;
    sampleGrid.appendChild(card);
  }
}

function updatePreview() {
  updateLayoutVisibility();
  updateSamplingVisibility();
  updateInputMode();
  const values = getValues();
  const validation = validate(values);
  const warnings = [...validation.warnings];
  if (values.mode === 'Research' && (values.labelType === 'plant' || values.labelType === 'all')) {
    if (!samplingState.trackedPlants || samplingState.trackedPlants.length === 0) {
      warnings.push('Generate a sampling plan before exporting plant labels.');
    }
  }
  if (values.labelType === 'all') {
    warnings.push('CSV export is unavailable for combined labels. Export plots, rows, and plants separately.');
  }
  renderMessages({ errors: validation.errors, warnings });

  countPlots.textContent = validation.totalPlots || '-';
  countRows.textContent = validation.totalRows || '-';
  countPlants.textContent = validation.totalPlants || '-';
  const structureCode = values.structureCode || 'S3';
  const mode = values.mode || 'Standard';
  const structureHasRows = structureCode === 'S2' || structureCode === 'S3';
  const structureHasPlants = structureCode === 'S1' || structureCode === 'S3';
  const plantLabelsEnabled = structureHasPlants && (mode === 'Research' || mode === 'Full');
  const totalLabels = values.labelType === 'plot'
    ? validation.totalPlots
    : values.labelType === 'row'
      ? validation.totalRows
      : values.labelType === 'all'
        ? validation.totalPlots + (structureHasRows ? validation.totalRows : 0) + (plantLabelsEnabled ? validation.totalPlants : 0)
        : validation.totalPlants;
  countTotal.textContent = totalLabels || '-';

  const fullFormat = values.labelType === 'plot'
    ? buildPlotIdFull(values.siteName, values.cropType, 1)
    : values.labelType === 'row'
      ? buildRowIdFull(values.siteName, values.cropType, 1, 1)
      : values.labelType === 'all'
        ? buildPlotIdFull(values.siteName, values.cropType, 1)
      : buildPlantIdFullFlexible(values.siteName, values.cropType, structureCode, 1, 1, 1);
  formatPreview.textContent = fullFormat;
  totalPreview.textContent = `Total labels: ${totalLabels || 0}`;

  renderSamples(values);

  const hasErrors = validation.errors.length > 0;
  if (!hasErrors) {
    formMessages.textContent = warnings.join('\n');
  }
  pdfButton.disabled = hasErrors;
  csvButton.disabled = hasErrors || values.labelType === 'all';
}

async function exportFile(endpoint, fileName) {
  const values = getValues();
  const validation = validate(values);
  renderMessages(validation);
  if (validation.errors.length) return;

  pdfButton.disabled = true;
  csvButton.disabled = true;
  formMessages.textContent = 'Preparing export...';

  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(values)
    });

    if (!response.ok) {
      const responseText = await response.text();
      try {
        const data = JSON.parse(responseText);
        renderMessages({ errors: data.errors || ['Failed to export.'], warnings: data.warnings || [] });
      } catch (parseError) {
        formMessages.textContent = responseText || 'Failed to export.';
      }
      return;
    }

    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);

    formMessages.textContent = 'Export ready.';
  } catch (error) {
    formMessages.textContent = 'Export failed. Please try again.';
  } finally {
    pdfButton.disabled = false;
    csvButton.disabled = false;
    updatePreview();
  }
}

function handleContinue() {
  const siteName = form.siteName.value.trim();
  const cropType = form.cropType.value.trim();
  const plotCount = parsePositiveInt(form.plotsCount.value);
  const inputMode = inputModeSelect ? inputModeSelect.value : 'manual';

  if (inputMode === 'excel') {
    setupMessages.textContent = '';
    stageB.classList.remove('hidden');
    updatePreview();
    return;
  }
  const errors = [];
  if (!siteName) errors.push('Site name is required.');
  if (!cropType) errors.push('Crop type is required.');
  if (!plotCount) errors.push('Number of plots must be an integer > 0.');

  if (errors.length) {
    renderMessages({ errors, warnings: [] }, setupMessages);
    return;
  }

  setupMessages.textContent = '';
  stageB.classList.remove('hidden');

  const defaultRows = parsePositiveInt(defaultRowsInput.value) || 1;
  const defaultPlants = parsePositiveInt(defaultPlantsInput.value) || 1;

  if (!plotsData.length || needsRebuild || plotsData.length !== plotCount) {
    rebuildPlots(plotCount, defaultRows, defaultPlants);
    needsRebuild = false;
  }

  updatePreview();
}

function markNeedsRebuild(message) {
  if (stageB.classList.contains('hidden')) return;
  needsRebuild = true;
  setupMessages.textContent = message || 'Plot count changed. Click Continue to rebuild plots.';
  resetSamplingState('Sampling plan cleared. Regenerate after changes.');
}

applyDefaultsButton.addEventListener('click', () => {
  const structureCode = structureSelect ? structureSelect.value : 'S3';
  if (structureCode === 'S4') {
    return;
  }

  if (structureCode === 'S1') {
    const defaultPlantsPerPlot = parsePositiveInt(defaultPlantsPerPlotInput.value);
    if (!defaultPlantsPerPlot) {
      formMessages.textContent = 'Default plants per plot must be an integer greater than 0.';
      return;
    }
    if (applyDefaultsCheckbox.checked) {
      plotsData.forEach((plot) => {
        plot.plantCount = defaultPlantsPerPlot;
      });
    } else if (plotsData[activePlotIndex]) {
      plotsData[activePlotIndex].plantCount = defaultPlantsPerPlot;
    }
    renderPlots();
    updatePreview();
    return;
  }

  const defaultRows = parsePositiveInt(defaultRowsInput.value);
  const defaultPlants = parsePositiveInt(defaultPlantsInput.value);

  if (!defaultRows || !defaultPlants) {
    formMessages.textContent = 'Defaults must be integers greater than 0.';
    return;
  }

  if (applyDefaultsCheckbox.checked) {
    plotsData.forEach((_plot, index) => {
      applyDefaultsToPlot(index, defaultRows, defaultPlants);
    });
  } else {
    applyDefaultsToPlot(activePlotIndex, defaultRows, defaultPlants);
  }

  renderPlots();
  updatePreview();
});

copyPlotButton.addEventListener('click', () => {
  if (plotsData.length < 2) return;
  const structureCode = structureSelect ? structureSelect.value : 'S3';
  if (structureCode === 'S1') {
    const plantCount = plotsData[0].plantCount || 0;
    plotsData.forEach((plot, index) => {
      if (index === 0) return;
      plot.plantCount = plantCount;
    });
  } else if (structureCode === 'S4') {
    // nothing to copy
  } else {
    const sourceRows = plotsData[0].rows.map((row) => row.plantCount);
    plotsData.forEach((plot, index) => {
      if (index === 0) return;
      plot.rows = sourceRows.map((count, rowIndex) => ({
        rowNo: rowIndex + 1,
        plantCount: count
      }));
    });
  }
  renderPlots();
  updatePreview();
});

continueButton.addEventListener('click', handleContinue);
form.plotsCount.addEventListener('input', markNeedsRebuild);
form.cropType.addEventListener('change', applyCropDefaults);
if (inputModeSelect) {
  inputModeSelect.addEventListener('change', updatePreview);
}
if (structureSelect) {
  structureSelect.addEventListener('change', () => {
    resetSamplingState('Sampling plan cleared. Regenerate for new structure.');
    markNeedsRebuild('Structure changed. Click Continue to rebuild plots.');
    updatePreview();
  });
}
if (modeSelect) {
  modeSelect.addEventListener('change', () => {
    resetSamplingState('Sampling plan cleared. Regenerate for new mode.');
    updatePreview();
  });
}
form.addEventListener('submit', (event) => event.preventDefault());
form.addEventListener('input', updatePreview);
layoutModeSelect.addEventListener('change', updatePreview);
if (labelTypeSelect) {
  labelTypeSelect.addEventListener('change', updatePreview);
}
if (qrModeSelect) {
  qrModeSelect.addEventListener('change', updatePreview);
}
if (samplingTypeSelect) {
  samplingTypeSelect.addEventListener('change', updateSamplingVisibility);
}
if (downloadTemplateButton) {
  downloadTemplateButton.addEventListener('click', downloadTemplate);
}
if (importButton) {
  importButton.addEventListener('click', importExcel);
}
paperPresetSelect.addEventListener('change', () => {
  if (paperPresetSelect.value === 'Label3x5') {
    layoutModeSelect.value = 'single';
  }
  updatePreview();
});
previewButton.addEventListener('click', () => {
  updatePreview();
  document.getElementById('previewSection').scrollIntoView({ behavior: 'smooth' });
});

pdfButton.addEventListener('click', () => {
  const labelType = labelTypeSelect.value || 'plant';
  exportFile('/api/pdf', `farm-${labelType}-labels.pdf`);
});
csvButton.addEventListener('click', () => {
  const labelType = labelTypeSelect.value || 'plant';
  exportFile('/api/csv', `farm-${labelType}-labels.csv`);
});

if (generateSamplingButton) {
  generateSamplingButton.addEventListener('click', async () => {
    const values = getValues();
    const validation = validate(values);
    renderMessages(validation, samplingSummary);
    if (validation.errors.length) return;
    if (values.mode !== 'Research') {
      samplingSummary.textContent = 'Sampling is only available in Research mode.';
      return;
    }

    const samplingPlan = {
      samplingType: samplingTypeSelect.value,
      samplesPerPlot: document.getElementById('samplesPerPlot').value,
      samplesPerRow: document.getElementById('samplesPerRow').value,
      plots: document.getElementById('samplingPlots').value,
      rows: document.getElementById('samplingRows').value,
      seed: samplingSeedInput.value.trim()
    };

    samplingSummary.textContent = 'Generating sampling plan...';

    try {
      const response = await fetch('/api/sample', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...values, samplingPlan })
      });
      const responseText = await response.text();
      if (!response.ok) {
        try {
          const data = JSON.parse(responseText);
          renderMessages({ errors: data.errors || ['Sampling failed.'], warnings: data.warnings || [] }, samplingSummary);
        } catch (error) {
          samplingSummary.textContent = responseText || 'Sampling failed.';
        }
        return;
      }
      const data = JSON.parse(responseText);
      samplingState = {
        planId: data.samplingPlanId,
        trackedPlants: data.trackedPlants || [],
        totalSamples: data.totalSamples || 0,
        seed: data.seed
      };
      samplingSummary.textContent = `Sampling plan ready: ${samplingState.totalSamples} plants tracked (seed: ${samplingState.seed}).`;
      updatePreview();
    } catch (error) {
      samplingSummary.textContent = 'Sampling failed. Please try again.';
    }
  });
}

async function loadDefaults() {
  try {
    const response = await fetch('/api/presets');
    if (!response.ok) return;
    const data = await response.json();
    if (data?.defaults) {
      document.getElementById('labelWidth').value = data.defaults.labelWidthMm;
      document.getElementById('labelHeight').value = data.defaults.labelHeightMm;
      document.getElementById('marginTop').value = data.defaults.marginsMm.top;
      document.getElementById('marginRight').value = data.defaults.marginsMm.right;
      document.getElementById('marginBottom').value = data.defaults.marginsMm.bottom;
      document.getElementById('marginLeft').value = data.defaults.marginsMm.left;
      document.getElementById('gapX').value = data.defaults.gapsMm.x;
      document.getElementById('gapY').value = data.defaults.gapsMm.y;
      document.getElementById('safeMargin').value = data.defaults.safeMarginMm;
      document.getElementById('includeBleed').checked = Boolean(data.defaults.includeBleed);
    }
  } catch (error) {
    // Defaults fall back to HTML values.
  }
}

loadDefaults().finally(() => {
  loadImportHistory();
  updatePreview();
});
