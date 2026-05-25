"""HTML templates for MCP Apps rendered in compatible hosts.

Each template loads the MCP Apps SDK from unpkg, receives tool output via
``ontoolresult``, and renders an interactive view. Vanilla JS — no build step.
"""

DAG_GRAPH_URI = "ui://mwaa/dag-graph"

DAG_GRAPH_HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="color-scheme" content="light dark">
  <title>DAG Dependency Graph</title>
  <style>
    :root {
      --fg: light-dark(#1f2328, #e6edf3);
      --bg: light-dark(#ffffff, #0d1117);
      --muted: light-dark(#656d76, #8b949e);
      --border: light-dark(#d0d7de, #30363d);
      --accent: light-dark(#0969da, #58a6ff);
    }
    html, body { margin: 0; padding: 0; background: transparent; color: var(--fg); }
    body {
      font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
      padding: 12px;
    }
    h1 { font-size: 14px; margin: 0 0 6px; font-weight: 600; }
    .meta { color: var(--muted); font-size: 12px; margin-bottom: 12px; }
    .graph-wrap {
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px;
      overflow: auto;
      background: var(--bg);
    }
    .error { color: #cf222e; padding: 12px; }
    .mermaid { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    button.copy {
      background: transparent; border: 1px solid var(--border); border-radius: 6px;
      color: var(--fg); padding: 4px 8px; font-size: 12px; cursor: pointer;
      margin-left: 8px;
    }
  </style>
</head>
<body>
  <h1 id="title">Loading DAG graph...</h1>
  <div class="meta" id="meta"></div>
  <div class="graph-wrap" id="wrap">
    <div id="content">Waiting for tool output...</div>
  </div>

  <script type="module">
    import { App } from "https://unpkg.com/@modelcontextprotocol/ext-apps@0.4.0/app-with-deps";
    import mermaid from "https://unpkg.com/mermaid@10/dist/mermaid.esm.min.mjs";

    mermaid.initialize({ startOnLoad: false, theme: matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'default' });

    const titleEl = document.getElementById('title');
    const metaEl = document.getElementById('meta');
    const contentEl = document.getElementById('content');

    function renderError(msg) {
      contentEl.innerHTML = `<div class="error">${msg}</div>`;
    }

    async function renderGraph(data) {
      const dagId = data?.summary?.dag_id || 'unknown';
      const env = data?.summary?.environment_name || '';
      const nodeCount = data?.summary?.node_count ?? (data?.nodes?.length ?? 0);
      const edgeCount = data?.summary?.edge_count ?? (data?.edges?.length ?? 0);
      titleEl.textContent = `${dagId}`;
      metaEl.textContent = `${env} • ${nodeCount} tasks • ${edgeCount} edges`;

      const mermaidSrc = data?.mermaid || '';
      if (!mermaidSrc) {
        renderError('No mermaid graph in tool output.');
        return;
      }
      try {
        const { svg } = await mermaid.render('dag-svg', mermaidSrc);
        contentEl.innerHTML = svg;
      } catch (err) {
        renderError(`Mermaid render error: ${err.message}`);
      }
    }

    async function main() {
      const app = new App();
      app.ontoolresult = (result) => {
        try {
          const payload = result?.structuredContent ?? result?.content ?? result;
          renderGraph(payload);
        } catch (err) {
          renderError(`Render error: ${err.message}`);
        }
      };
      app.onhostcontextchanged = () => {};
      await app.connect();
    }
    main().catch(err => renderError(`App init failed: ${err.message}`));
  </script>
</body>
</html>
"""


RUN_HEATMAP_URI = "ui://mwaa/run-heatmap"

RUN_HEATMAP_HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="color-scheme" content="light dark">
  <title>Run History Heatmap</title>
  <style>
    :root {
      --fg: light-dark(#1f2328, #e6edf3);
      --bg: light-dark(#ffffff, #0d1117);
      --muted: light-dark(#656d76, #8b949e);
      --border: light-dark(#d0d7de, #30363d);
      --success: #2da44e;
      --failed: #cf222e;
      --upstream-failed: #bf8700;
      --skipped: #8b949e;
      --running: #0969da;
      --queued: #6e7681;
      --empty: light-dark(#f6f8fa, #161b22);
    }
    html, body { margin: 0; padding: 0; background: transparent; color: var(--fg); }
    body {
      font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
      padding: 12px;
    }
    h1 { font-size: 14px; margin: 0 0 6px; font-weight: 600; }
    .meta { color: var(--muted); font-size: 12px; margin-bottom: 12px; }
    table { border-collapse: collapse; }
    th, td {
      border: 1px solid var(--border);
      padding: 0;
      vertical-align: middle;
    }
    th {
      font-weight: 500;
      font-size: 11px;
      padding: 4px 8px;
      text-align: left;
      background: var(--bg);
      position: sticky;
      left: 0;
    }
    td.cell {
      width: 22px;
      height: 22px;
      cursor: pointer;
      text-align: center;
    }
    td.success { background: var(--success); }
    td.failed { background: var(--failed); }
    td.upstream_failed { background: var(--upstream-failed); }
    td.skipped { background: var(--skipped); }
    td.running { background: var(--running); }
    td.queued { background: var(--queued); }
    td.empty { background: var(--empty); }
    .legend { display: flex; gap: 12px; font-size: 11px; margin-top: 12px; align-items: center; }
    .legend span.swatch { display: inline-block; width: 12px; height: 12px; border-radius: 2px; vertical-align: middle; margin-right: 4px; }
    .tip {
      position: absolute; background: var(--bg); color: var(--fg);
      border: 1px solid var(--border); border-radius: 6px;
      padding: 6px 10px; font-size: 12px; pointer-events: none;
      z-index: 99; display: none; max-width: 320px;
    }
    .error { color: var(--failed); padding: 12px; }
  </style>
</head>
<body>
  <h1 id="title">Loading run history...</h1>
  <div class="meta" id="meta"></div>
  <div id="content">Waiting for tool output...</div>
  <div id="tip" class="tip"></div>
  <div class="legend" id="legend"></div>

  <script type="module">
    import { App } from "https://unpkg.com/@modelcontextprotocol/ext-apps@0.4.0/app-with-deps";

    const titleEl = document.getElementById('title');
    const metaEl = document.getElementById('meta');
    const contentEl = document.getElementById('content');
    const tipEl = document.getElementById('tip');
    const legendEl = document.getElementById('legend');

    const STATE_ORDER = ['success', 'failed', 'upstream_failed', 'skipped', 'running', 'queued'];

    function renderError(msg) {
      contentEl.innerHTML = `<div class="error">${msg}</div>`;
    }

    function renderLegend() {
      legendEl.innerHTML = STATE_ORDER.map(s => `<span><span class="swatch" style="background:var(--${s.replace('_', '-')})"></span>${s}</span>`).join('');
    }

    function showTip(e, text) {
      tipEl.textContent = text;
      tipEl.style.display = 'block';
      tipEl.style.left = (e.pageX + 12) + 'px';
      tipEl.style.top = (e.pageY + 12) + 'px';
    }
    function hideTip() { tipEl.style.display = 'none'; }

    function renderHeatmap(data) {
      const cells = data?.cells || [];
      const tasks = data?.task_ids || [];
      const dates = data?.execution_dates || [];
      const dagId = data?.summary?.dag_id || 'unknown';
      const env = data?.summary?.environment_name || '';

      titleEl.textContent = `${dagId}`;
      metaEl.textContent = `${env} • ${tasks.length} tasks • ${dates.length} runs`;

      if (!cells.length) {
        renderError('No cells in tool output.');
        return;
      }

      // Index cells by (task_id, execution_date)
      const cellIndex = {};
      for (const c of cells) {
        cellIndex[`${c.task_id}\\u0000${c.execution_date}`] = c;
      }

      const rows = tasks.map(task => {
        const tds = dates.map(date => {
          const c = cellIndex[`${task}\\u0000${date}`];
          const state = c?.state || 'empty';
          const tip = c
            ? `${task}\\n${date}\\nstate=${state}\\nrun=${c.dag_run_id || ''}`
            : `${task} — no run`;
          return `<td class="cell ${state}" data-tip="${tip.replace(/"/g, '&quot;')}" data-run="${c?.dag_run_id || ''}" data-task="${task}"></td>`;
        }).join('');
        return `<tr><th>${task}</th>${tds}</tr>`;
      }).join('');

      contentEl.innerHTML = `
        <table>
          <thead>
            <tr><th></th>${dates.map(d => `<th title="${d}">${d.slice(5,10)}</th>`).join('')}</tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      `;
      renderLegend();

      contentEl.querySelectorAll('td.cell').forEach(td => {
        td.addEventListener('mouseenter', (e) => showTip(e, td.dataset.tip));
        td.addEventListener('mouseleave', hideTip);
      });
    }

    async function main() {
      const app = new App();
      app.ontoolresult = (result) => {
        try {
          const payload = result?.structuredContent ?? result?.content ?? result;
          renderHeatmap(payload);
        } catch (err) {
          renderError(`Render error: ${err.message}`);
        }
      };
      app.onhostcontextchanged = () => {};
      await app.connect();
    }
    main().catch(err => renderError(`App init failed: ${err.message}`));
  </script>
</body>
</html>
"""
