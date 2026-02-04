/*
 * Copyright (c) 2026 OPSCURE.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of OPSCURE.
 * Unauthorized copying, modification, distribution, or use of this software,
 * via any medium, is strictly prohibited without prior written permission.
 *
 * Licensed under the OPSCURE Software License Agreement.
 */

const vscode = require("vscode");
const axios = require("axios");
const path = require("path");
const { spawn, execSync } = require("child_process");
const fs = require("fs");
const { EventSource } = require("eventsource");

let goAgentProcess = null;
let AGENT_BASE = null;
let fixEventSource = null;

/* ================= HARD PROCESS KILL ================= */
function killProcessTree(pid) {
  try {
    if (process.platform === "win32") {
      execSync(`taskkill /PID ${pid} /T /F`);
    } else {
      process.kill(-pid, "SIGKILL");
    }
  } catch {}
}

function killByPort(port) {
  try {
    if (process.platform === "win32") {
      const out = execSync(`netstat -ano | findstr LISTENING | findstr :${port}`).toString();
      out.split("\n").forEach(line => {
        const parts = line.trim().split(/\s+/);
        const pid = parts[parts.length - 1];
        if (pid && /^\d+$/.test(pid)) {
          execSync(`taskkill /PID ${pid} /T /F`);
        }
      });
    } else {
      execSync(`lsof -ti tcp:${port} | xargs kill -9`);
    }
  } catch {}
}

const CONFIG_FILE = "folder.config.json";

function getFolderConfigPath(context) {
  return path.join(context.extensionPath, "server", CONFIG_FILE);
}

function readFolderConfig(context) {
  const configPath = getFolderConfigPath(context);
  if (!fs.existsSync(configPath)) {
    fs.writeFileSync(configPath, JSON.stringify({ folders: [] }, null, 2));
  }
  return JSON.parse(fs.readFileSync(configPath, "utf8"));
}

function saveFolderConfig(context, data) {
  fs.writeFileSync(
    getFolderConfigPath(context),
    JSON.stringify(data, null, 2)
  );
}

/* ================= AGENT PORT ================= */
function waitForAgentPort(extPath, timeoutMs = 1500, intervalMs = 50) {
  const portFile = path.join(extPath, "server", "agent.port");
  const start = Date.now();

  return new Promise((resolve, reject) => {
    const timer = setInterval(() => {
      try {
        if (fs.existsSync(portFile)) {
          const port = fs.readFileSync(portFile, "utf8").trim();
          if (/^\d+$/.test(port)) {
            clearInterval(timer);
            return resolve(port);
          }
        }
      } catch {}

      if (Date.now() - start > timeoutMs) {
        clearInterval(timer);
        reject(new Error("Timed out waiting for agent.port"));
      }
    }, intervalMs);
  });
}

/* ================= ACTIVATE ================= */
function activate(context) {
  // Default view: show OPSCURE execution UI
  vscode.commands.executeCommand(
    "setContext",
    "opscure.showFolderManager",
    false
  );

  try {
    const isWin = process.platform === "win32";
    const goAgentBinary = isWin ? "go-agent.exe" : "go-agent";
    const goAgentPath = path.join(context.extensionPath, "server", goAgentBinary);
    const goAgentCwd = path.join(context.extensionPath, "server", "go_agent");

    goAgentProcess = spawn(goAgentPath, [], {
      cwd: goAgentCwd,
      windowsHide: true,
      stdio: "inherit"
    });

    waitForAgentPort(context.extensionPath)
      .then(port => {
        AGENT_BASE = `http://127.0.0.1:${port}`;
        vscode.window.showInformationMessage("OPSCURE agent connected on port " + port);
      })
      .catch(() => vscode.window.showErrorMessage("Go agent port not detected"));

    context.subscriptions.push({
      dispose: () => {
        if (goAgentProcess?.pid) killProcessTree(goAgentProcess.pid);
      }
    });
  } catch {
    vscode.window.showErrorMessage("Failed to start Go agent sidecar");
  }

  const folderManagerProvider = new FolderManagerViewProvider(context);

  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(
      "folderManagerView",
      folderManagerProvider
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("opscure.openFolderManager", async () => {
      // Hide OPSCURE, show Folder Manager
      await vscode.commands.executeCommand(
        "setContext",
        "opscure.showFolderManager",
        true
      );

      await vscode.commands.executeCommand(
        "workbench.view.extension.logFetcherContainer"
      );
    })
  );

  const provider = new LogFetcherViewProvider();
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider("logFetcherView", provider)
  );

  let capturing = false;
  let batch = [];
  let pendingQueue = [];
  let sending = false;
  let folderEventSource = null;

  function startFolderStream(folderPath, provider) {
    if (folderEventSource) {
      folderEventSource.close();
      folderEventSource = null;
    }

    const url = `${AGENT_BASE}/logs/folder/stream?paths=${encodeURIComponent(folderPath)}`;
    folderEventSource = new EventSource(url);

    folderEventSource.onmessage = (e) => {
      try {
        const log = JSON.parse(e.data);

        // 1. Show in COMMS_STREAM
        provider.postCapturedLog({
          severity: log.level || "INFO",
          message: log.message,
          service: log.service,
          timestamp: log.timestamp,
          file: log.file,
          folder: log.folder
        });

        // 2. Push into batch system (DO NOT remove batch logic)
        pushLogFromAgent(log);

      } catch (err) {
        console.error("Folder stream parse error", err);
      }
    };

    folderEventSource.onerror = () => {
      provider.postFixStatus("❌ Folder log stream disconnected.");
    };
  }

  function pushLogFromAgent(agentLog) {

    const log = {
      severity: agentLog.level || "INFO", 
      timestamp: agentLog.timestamp,
      message: agentLog.message,
      raw: agentLog.message
    };

    batch.push(log);

    if (batch.length >= 50) {
      pendingQueue.push({ logs: batch });
      batch = [];  
      processQueue();
    }
  }

  async function processQueue() {
    if (sending || pendingQueue.length === 0) return;
    sending = true;
    const payload = pendingQueue.shift();
    await sendBatch(payload, provider);
    sending = false;
    processQueue();
  }


  provider.onMessage = async (msg) => {
    if (msg.type === "loadFolders") {
      provider.view.webview.postMessage({
        type: "folders",
        data: readFolderConfig(context).folders
      });
    }

    if (msg.type === "start") {
      capturing = true;

      if (!AGENT_BASE) {
        vscode.window.showErrorMessage("Go agent not connected yet.");
        return;
      }

      // if (!msg.folderPath) {
      //   vscode.window.showErrorMessage("Folder path is required.");
      //   return;
      // }

      vscode.window.showInformationMessage("Connecting to folder log stream...");

      startFolderStream(msg.folderPath, provider);
    }
    if (msg.type === "stop") {
      capturing = false;
      if (folderEventSource) {
        folderEventSource.close();
        folderEventSource = null;
        vscode.window.showInformationMessage("Folder log stream stopped.");
      }
    }
    if (msg.type === "analyze") provider.sendForAnalyze();
    if (msg.type === "acceptFix") {
      await applyFixFromAI(provider);
    }
    if (msg.type === "ignoreFix") {
      vscode.window.showInformationMessage("Fix ignored by user.");
    }
  };

  context.subscriptions.push({
    dispose: () => {
      if (fixEventSource) fixEventSource.close();
    }
  });
  vscode.window.showInformationMessage("OPSCURE Activated!");
}

function deactivate() {
  if (goAgentProcess?.pid) killProcessTree(goAgentProcess.pid);
  if (fixEventSource) fixEventSource.close();
}

/* ===================== LOG VIEW PROVIDER ===================== */
class LogFetcherViewProvider {
  constructor() {
    this.view = undefined;
    this.parsedResponses = [];
    this.rawBundles = [];
    this.bundleCounter = 1;
    this.onMessage = undefined;
    this.lastAnalyzeResponse = null;
  }

  resolveWebviewView(view) {
    this.view = view;
    view.webview.options = { enableScripts: true };
    view.webview.html = this.getHtml();
    view.webview.onDidReceiveMessage((msg) => {
      this.onMessage?.(msg);
    });
  }

  postCapturedLog(log) {
    this.view?.webview.postMessage({ type: "captured", data: log });
  }

  postParsedLog(log) {
    const parsedLog = {
      severity: log.severity || "INFO",
      message: log.message || "",
      service: log.service || "unknown",
      timestamp: log.timestamp || new Date().toISOString()
    };
    this.parsedResponses.push(parsedLog);
    this.view?.webview.postMessage({ type: "parsed", data: parsedLog });
  }

  postFixStatus(msg) {
    this.view?.webview.postMessage({ type: "fixStatus", data: msg });
  }

  // --- API TRAFFIC HELPER ---
  postApiTraffic(endpoint, request, response) {
    this.view?.webview.postMessage({
      type: "apiTraffic",
      data: { endpoint, request, response, time: new Date().toLocaleTimeString() }
    });
  }

  storeBundle(bundle) {
    if (bundle) this.rawBundles.push(bundle);
  }

  async sendForAnalyze() {
    if (!AGENT_BASE) {
      vscode.window.showWarningMessage("Agent not ready yet.");
      return;
    }
    if (!this.rawBundles.length) return;
    const sequence = [];
    this.rawBundles.forEach(b => {
      const seq = b.Sequence || b.sequence;
      if (Array.isArray(seq)) {
        seq.forEach(item => {
          if (item?.Data) {
            sequence.push({ Data: item.Data });
          }
        });
      }
    });
    if (!sequence.length) return;
    const today = new Date();
    const dateStr = String(today.getDate()).padStart(2, "0") + String(today.getMonth() + 1).padStart(2, "0") + today.getFullYear();
    const bundleId = `bundle${dateStr}_${String(this.bundleCounter++).padStart(2, "0")}`;
    const workspace = vscode.workspace.rootPath;
    const gitConfig = workspace ? getGitConfig(workspace) : null;
    const requestBody = {
      bundle: {
        id: bundleId,
        Sequence: sequence,
        git_config: gitConfig
      }
    };

    try {
      const res = await axios.post(`${AGENT_BASE}/logs/preprocess`, requestBody, { headers: { "Content-Type": "application/json" } });
      this.lastAnalyzeResponse = res.data;
      
      // LOG TRAFFIC FOR TESTING
      this.postApiTraffic("/logs/preprocess", requestBody, res.data);
      
      this.view?.webview.postMessage({ type: "analyzeResponse", data: res.data });
    } catch (e) {
      this.postApiTraffic("/logs/preprocess (ERROR)", requestBody, e.response?.data || e.message);
    }
  }

  getHtml() {
    return `<!DOCTYPE html>
    <html>
    <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
    :root {
      --neon: #00f2ff;
      --warn: #ffcc00;
      --crit: #ff3131;
      --bg: #020408;
      --font-mono: 'JetBrains Mono', 'Fira Code', monospace;
    }

    * { box-sizing: border-box; scrollbar-width: none; }
    *::-webkit-scrollbar { display: none; }

    body {
      margin: 0; padding: 0;
      background-color: var(--bg);
      color: #fff;
      font-family: var(--font-mono);
      font-size: clamp(13px, 1.2vw, 16px);
      height: 100vh;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      background-image: 
        linear-gradient(rgba(0, 242, 255, 0.05) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0, 242, 255, 0.05) 1px, transparent 1px);
      background-size: 40px 40px;
    }

    header {
      height: 60px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 0 25px;
      border-bottom: 1px solid rgba(0, 242, 255, 0.2);
      background: rgba(0,0,0,0.8);
      position: relative;
      flex-shrink: 0;
    }


    .title { 
      letter-spacing: 3px; 
      font-weight: 900; 
      font-size: clamp(14px, 1.4vw, 18px); 
      color: var(--neon); 
    }

    /* --- DYNAMIC VIEWPORT SCALING --- */
    .main-deck {
      flex: 1;
      display: flex;
      flex-direction: row; /* Default Desktop */
      gap: 15px;
      padding: 15px;
      overflow: hidden;
    }

    @media (max-width: 900px) {
      .main-deck { 
        flex-direction: column; /* Sidebar Mode */
      }
      
      .panel-intel {
        flex: 1.6;   /* increase this number to grow height */
      }

      .panel-comms {
        flex: 1;     /* default weight */
      }
      /* Priority Scaling for Sidebar */
      .panel-intel { 
        flex: 0 1 auto; /* Grow to fit, but shrink if needed */
        max-height: 50%; /* Never take more than half the screen */
        font-size: clamp(13px, 1.1vw, 15px);
      }
      
      .panel-comms { 
        flex: 1 1 40%; /* Take at least 40% and grow to fill the rest */
        min-height: 200px; 
      }
    }

    /* --- PANEL SYSTEM --- */
    .panel {
      background: rgba(255,255,255,0.02);
      border: 1px solid rgba(0, 242, 255, 0.1);
      position: relative;
      display: flex;
      flex-direction: column;
      padding: 15px;
      backdrop-filter: blur(10px);
      overflow: hidden;
    }

    .panel-intel { width: 350px; } /* Desktop width */
    @media (max-width: 900px) { .panel-intel { width: 100%; } }

    .panel::before, .panel::after {
      content: ''; position: absolute; width: 8px; height: 8px;
      border-color: var(--neon); border-style: solid;
    }
    .panel::before { top: -1px; left: -1px; border-width: 2px 0 0 2px; }
    .panel::after { bottom: -1px; right: -1px; border-width: 0 2px 2px 0; }

    .panel-label { font-size: clamp(11px, 1vw, 13px); color: var(--warn); margin-bottom: 15px; letter-spacing: 2px; flex-shrink: 0; }

    /* --- INTERNAL SCROLLING --- */
    .scroll-content {
      flex: 1;
      overflow-y: auto;
      overflow-x: hidden;
    }

    /* --- LOG ENTRY STYLING --- */
    .log-entry {
      padding: 8px;
      border-bottom: 1px solid rgba(255,255,255,0.03);
      display: flex;
      gap: 10px;
      font-size: clamp(12px, 1.1vw, 14px);
      word-break: break-all;
    }

    .log-entry.ERROR { color: var(--crit); background: rgba(255, 49, 49, 0.05); border-left: 2px solid var(--crit); }

    /* --- METRICS --- */
    .metric-row {
      display: flex; gap: 10px; margin-bottom: 15px;
    }
    .metric-stat {
      flex: 1; background: rgba(0, 242, 255, 0.05); padding: 10px; border: 1px solid rgba(0, 242, 255, 0.1);
    }
    .metric-val { font-size: clamp(20px, 2vw, 28px); font-weight: 900; display: block; color: var(--neon); }
    .metric-tag { font-size: clamp(9px, 0.8vw, 11px); opacity: 0.6; text-transform: uppercase; }

    .header-top {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .status {
      font-size: 9px;
      color: var(--neon);
    }

    .header-controls {
      margin-top: 10px;
      display: flex;
      align-items: center;
      gap: 12px;
    }

    header {
      height: auto;        /* IMPORTANT */
      padding: 12px 25px;  /* instead of fixed height */
      flex-direction: column;
      align-items: stretch;
    }

    .btn-wow {
      background: var(--neon); color: #000; border: none; padding: 10px 20px;
      font-weight: 900; cursor: pointer; margin-left: 15px;
      clip-path: polygon(10% 0%, 100% 0%, 90% 100%, 0% 100%);
      font-size: clamp(12px, 1vw, 14px);
    }

    .btn-wow:disabled { background: #1a1a1a; color: #444; }

    .log-meta {
      min-width: 180px;
      opacity: 0.75;
      line-height: 1.4;
      font-size: clamp(12px, 1.05vw, 14px);
    }

    .log-time {
      color: var(--neon);
      font-weight: 600;
      font-size: clamp(12px, 1vw, 14px);
    }

    .log-path {
      color: #9befff;
      font-size: clamp(12px, 1.05vw, 14px);
    }

    .log-file {
      color: #b8ffea;
      font-size: clamp(12px, 1.05vw, 14px);
    }

    .log-message {
      flex: 1;
      margin-top: 4px;
      font-size: clamp(13px, 1.15vw, 15px);
      line-height: 1.55;
    }

    /* --- COMMAND BAR --- */
    .command-bar {
      display: flex;
      align-items: center;
      gap: 14px;
      width: 100%;
    }

    .prompt {
      color: var(--neon);
      font-weight: 900;
      font-size: 14px;
    }

    /* --- SELECT STYLING --- */
    .select-wrap {
      flex: 1;
      position: relative;
    }

    /* --- SELECT DROPDOWN READABILITY FIX --- */
    select option[disabled] {
      color: rgba(0,242,255,0.4);
    }

    select {
      background-color: rgba(5, 15, 20, 0.95); /* dark base */
      color: var(--neon);
    }

    /* Dropdown options (works in most VS Code builds) */
    select option {
      background-color: #020b10;  /* deep tactical black */
      color: #00f2ff;             /* neon text */
    }

    /* Selected option */
    select option:checked {
      background-color: #003840;  /* teal highlight */
      color: #ffffff;
    }

    /* Hover (best-effort, browser dependent) */
    select option:hover {
      background-color: #004b55;
    }

    /* --- BUTTONS --- */
    .btn-primary {
      background: var(--neon);
      color: #000;
      border: none;
      padding: 12px 22px;
      font-weight: 900;
      cursor: pointer;
      clip-path: polygon(12% 0%, 100% 0%, 88% 100%, 0% 100%);
    }

    .btn-primary:disabled {
      background: #1a1a1a;
      color: #444;
      cursor: not-allowed;
    }

    .btn-halt {
      background: transparent;
      border: 1px solid rgba(255,49,49,0.6);
      color: var(--crit);
      padding: 10px 16px;
      font-size: 11px;
      cursor: pointer;
      letter-spacing: 1px;
    }

    </style>
    </head>

    <body>

    <header>
      <div class="header-top">
        <div class="title">OPSCURE // TACTICAL_DECK</div>
        <div class="status">LINK_ESTABLISHED</div>
      </div>

      <div class="header-controls">
        <span class="prompt">$</span>

        <div class="select-wrap">
          <select id="folderSelect"></select>
        </div>

        <button id="startBtn" class="btn-primary" onclick="start()" disabled>
          ▶ EXECUTE
        </button>

        <button class="btn-halt" onclick="stopLogs()">
          ■ HALT
        </button>
      </div>
    </header>

    <div class="main-deck">
      <div class="panel panel-intel">
        <div class="panel-label">// INTELLIGENCE_MONITOR</div>
        <div id="analyzeRes" class="scroll-content">
            <div style="text-align: center; margin-top: 30px; opacity: 0.2; font-size: 10px;">AWAITING DATA STREAM...</div>
        </div>
        <div style="margin-top: 15px; flex-shrink: 0;">
            <div style="border-left: 2px solid var(--neon); padding-left: 10px; margin-bottom: 10px;">
                <span style="font-size: 18px; font-weight: 900;" id="logCount">000</span>
                <div style="font-size: 7px; opacity: 0.5;">INGESTED_PACKETS</div>
            </div>
            <button id="scanBtn" class="btn-wow" style="width:100%; margin:0; display:none;" onclick="runScan()">INITIATE SCAN</button>
        </div>
      </div>

      <div class="panel panel-comms">
        <div class="panel-label">// COMMS_STREAM</div>
        <div class="scroll-content" id="captured"></div>
      </div>
    </div>

    <script>
    const vscode = acquireVsCodeApi();

    // Request folder list when OPSCURE UI loads
    vscode.postMessage({ type: "loadFolders" });


    document.getElementById("folderSelect").addEventListener("change", validate);

    let count = 0;

    function validate(){
      const select = document.getElementById("folderSelect");
      document.getElementById("startBtn").disabled = !select.value;
    }

    function start(){
      const folder = document.getElementById("folderSelect").value;
      vscode.postMessage({
        type: "start",
        folderPath: folder
      });
    }

    function stopLogs(){
      vscode.postMessage({type:'stop'});
    }

    function runScan(){
      vscode.postMessage({type:"analyze"});
      const b = document.getElementById("scanBtn");
      b.textContent = "SCANNING...";
      b.disabled = true;
    }

    window.addEventListener("message", e => {
      const { type, data } = e.data;

      if(type==="captured"){
        count++;
        document.getElementById("logCount").textContent = count.toString().padStart(3, '0');
        const container = document.getElementById("captured");
        const div = document.createElement("div");
        div.className = "log-entry " + (data.severity || "");
        div.innerHTML = \`
          <div class="log-meta">
            <div class="log-time">[\${new Date().toLocaleTimeString([], {hour12:false})}]</div>
            <div class="log-path">📁 \${data.folder || "unknown"}</div>
            <div class="log-file">📄 \${data.file || data.service || "unknown"}</div>
          </div>
          <div class="log-message">
            \${data.message}
          </div>\`;
        container.prepend(div);
      }

      if(type==="parsed"){
        const b = document.getElementById("scanBtn");
        b.style.display = "block";
        b.textContent = "RUN DIAGNOSTICS";
        b.disabled = false;
      }

      if(type==="analyzeResponse"){
        document.getElementById("scanBtn").textContent = "RE-SCAN";
        document.getElementById("scanBtn").disabled = false;
        renderAnalyze(data.analyze_response, data.preprocess_response);
      }

      if(type==="fixStatus"){
        const monitor = document.getElementById("analyzeRes");
        const div = document.createElement("div");
        div.style.color = "var(--neon)";
        div.style.fontSize = "10px";
        div.style.padding = "8px";
        div.style.borderLeft = "2px solid var(--neon)";
        div.style.background = "rgba(0, 242, 255, 0.05)";
        div.style.marginTop = "8px";
        div.textContent = "> " + data;
        monitor.appendChild(div);
        monitor.scrollTop = monitor.scrollHeight;
      }
      if(type === "folders"){
        const select = document.getElementById("folderSelect");
        select.innerHTML = "";

        // Default placeholder option
        const placeholder = document.createElement("option");
        placeholder.value = "";
        placeholder.textContent = "Select log folder";
        placeholder.disabled = true;
        placeholder.selected = true;
        select.appendChild(placeholder);

        // Actual folders
        data.forEach(f => {
          const o = document.createElement("option");
          o.value = f.path;
          o.textContent = f.label;
          select.appendChild(o);
        });

        // Keep EXECUTE disabled until user selects a folder
        document.getElementById("startBtn").disabled = true;
      }
    });

    function renderAnalyze(res, preprocess){
      const root = res.recommendation;
      if(!root) return;
      const rca = root.root_cause_analysis || {};
      const metrics = preprocess?.bundle?.metrics || {};
      const rec = (root.recommendations||[])[0];

      document.getElementById("analyzeRes").innerHTML = \`
        <div style="margin-bottom: 15px;">
            <div style="color:var(--neon); font-weight:900; font-size:clamp(15px,1.4vw,18px); margin-bottom:5px;">\${rca.summary||"Incident Detected"}</div>
            <div style="font-size:9px; opacity:0.8; line-height:1.4;">\${rca.primary_cause || "Analyzing logic..."}</div>
        </div>

        <div class="metric-row">
            <div class="metric-stat">
                <span class="metric-val" style="color:var(--crit)">\${metrics.errorRateZ || '0.0'}</span>
                <span class="metric-tag">ERR_VEC</span>
            </div>
            <div class="metric-stat">
                <span class="metric-val">\${metrics.cpuZ || '0.0'}</span>
                <span class="metric-tag">SYS_LOAD</span>
            </div>
        </div>

        \${rec ? \`
          <div style="padding: 10px; border: 1px solid var(--warn); background: rgba(255, 204, 0, 0.05);">
            <div style="font-size:clamp(13px,1.1vw,15px); margin-bottom: 10px;">\${rec.description}</div>
            <button class="btn-wow" onclick="vscode.postMessage({type:'acceptFix'})" style="width:100%; margin:0; height:30px; font-size:9px; clip-path:none;">APPLY PATCH</button>
          </div>
        \` : ""}
      \`;
    }
    </script>
    </body>
    </html>`;
  }
}

class FolderManagerViewProvider {
  constructor(context) {
    this.context = context;
    this.view = null;
  }

  resolveWebviewView(view) {
    this.view = view;
    view.webview.options = { enableScripts: true };
    view.webview.html = this.getHtml();

    view.webview.onDidReceiveMessage(msg => {
      if (msg.type === "load") {
        this.view.webview.postMessage({
          type: "folders",
          data: readFolderConfig(this.context).folders
        });
      }

      if (msg.type === "back") {
        // Hide Folder Manager, show OPSCURE
        vscode.commands.executeCommand(
          "setContext",
          "opscure.showFolderManager",
          false
        );

        vscode.commands.executeCommand(
          "workbench.view.extension.logFetcherContainer"
        );
        return;
      }
      if (msg.type === "add") {
        const cfg = readFolderConfig(this.context);
        cfg.folders.push(msg.data);
        saveFolderConfig(this.context, cfg);

        this.view.webview.postMessage({
          type: "folders",
          data: cfg.folders
        });
      }

      if (msg.type === "update") {
        const cfg = readFolderConfig(this.context);
        const idx = cfg.folders.findIndex(f => f.id === msg.data.id);
        if (idx !== -1) {
          cfg.folders[idx] = msg.data;
          saveFolderConfig(this.context, cfg);
        }

        this.view.webview.postMessage({
          type: "folders",
          data: cfg.folders
        });
      }

      if (msg.type === "delete") {
        const cfg = readFolderConfig(this.context);
        cfg.folders = cfg.folders.filter(f => f.id !== msg.id);
        saveFolderConfig(this.context, cfg);

        this.view.webview.postMessage({
          type: "folders",
          data: cfg.folders
        });
      }
    });
  }

  getHtml() {
    /* SAME CSS TOKENS AS MAIN UI */
    return `
      <!DOCTYPE html>
      <html>
      <head>
      <style>
      :root {
        --neon: #00f2ff;
        --bg: #020408;
        --font: 'JetBrains Mono', monospace;
        --crit: #ff3131;
      }

      * {
        box-sizing: border-box;
      }

      body {
        background: var(--bg);
        color: #fff;
        font-family: var(--font);
        margin: 0;
        padding: 12px;
        height: 100vh;
        display: flex;
        flex-direction: column;
        overflow: hidden;
      }

      .header {
        flex-shrink: 0;
      }

      h3 {
        color: var(--neon);
        letter-spacing: 2px;
        margin: 10px 0;
        font-size: 13px;
      }

      button {
        background: var(--neon);
        border: none;
        padding: 8px 10px;
        font-weight: 900;
        cursor: pointer;
        font-size: 11px;
      }

      button.secondary {
        background: transparent;
        color: var(--neon);
        border: 1px solid rgba(0,242,255,.3);
      }

      button.danger {
        background: transparent;
        color: var(--crit);
        border: 1px solid rgba(255,49,49,.6);
      }

      input {
        width: 100%;
        background: transparent;
        border: 1px solid rgba(0,242,255,.3);
        color: var(--neon);
        padding: 8px;
        margin-bottom: 8px;
        font-size: 12px;
      }

      .form {
        flex-shrink: 0;
      }

      .list {
        flex: 1;
        overflow-y: auto;
        margin-top: 10px;
        border-top: 1px solid rgba(0,242,255,.15);
        padding-top: 10px;
      }

      .folder-row {
        border: 1px solid rgba(0,242,255,.15);
        padding: 8px;
        margin-bottom: 8px;
        font-size: 11px;
      }

      .folder-label {
        color: var(--neon);
        font-weight: 700;
      }

      .folder-path {
        opacity: .7;
        word-break: break-all;
        margin: 4px 0;
      }

      .actions {
        display: flex;
        gap: 6px;
        margin-top: 6px;
      }
      </style>
      </head>

      <body>
        <div class="header">
          <button class="secondary" onclick="goBack()">⬅ BACK</button>
          <h3>// FOLDER_MANAGER</h3>
        </div>

        <div class="form">
          <input id="label" placeholder="LABEL" />
          <input id="path" placeholder="FOLDER PATH" />
          <button onclick="save()">SAVE</button>
        </div>

        <div class="list" id="list"></div>

      <script>
        const vscode = acquireVsCodeApi();
        let editingId = null;

        function goBack() {
          vscode.postMessage({ type: "back" });
        }

        vscode.postMessage({ type: "load" });

        function save() {
          const data = {
            id: editingId || Date.now().toString(),
            label: label.value,
            path: path.value
          };

          vscode.postMessage({
            type: editingId ? "update" : "add",
            data
          });

          editingId = null;
          label.value = "";
          path.value = "";
        }

        function edit(folder) {
          editingId = folder.id;
          label.value = folder.label;
          path.value = folder.path;
        }

        function remove(id) {
          vscode.postMessage({ type: "delete", id });
        }

        window.addEventListener("message", e => {
          if (e.data.type === "folders") {
            list.innerHTML = "";

            e.data.data.forEach(f => {
              const div = document.createElement("div");
              div.className = "folder-row";

              div.innerHTML = \`
                <div class="folder-label">📁 \${f.label}</div>
                <div class="folder-path">\${f.path}</div>
                <div class="actions">
                  <button class="secondary" onclick='edit(\${JSON.stringify(f)})'>EDIT</button>
                  <button class="danger" onclick="remove('\${f.id}')">DELETE</button>
                </div>
              \`;

              list.appendChild(div);
            });
          }
        });
      </script>
      </body>

      </html>`;
  }
}


/* ================= GIT HELPERS ================= */
function getGitConfig(workspace) {
  try {
    const userName = execSync("git config --global user.name", { cwd: workspace }).toString().trim();
    const userEmail = execSync("git config --global user.email", { cwd: workspace }).toString().trim();
    let localConfig = "";
    let globalConfig = "";
    try { localConfig = execSync("git config --local --list", { cwd: workspace }).toString(); } catch {}
    try { globalConfig = execSync("git config --global --list", { cwd: workspace }).toString(); } catch {}
    return {
      user_name: userName || "",
      user_email: userEmail || "",
      local_config_content: localConfig,
      global_config_content: globalConfig
    };
  } catch { return null; }
}

/* ================= HELPERS ================= */
function detectSeverity(text) {
  if (/error/i.test(text)) return "ERROR";
  if (/warn/i.test(text)) return "WARN";
  if (/debug/i.test(text)) return "DEBUG";
  return "INFO";
}

async function sendBatch(payload, provider) {
  if (!AGENT_BASE) return;
  try {
    const res = await axios.post(`${AGENT_BASE}/stream/ingest`, payload, { headers: { "Content-Type": "application/json" } });
    
    // LOG TRAFFIC FOR TESTING
    provider.postApiTraffic("/stream/ingest", payload, res.data);

    if (res.data?.bundle) {
      provider.storeBundle(res.data.bundle);
    }
    const seq = res.data?.bundle?.Sequence || res.data?.bundle?.sequence || [];
    seq.forEach(item => {
      const d = item.Data || item.data;
      if (!d) return;
      provider.postParsedLog({
        severity: d.level || "INFO",
        message: d.message || "",
        service: d.service || "unknown",
        timestamp: d.timestamp || new Date().toISOString()
      });
    });
  } catch (e) {
    provider.postApiTraffic("/stream/ingest (ERROR)", payload, e.message);
  }
}

function startLiveStream(provider) {
  if (fixEventSource) {
    fixEventSource.close();
    fixEventSource = null;
  }

  fixEventSource = new EventSource(`${AGENT_BASE}/stream/live`);

  fixEventSource.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data);

      const seq = data?.Sequence || [];
      seq.forEach(item => {
        if (item?.Data) {
          provider.postCapturedLog({
            severity: item.Data.level,
            message: item.Data.message,
            service: item.Data.service,
            timestamp: item.Data.timestamp
          });
        }
      });
    } catch {}
  };

  fixEventSource.onerror = () => {
    provider.postFixStatus("❌ Live stream disconnected.");
  };
}

function startFixStream(provider){
  if (fixEventSource) {
    fixEventSource.close();
    fixEventSource = null;
  }
  fixEventSource = new EventSource(`${AGENT_BASE}/fix/stream`);
  fixEventSource.onmessage = (e) => {
    const msg = e.data;
    if(msg === "__CLOSE__"){
      provider.postFixStatus("✅ Fix workflow finished.");
      fixEventSource.close();
      fixEventSource = null;
      return;
    }
    provider.postFixStatus(msg);
  };
  fixEventSource.onerror = () => {
    provider.postFixStatus("❌ Fix stream disconnected.");
    fixEventSource?.close();
    fixEventSource = null;
  };
}

async function applyFixFromAI(provider) {
  if (!AGENT_BASE) {
    vscode.window.showErrorMessage("Go agent not connected yet.");
    return;
  }
  if (!provider.lastAnalyzeResponse) {
    vscode.window.showWarningMessage("No AI response available.");
    return;
  }
  const workspace = vscode.workspace.rootPath;
  if (!workspace) {
    vscode.window.showErrorMessage("No workspace folder open.");
    return;
  }
  
  const fixRequestBody = {
    ai_response: provider.lastAnalyzeResponse,
    workspace: workspace
  };

  try {
    const res = await axios.post(`${AGENT_BASE}/fix/apply`, fixRequestBody, {
      headers: { "Content-Type": "application/json" }
    });
    
    // LOG TRAFFIC FOR TESTING
    provider.postApiTraffic("/fix/apply", fixRequestBody, res.data);
    
    vscode.window.showInformationMessage("OPSCURE: Fix execution started.");
    startFixStream(provider);
  } catch (err) {
    provider.postApiTraffic("/fix/apply (ERROR)", fixRequestBody, err.response?.data || err.message);
    vscode.window.showErrorMessage("Failed to apply fix: " + (err.response?.data || err.message));
  }
}

module.exports = { activate, deactivate };