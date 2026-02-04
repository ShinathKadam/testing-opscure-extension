# OPSCURE — Intelligent Log Streaming & Analysis for VS Code

OPSCURE is a Visual Studio Code extension that captures live runtime logs directly from the VS Code terminal, cleans and correlates them using a local Go sidecar agent, and performs AI-driven analysis to generate actionable recommendations.

It enables a continuous observability pipeline inside the editor:

capture → stream → clean → correlate → analyze → recommend → (upcoming) fix

---

# 📌 Table of Contents

1. Overview  
2. Key Features  
3. System Architecture  
4. Prerequisites  
5. Installation  
6. How OPSCURE Works (Actual Flow)  
7. Usage Guide  
8. Security & Privacy  
9. License & Legal  
10. Troubleshooting  
11. Roadmap  
12. Support  

---

# 🔍 1. Overview

OPSCURE is designed to help developers and platform teams understand runtime behavior of applications directly from VS Code.

It works by capturing terminal output in real time, batching logs automatically, and sending them to a local Go sidecar service for cleanup and correlation. The processed data can then be analyzed using an AI engine to generate root-cause insights and remediation recommendations.

OPSCURE runs primarily on your local system and integrates seamlessly into the VS Code interface.

---

# 🚀 2. Key Features

- Live log capture from the active VS Code terminal  
- Automatic batching (50 logs per batch)  
- Local Go sidecar for cleanup and correlation  
- Real-time parsed log visualization  
- On-demand AI analysis  
- Structured preprocess pipeline for AI systems  
- Extensible design for automated remediation (upcoming)

---

# 🧠 3. System Architecture

OPSCURE consists of two main components:

### 1. VS Code Extension (JavaScript)
- Captures terminal logs
- Manages batching and UI
- Displays parsed and analyzed data
- Sends data to the Go sidecar

### 2. Go Sidecar Agent
- Runs locally as a background process
- Cleans and normalizes logs
- Performs stream management and correlation
- Prepares AI-compatible request structures
- Acts as a bridge to the AI analysis service

The extension communicates with the sidecar over localhost using HTTP.

---

# 🧩 4. Prerequisites

Before installing OPSCURE, ensure you have the following:

### ✅ Required

| Software | Purpose | Download |
|---------|---------|----------|
| Visual Studio Code | Host editor | https://code.visualstudio.com/ |
| Node.js (v16+) | Extension runtime | https://nodejs.org/ |
| npm | Package manager (comes with Node.js) | https://nodejs.org/ |
| Go (v1.18+) | Build/run Go sidecar | https://go.dev/dl/ |

> 💡 If your extension package already includes a compiled Go binary, Go does not need to be installed separately.

---

# 📦 5. Installation

### From Visual Studio Marketplace

1. Open VS Code  
2. Go to Extensions (`Ctrl + Shift + X`)  
3. Search for **OPSCURE**  
4. Click **Install**

Or install via command line:

```bash
code --install-extension shrinatech.opscure
```

# ▶️ 6. How OPSCURE Works (Actual Flow)

OPSCURE operates as a real-time log intelligence pipeline.

### 🔹 Step 1: Runtime Log Capture

The extension listens to the active terminal using the proposed VS Code API:

```stylus
vscode.window.onDidWriteTerminalData
```

All logs written to the opened terminal are captured automatically.

Each log is normalized with:

- severity

- timestamp

- raw message

### 🔹 Step 2: Automatic Batching & Streaming

Captured logs are grouped into batches of 50 logs.

When a batch reaches 50 entries, it is automatically sent to the Go sidecar:

```bash
POST /stream/ingest
```

The extension immediately starts collecting the next batch without waiting.

### 🔹 Step 3: Cleanup, Correlation & Live Display

The Go sidecar:

- Cleans and normalizes logs

- Applies stream management logic

- Builds correlation bundles

If the response contains a non-null sequence, the extension extracts:

- level

- service

- message

- timestamp

These are displayed live in the Parsed Response Logs section.

### 🔹 Step 4: AI-Driven Analysis

1. When the user clicks the Analyze button:

2. The extension gathers all non-null sequences collected so far.

Sends them to the Go agent:

```bash
POST /logs/preprocess
```

3. The Go agent builds the exact structure required by the AI system.

4. The same structured payload is immediately forwarded to:

```bash
POST /ai/analyze
```

5. The AI system returns recommendations and analysis results.

6. Both preprocess and AI responses are displayed in the UI.

### 🔹 Step 5: Automated Remediation (Upcoming)

The next phase of OPSCURE will introduce controlled remediation:

- Applying AI recommendations to local files

- Applying configuration fixes

- Triggering safe remote actions

This will complete the closed-loop system:

observe → analyze → recommend → fix → verify

---

# 🧪 7. Usage Guide
### ▶ Open OPSCURE

- Click the OPSCURE icon in the Activity Bar

- The sidecar agent starts automatically

- The OPSCURE panel opens

### ▶ Start capturing logs

- Run your application in the VS Code terminal

- Logs are captured automatically

- Every 50 logs are streamed to the Go agent

### ▶ View parsed logs

- Cleaned logs appear in Parsed Response Logs

- Severity, service, message, and timestamp are displayed

### ▶ Analyze

- Click the Analyze button

- Preprocess + AI analysis is executed

- Structured results appear in the analysis sections

---

# 🔐 8. Security & Privacy

- OPSCURE primarily runs locally.

- Log capture occurs only from the user’s active terminal.

- The Go sidecar communicates over 127.0.0.1.

- No log data is transmitted externally unless explicitly configured for AI analysis.

OPSCURE does not sell or share user data.

---

# ⚖️ 9. License & Legal

This software is proprietary and owned by OPSCURE.

- Unauthorized modification, redistribution, or reverse engineering is prohibited.

- The software is licensed, not sold.

See:

- LICENSE

- EULA.txt

for full legal terms.

---

# ❓ 10. Troubleshooting
### Sidecar not starting

- Ensure the binary is not blocked by antivirus software

- Ensure the configured port is free

### Logs not appearing

- Confirm logs are produced in the active terminal

- Ensure capture is enabled

### Analyze button not visible

- At least one valid processed sequence must be received

### AI request failing

- Check network connectivity

- Verify AI endpoint availability

---

# 🛣 11. Roadmap

Planned enhancements include:

- AI-driven automatic fixes

- Diff preview before applying changes

- Rollback support

- Incident report export

- Remote remediation support

- Configuration profiles

---

# 🧑‍💻 12. Support

For support, licensing, or enterprise deployment inquiries, contact OPSCURE through official channels.

---

© 2026 OPSCURE. All rights reserved.

---