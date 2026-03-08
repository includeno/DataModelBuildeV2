# DataFlow Engine

A comprehensive, frontend-first platform for hierarchical data filtering, joining, transformation, and analysis. This application bridges the gap between visual no-code ETL tools and raw SQL execution environments, powered by a high-performance Python backend.

## 🌟 Project Functionality Overview

### 🎨 Frontend (Client-Side)

Built with **React 18**, **TypeScript**, and **Tailwind CSS**, the frontend provides a modern, responsive interface for building complex data pipelines without writing code.

**1. Interactive Operation Tree (Visual Workflow)**
*   **Hierarchical Structure**: Operations are organized in a parent-child tree structure, allowing for logical branching and granular control over data flow.
*   **State Management**: Toggle nodes on/off to debug specific branches of logic without deleting configuration.
*   **Visual Cues**: Color-coded icons for different operation types (Source, Filter, Join, Transform, etc.) and visual indicators for node status.
*   **Logic Path Visualization**: A specialized modal to visualize the synthesized logic path from the root to any selected node.

**2. Advanced Command Editor**
*   **No-Code Logic Builder**:
    *   **Recursive Filters**: Build complex `AND`/`OR` logic groups with deeply nested conditions.
    *   **Joins**: Visual configuration for `LEFT`, `RIGHT`, `INNER`, and `FULL` joins against other datasets or operation results.
    *   **Aggregations**: `GROUP BY` functionality with support for multiple metrics (`SUM`, `MEAN`, `COUNT`, etc.) and `HAVING` clauses.
    *   **Transformations**: Field mapping engine supporting both simple expressions and custom Python snippets.
    *   **Variable System**: Extract values from data (e.g., "Top 10 Customers") and reuse them dynamically in downstream filters.

**3. SQL Studio**
*   **Direct Querying**: A dedicated IDE-like environment to run raw SQL queries directly against the DuckDB backend.
*   **Multi-Tab Support**: Work on multiple queries simultaneously with session persistence.
*   **Execution History**: Tracks run time, row counts, and status (Success/Error) for previous queries.
*   **Performance Metrics**: Real-time display of query execution duration.

**4. Data Visualization & Management**
*   **Smart Preview**:
    *   **Standard Grid**: Paginated view of results with automatic type formatting.
    *   **Complex View**: Master-detail visualization for inspecting 1:N relationships (e.g., viewing an Order and expanding to see all associated Line Items).
*   **Data Import**: Drag-and-drop support for `.csv` and `.xlsx` files with automatic schema detection.
*   **Schema Editor**: View and modify field data types and date formats to ensure data integrity.

**5. Session & Configuration**
*   **Workspace Management**: Create, save, and switch between isolated analysis sessions.
*   **Customization**: Extensive appearance settings (text sizes, colors, guide lines) and layout configuration (panel positioning).
*   **Dual Mode Connectivity**: Switch seamlessly between a local Python backend and an in-browser "Mock Server" for demonstration purposes.

---

### ⚙️ Backend (Server-Side)

Built with **Python**, **FastAPI**, and **DuckDB**, the backend ensures high-performance data processing and persistence.

**1. Hybrid Execution Engine**
*   **DuckDB Integration**: Utilizes DuckDB for blazing-fast SQL execution, initial data ingestion, and heavy join operations.
*   **Pandas Processing**: Leverages Pandas for complex row-wise transformations, custom Python logic execution, and variable extraction.
*   **Stateful Execution**: Capable of executing partial branches of the operation tree, allowing for rapid iteration and debugging.

**2. Data Storage & Persistence**
*   **Session Isolation**: Each user session has its own directory, SQLite/DuckDB database file, and metadata JSON, ensuring complete data isolation.
*   **File Handling**: Robust parsing of CSV and Excel files with automatic column sanitization.
*   **State Recovery**: Persists the full operation tree and dataset metadata, allowing users to pick up exactly where they left off.

**3. API Architecture**
*   **RESTful Endpoints**: Clean, typed endpoints for `/upload`, `/execute`, `/query`, and `/sessions`.
*   **Streaming Responses**: Supports large data exports via streaming CSV responses.
*   **Error Handling**: Granular error reporting propagated to the frontend for UI feedback.

**4. Advanced Analysis**
*   **Overlap Detection**: Algorithms to compare data across different branches of the tree to identify record overlaps or exclusions.
*   **Mock Capability**: Includes a logic layer that mimics backend behavior for frontend-only testing and development without a server.

## 🚀 Quick Start

### Prerequisites
- **Node.js** (v16+)
- **Python 3.9+** (For backend)

### 🌐 Web Application

Run the React frontend (defaults to Mock Mode):

```bash
npm install
npm run dev
```

### 🐍 Backend Server

To use the real Python backend engine:

1. Install Python dependencies:
   ```bash
   pip install -r backend/requirements.txt
   ```
2. Start the server:
   ```bash
   npm run backend
   ```
3. In the Web App, open **Settings**, enter `http://localhost:8000`, and add the server.

### 🐳 Docker / Compose

Build backend image:

```bash
npm run docker:build
```

Run backend container directly:

```bash
npm run docker:run
```

Use Docker Compose:

```bash
npm run docker:compose:up
npm run docker:compose:logs
npm run docker:compose:down
```

Package backend Docker image as a tar.gz artifact:

```bash
npm run docker:package
```

Build release artifacts (web bundle + optional Docker package):

```bash
npm run package:release
```

### ✅ CI (main branch)

GitHub Actions workflow is provided at `.github/workflows/main-tests.yml` and will run frontend and backend tests on:
- push to `main`
- pull requests targeting `main`
