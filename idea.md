
# System Feature Brainstorming

## 2026-02-02

### 🧠 AI & Intelligence
*   **NLP to Pipeline (Text-to-Graph)**:
    *   *Concept*: Users type "Find employees in Engineering earning > 100k and join with their sales records."
    *   *Action*: The system automatically generates the Operation Tree (Source -> Filter -> Join) using an LLM.
*   **Smart Data Cleaning**:
    *   *Concept*: A "Magic Transform" node that detects inconsistencies (e.g., "NY" vs "New York", date format mismatches) and auto-suggests Python transformation code to standardize them.
*   **Formula Assistant**:
    *   *Concept*: Inside the `Transform` node (Python mode), provide an AI autocomplete that knows the dataset schema. User types `# calculate bmi` and AI inserts `return row['weight'] / (row['height']**2)`.

### 📊 Visualization & Reporting
*   **Embedded Charting Nodes**:
    *   *Concept*: Add a `chart` command type. Instead of outputting a table, this node outputs a JSON config for Recharts/ECharts (Bar, Line, Scatter).
    *   *UI*: The "Preview Panel" renders the interactive chart instead of the data grid.
*   **Data Profile "Sparklines"**:
    *   *Concept*: Show small histograms or null-value heatmaps directly on the Tree Node cards to visualize data quality and distribution changes at every step without opening the preview.
*   **Canvas Mode (Mind Map View)**:
    *   *Concept*: An alternative to the "Tree View" (sidebar). A generic infinite canvas (like React Flow) where nodes can be dragged freely, allowing for DAGs (Directed Acyclic Graphs) instead of just strict parent-child trees (merging multiple branches back together).

### ⚙️ Advanced Logic & Processing
*   **Fuzzy Join (Smart Merge)**:
    *   *Concept*: A Join type that doesn't require exact matches. Uses Levenshtein distance or DuckDB's Jaccard similarity to join "Apple Inc" with "Apple Incorporated".
*   **Reusable "Macro" Nodes**:
    *   *Concept*: Allow users to select a branch of 5 operations, right-click, and "Save as Macro". This creates a custom node type that can be reused in other sessions (e.g., a standard "Clean Financial Data" macro).
*   **Time-Travel Debugging**:
    *   *Concept*: Since the system is stateless/functional, add a slider at the bottom of the Workspace. Dragging the slider replays the data transformation step-by-step, highlighting which rows were dropped at which filter.

### 🔌 Integration & Connectivity
*   **Live Data Streams (Webhooks)**:
    *   *Concept*: A Source type that listens to a Webhook URL. When JSON is posted to that URL, the pipeline runs automatically and pushes the result to a destination.
*   **Google Sheets / Excel 365 Sync**:
    *   *Concept*: Instead of file upload, authorize with OAuth. The system reads directly from a Sheet. If the Sheet updates, the "Run" button fetches the latest data.
*   **WASM Client-Side Execution**:
    *   *Concept*: Port the backend logic to **DuckDB-WASM** and **Pyodide**.
    *   *Benefit*: Run the entire app 100% in the browser (Static Web App) without needing the Python backend server, improving security and deployment costs.

### 🛡️ Enterprise & Governance
*   **Pipeline Versioning (Git-lite)**:
    *   *Concept*: A "History" tab for the Session. Every time the user clicks "Run", save a snapshot of the Tree JSON. Allow reverting to previous logic states.
*   **Data Lineage Graph**:
    *   *Concept*: Visualizing which original CSV column ended up in the final report after 10 joins and 5 renames.

## 2026-02-03

### 🤝 Collaboration & Workflow
*   **Real-Time Multiplayer**:
    *   *Concept*: WebSocket integration to see teammates' cursors and selection in the operation tree (similar to Figma/Miro).
    *   *Action*: Allow locking specific branches so only one user can edit a complex filter group at a time.
*   **Node Annotations (Sticky Notes)**:
    *   *Concept*: Add a "Comment" layer to the tree. Users can attach markdown notes to complex Operation Nodes explaining *why* a specific regex or filter logic was used.

### 🛠️ DevTools & Extensibility
*   **"Eject" to Code**:
    *   *Concept*: A button that compiles the current visual tree into a standalone, production-ready Python script (using Pandas or Polars) or a raw SQL query (CTE chain).
    *   *Benefit*: Bridging the gap between No-Code exploration and engineering deployment.
*   **Instant API Generation**:
    *   *Concept*: Turn any "Result Node" into a REST endpoint.
    *   *Action*: The system auto-generates a route like `/api/v1/endpoints/{nodeId}`. Defined `Variables` in the Setup phase become query parameters automatically.

### 🛡️ Data Quality (DataOps)
*   **Validation Nodes (Expectations)**:
    *   *Concept*: A dedicated node type for asserting data quality.
    *   *Logic*: `expect(column).to_be_unique()`, `expect(age).to_be_between(0, 120)`. If the condition fails for >N% of rows, the pipeline halts with a visual error report.
*   **PII Masking Toggle**:
    *   *Concept*: Auto-detection of sensitive columns (Email, SSN, Credit Card). A global toggle "View: Safe Mode" masks these columns in the UI preview (`j***@gmail.com`) while maintaining the raw data in the backend for joins.

### 🔀 Advanced Flow Control
*   **Router / Splitter Node**:
    *   *Concept*: A node that splits a single data stream into multiple exclusive branches based on logic (e.g., `Region='US'` goes to Branch A, `Region='EU'` goes to Branch B).
    *   *UI*: Visual indication of data splitting into parallel tracks.
*   **Loop / Iterator Node**:
    *   *Concept*: Iterate over a "List Variable" (e.g., list of Department IDs) and run the child branch for each item, unioning the results at the end.

### 🎨 UX & Polish
*   **Command Palette (`Ctrl+K`)**:
    *   *Concept*: Quick keyboard navigation to jump to a node, add a command, search for a dataset, or toggle settings without using the mouse.
*   **Minimap Navigator**:
    *   *Concept*: For very large operation trees (50+ nodes), a small overlay map in the corner to visualize the overall structure and current viewport location.
