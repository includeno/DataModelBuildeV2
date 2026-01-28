# DataFlow Engine

A frontend-first platform for hierarchical data filtering, joining, and transformation using a tree-based operation structure. This application allows users to build complex data logic visually or execute direct SQL queries.

## ✨ Features

- **Visual Operation Tree**: Build data pipelines with parent/child relationships.
- **Command Editor**: visual interface for Filters, Joins, Sorts, Aggregations, and Transforms.
- **SQL Studio**: Full SQL interface with history log and multi-tab support.
- **Dual Mode**:
  - **Mock Server**: Zero-setup mode running entirely in the browser for demos.
  - **Real Backend**: Python + DuckDB engine for high-performance CSV processing.
- **Cross-Platform**: Deployable as a Web App or Native Desktop App (Tauri).

## 🚀 Quick Start

### Prerequisites
- **Node.js** (v16+)
- **Rust** (Only required for building the Desktop App)
- **Python 3.9+** (Only required for running the local Backend)

### 🌐 Web Application

Run the React frontend in development mode. By default, it uses the **Mock Server**, so no backend is required.

```bash
# Run via helper script
./run_web.sh

# OR via npm
npm install
npm run dev
```

Build for production:
```bash
./build_web.sh
```

### 🖥️ Desktop Application (Tauri)

Build the native application for your OS (Windows, macOS, or Linux). Ensure Rust is installed via [rustup.rs](https://rustup.rs).

```bash
# Run via helper script
./build_tauri.sh

# OR via npm
npm install
npm run build:tauri
```
*Artifacts will be generated in `src-tauri/target/release/bundle`.*

### 🐍 Backend Server (Optional)

To process real CSV files and use the actual DuckDB engine:

1. Install Python dependencies:
   ```bash
   pip install -r backend/requirements.txt
   ```
2. Start the server:
   ```bash
   npm run backend
   # Runs on http://localhost:8000
   ```
3. In the Web App, click the **"Mock Server"** button in the top right and add/select `http://localhost:8000`.

## 📂 Project Structure

- **src/**: React frontend source code.
  - **components/**: UI components (CommandEditor, SqlEditor, etc.).
  - **utils/api.ts**: API abstraction layer handling both HTTP requests and Mock logic.
- **backend/**: Python FastAPI server with DuckDB integration.
- **src-tauri/**: Rust configuration for the desktop wrapper.
- **scripts**: `.sh` helper files for building and running.
