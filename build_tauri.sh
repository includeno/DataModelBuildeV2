#!/bin/bash
set -e

echo "📦 Installing dependencies..."
npm install

# Basic check for Rust environment
if ! command -v cargo &> /dev/null; then
    echo "❌ Error: Rust (Cargo) is not installed."
    echo "   Please install Rust via https://rustup.rs/ to build the desktop app."
    exit 1
fi

echo "🦀 Building Tauri Desktop Application..."
# Runs 'tauri build' which builds the frontend first, then the Rust binary
npm run build:tauri

echo "✅ Tauri build successful!"
echo "   Check 'src-tauri/target/release/bundle' for your installers/executables."