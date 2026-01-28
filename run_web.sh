#!/bin/bash
set -e

echo "📦 Installing dependencies..."
npm install

echo "🚀 Starting Web Development Server..."
# Runs 'vite'
npm run dev