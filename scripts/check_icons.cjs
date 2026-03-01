const fs = require('fs');
const path = require('path');

const files = [
    'src-tauri/app-icon.png',
    'src-tauri/icons/icon.icns',
    'src-tauri/icons/icon.png',
    'src-tauri/icons/128x128.png',
    'src-tauri/icons/32x32.png'
];

files.forEach(file => {
    try {
        const stats = fs.statSync(file);
        console.log(`${file}: ${stats.size} bytes`);
    } catch (err) {
        console.error(`${file}: Not found`);
    }
});
