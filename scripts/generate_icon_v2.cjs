const fs = require('fs');
const { PNG } = require('pngjs');

const width = 512;
const height = 512;

const png = new PNG({
    width: width,
    height: height,
    filterType: -1
});

const cx = width / 2;
const cy = height / 2;
const r = 200;
const r2 = r * r;

for (let y = 0; y < png.height; y++) {
    for (let x = 0; x < png.width; x++) {
        const idx = (png.width * y + x) << 2;

        const dx = x - cx;
        const dy = y - cy;
        
        if (dx*dx + dy*dy <= r2) {
            // Red circle
            png.data[idx] = 255; // R
            png.data[idx + 1] = 0; // G
            png.data[idx + 2] = 0; // B
            png.data[idx + 3] = 255; // A
        } else {
            // White background
            png.data[idx] = 255; // R
            png.data[idx + 1] = 255; // G
            png.data[idx + 2] = 255; // B
            png.data[idx + 3] = 255; // A
        }
    }
}

png.pack().pipe(fs.createWriteStream('app-icon.png'));
console.log('Created app-icon.png with red circle');
