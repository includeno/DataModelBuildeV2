const fs = require('fs');
const { PNG } = require('pngjs');

const width = 512;
const height = 512;

const png = new PNG({
    width: width,
    height: height,
    filterType: -1
});

for (let y = 0; y < png.height; y++) {
    for (let x = 0; x < png.width; x++) {
        const idx = (png.width * y + x) << 2;

        // Blue square
        png.data[idx] = 0; // R
        png.data[idx + 1] = 0; // G
        png.data[idx + 2] = 255; // B
        png.data[idx + 3] = 255; // A
    }
}

png.pack().pipe(fs.createWriteStream('app-icon.png'));
console.log('Created app-icon.png');
