const fs = require('fs');
const csv = require('csv-parse');

async function processCSV(filePath) {
    try {
        const fileContent = fs.readFileSync(filePath, 'utf8');

        return new Promise((resolve, reject) => {
            csv.parse(fileContent, {
                columns: true,
                skip_empty_lines: true
            }, (err, records) => {
                if (err) {
                    console.error('Error parsing CSV:', err);
                    return reject(err);
                }

                const emails = [];
                const intercomIdMap = {};

                records.forEach((row) => {
                    if (row['Email']) {
                        emails.push(row['Email']);
                    }
                    if (row['Id'] && row['Email']) {
                        intercomIdMap[row['Id']] = row['Email'];
                    }
                });

                resolve({ emails, intercomIdMap });
            });
        });

    } catch (error) {
        console.error('Error processing CSV file:', error);
        throw error;
    }
}

exports.processCSV=processCSV;
