const fs = require('fs');
const path = require('path');
const csv = require('./csv/csv');
const mongodb = require('./mongodb/db');

const dryRun = true; // Set to `false` to write to DB, `true` for dry-run (write to file)

async function writeToFile(data, filePath) {
    return new Promise((resolve, reject) => {
        fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8', (err) => {
            if (err) {
                console.error('Error writing to file:', err);
                return reject(err);
            }
            console.log(`Data successfully written to file: ${filePath}`);
            resolve();
        });
    });
}

async function main() {
    const desktopPath = path.join(require('os').homedir(), 'Desktop');
    const inputFilePath = path.join(desktopPath, '');

    const result = await csv.processCSV(inputFilePath);
    const emailArr = result.emails;
    const intercomIdEmailArr = result.intercomIdMap;
    const totalEmails = emailArr.length;
    console.log("No. of emails are", totalEmails);

    const batchSize = 5;
    let allFoundRecords = [];
    let allNotFoundRecords = [];
    let allErrorRecords = [];
    let allInternalIdDocuments = [];

    for (let startRow = 0; startRow < 10; startRow += batchSize) {
        const endRow = startRow + batchSize - 1;
        console.log(`Processing batch from row ${startRow} to ${endRow}`);

        const emailBatch = emailArr.slice(startRow, endRow + 1);
        const result = await mongodb.fetchUserDocs(emailBatch);

        allFoundRecords = allFoundRecords.concat(result.foundRecords);
        allNotFoundRecords = allNotFoundRecords.concat(result.notFoundRecords);
        allErrorRecords = allErrorRecords.concat(result.errorRecords);

        console.log(`Starting document creation from row ${startRow} to ${endRow}`);
        const internalIdDocument = await mongodb.docCreation(intercomIdEmailArr, allFoundRecords);

        allInternalIdDocuments = allInternalIdDocuments.concat(internalIdDocument);

        if (!dryRun) {
            console.log(`Upserting external reference docs from row ${startRow} to ${endRow}`);
            for (let doc of internalIdDocument) {
                // await mongodb.upsertExternalReference(doc);
            }
        }
        console.log(`Batch from row ${startRow} to ${endRow} processed.`);
    }

    if (dryRun) {
        const outputFilePath = path.join(desktopPath, ``);
        await writeToFile(allInternalIdDocuments, outputFilePath);
    }

    await mongodb.closeConnection();
}
main().catch(error => console.error("Error in processing:", error));
