const path = require('path');
const csv = require('./csv/csv');
const mongodb = require('./mongodb/db');

async function main() {
    const desktopPath = path.join(require('os').homedir(), 'Desktop');
    const inputFilePath = path.join(desktopPath, '');

    const result = await csv.processCSV(inputFilePath);
    // console.log("result",result);
    const emailArr = result.emails;
    // console.log("Email fetched from CSV:", emailArr);
    const totalEmails = emailArr.length;
    console.log("No. of emails are", totalEmails);
    const intercomIdEmailArr = result.intercomIdMap;
    // console.log("intercomIds", intercomIdEmailArr);

    const batchSize = 5;
    let allFoundRecords = [];
    let allNotFoundRecords = [];
    let allErrorRecords = [];

    for (let startRow = 0; startRow < 10; startRow += batchSize) {
        const endRow = startRow + batchSize - 1;

        console.log(`Processing batch from row ${startRow} to ${endRow}`);
        const emailBatch = emailArr.slice(startRow, endRow + 1);
        const result = await mongodb.fetchUserDocs(emailBatch);

        allFoundRecords = allFoundRecords.concat(result.foundRecords);
        allNotFoundRecords = allNotFoundRecords.concat(result.notFoundRecords);
        allErrorRecords = allErrorRecords.concat(result.errorRecords);

        console.log(`Batch from row ${startRow} to ${endRow} processed.`);
        console.log(`Starting the document creation from row ${startRow} to ${endRow}`);
        const internalIdDocument = await mongodb.docCreation(intercomIdEmailArr, allFoundRecords);
        console.log(`Upserting the external reference docs from row ${startRow} to ${endRow}`);
        for(var doc in internalIdDocument)  await mongodb.upsertExternalReference(doc);
    }
    await mongodb.closeConnection();
}
main();