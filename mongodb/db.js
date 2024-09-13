const { MongoClient } = require('mongodb');

const uri ="";
const dbName = "";
const collectionName = "";
let client = null;

async function closeConnection() {
    if (client) {
        client.close();
        console.log("The mongoDB connection is closed");
        return;
    }
}

async function fetchUserDocs(emailBatch) {
    if (!client) {
        client = new MongoClient(uri);
        await client.connect();
        console.log("Connected successfully to MongoDB");
    }
    const foundRecords = [];
    const notFoundRecords = [];
    const errorRecords = [];
    const response = {
        foundRecords,
        notFoundRecords,
        errorRecords
    };
    let collection = null;
    try {
        const database = await client.db(dbName);
        collection = await database.collection(collectionName);
    } catch (error) {
        console.log("Error connecting to the collection:", error);
        return response;
    }
    if (!collection) {
        return response;
    }
    const query = { "profile.email": { $in: emailBatch } };
    try {
        const cursor = collection.find(query);
        await cursor.forEach(doc => {
            console.log("Found document:", doc);
            foundRecords.push(doc);
        });
        if (foundRecords.length === 0) {
            emailBatch.forEach(email => {
                notFoundRecords.push({ email, reason: "No record found in MongoDB" });
            });
        }
    } catch (error) {
        console.error("Error fetching records:", error);
        errorRecords.push({ emailBatch, error: error.message });
    }
    return response;
}

async function docCreation(intercomIds, allFoundRecords) {
    const resultArray = [];

    Object.entries(intercomIds).forEach(([key, value]) => {
        const user = allFoundRecords.find(userObj => userObj.profile && userObj.profile.email === value);

        if (user && user.profile && user.profile._id) {
            const newObject = {
                submgtId: `submgt-user-${user.profile._id}`,
                email: value,
                externalSystems: {
                    intercomId: key
                },
                type: 'contact'
            };
            console.log("newObject:", newObject);

            resultArray.push(newObject);
        }
    });

    return resultArray;
}

async function upsertExternalReference(internalIdDocument) {
    const client = new MongoClient("");
    try {
        await client.connect();
        const externalReferencesCollection = client.db("").collection("");
        const options = { upsert: true };

        return await externalReferencesCollection.insertOne(internalIdDocument, options);
    } finally {
        await client.close();
    }
}

exports.upsertExternalReference = upsertExternalReference;
exports.docCreation = docCreation;
exports.fetchUserDocs = fetchUserDocs;
exports.closeConnection = closeConnection;
