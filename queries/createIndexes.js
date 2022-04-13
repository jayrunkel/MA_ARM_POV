// Run like:
//mongosh "mongodb+srv://arm-poc-m80.vb0mj.mongodb.net/marketaxess" --username user --password pass --file createIndexes.js

const dbName = "marketaxess";
const colName = "txnVersions";

const col = db.getSiblingDB(dbName).getCollection(colName);


// ================================================================
// Indexes for Aggregation Queries
// ================================================================

// Index required to sort document in descending order to find the most recent transaction document
col.createIndex({payloadTs: -1, payloadPosition: -1});


// Index for first aggregation $match stage on all the queries
col.createIndex({submissionAccountId: 1, executingEntityIdCodeLei : 1, payloadTs: 1, status: 1});



// ================================================================
// Indexes for "Search" Queries
// ================================================================


// Required for searches by transactionReferenceNumber
col.createIndex({transactionReferenceNumber : 1});


// Required for searches by tradePrice
col.createIndex({tradePrice : 1});

