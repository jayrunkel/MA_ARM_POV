// ================================================================
//
// Query: Agg 1 - Reporting Volume Aggregation
//
// Description: 
// Count of status 
// Group by submissionAccountId, executingEntityIdCodeLei, assetClass, 
//          payloadTs, nationalCompetentAuthority

//
// Run this query against TXN_Latest if you only want the latest version
//
// ================================================================

// ================================================================
// Using preaggs
//
// To be run against the "agg1-txnVersionsGroupPreAgg" collection
// ================================================================

[{$match: {
 submissionAccountId: 2,
 executingEntityIdCodeLei: {
  $in: [
   'LEY_2_1',
   'LEY_2_2',
   'LEY_3_3'
  ]
 }
}}, {$unwind: {
 path: '$statusCounts'
}}, {$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  payloadTs: '$payloadTs',
  assetClass: '$assetClass',
  status: '$statusCounts.status'
 },
 count: {
  $sum: '$statusCounts.count'
 }
}}, {$group: {
 _id: {
  submissionAccountId: '$_id.submissionAccountId',
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
  payloadTs: '$_id.payloadTs',
  assetClass: '$_id.assetClass'
 },
 counts: {
  $push: {
   status: '$_id.status',
   count: '$count'
  }
 },
 totalCount: {
  $sum: '$count'
 }
}}, {$replaceRoot: {
 newRoot: {
  $mergeObjects: [
   '$_id',
   {
    counts: '$counts',
    totalCount: '$totalCount'
   }
  ]
 }
}}]



// ================================================================
// Without preaggs
// ================================================================

[{$match: {
 submissionAccountId: 1,
 executingEntityIdCodeLei: {
  $in: [
		'LEY_1_1',
		'LEY_1_2',
		'LEY_1_3'
  ]
 },
 payloadTs: {
  $gt: ISODate('2022-03-01T00:00:00.000Z'),
  $lt: ISODate('2022-03-02T00:00:00.000Z')
 }
}}, {$group: {
 _id: {
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  status: '$status',
  nationalCompetentAuthority: '$nationalCompetentAuthority'
 },
 count: {
  $count: {}
 }
}}, {$group: {
 _id: {
  assetClass: '$_id.assetClass',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority'
 },
 executingEntityIdCodeLei: {
  $addToSet: '$_id.executingEntityIdCodeLei'
 },
 statusCounts: {
  $push: {
   status: '$_id.status',
   count: '$count'
  }
 }
}}, {$project: {
 _id: 0,
 executingEntityIdCodeLei: 1,
 assetClass: '$_id.assetClass',
 nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
 statusCounts: 1
}}]


// First two stages only
[{$match: {
 submissionAccountId: 1,
 executingEntityIdCodeLei: {
  $in: [
		'LEY_1_1',
		'LEY_1_2',
		'LEY_1_3'
  ]
 },
 payloadTs: {
  $gt: ISODate('2022-03-01T00:00:00.000Z'),
  $lt: ISODate('2022-03-10T00:00:00.000Z')
 }
}}, {$group: {
 _id: {
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  status: '$status',
  nationalCompetentAuthority: '$nationalCompetentAuthority'
 }
}}]



