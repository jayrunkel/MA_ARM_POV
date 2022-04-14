// ================================================================
//
// Query: Agg 4 - Trax Rejections Aggregations
//
// Description:
// count of errors.errorCode
// Group by submissionAccountId, executingEntityIdCodeLei, assetClass, 
//          payloadTs, nationalCompetentAuthority
//
// Run this query against TXN_Latest if you only want the latest version
//
// ================================================================

// ================================================================
// using preaggs
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
 path: '$errorCodes'
}}, {$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  assetClass: '$assetClass',
  payloadTs: '$payloadTs',
  errorCode: '$errorCodes.errorCode'
 },
 count: {
  $sum: '$errorCodes.count'
 }
}}, {$group: {
 _id: {
  submissionAccountId: '$_id.submissionAccountId',
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
  assetClass: '$_id.assetClass',
  payloadTs: '$_id.payloadTs'
 },
 errorCodeCounts: {
  $push: {
   errorCode: '$_id.errorCode',
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
    errorCodeCounts: '$errorCodeCounts',
    totalCount: '$totalCount'
   }
  ]
 }
}}]

// ================================================================
// Without preaggs
// ================================================================

[{$match: {
 submissionAccountId: 2,
 executingEntityIdCodeLei: {
  $in: [
   'LeiCode6',
   'LeiCode7',
   'LeiCode8'
  ]
 },
 payloadTs: {
  $gt: ISODate('2022-03-01T00:00:00.000Z'),
  $lt: ISODate('2022-03-31T00:00:00.000Z')
 }
}}, {$group: {
 _id: {
  assetClass: '$assetClass',
  errorCodes: '$errorCodes',
  nationalCompetentAuthority: '$nationalCompetentAuthority'
 },
 executingEntityIdCodeLei: {
  $addToSet: '$executingEntityIdCodeLei'
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
  $addToSet: '$executingEntityIdCodeLei'
 },
 errorCodesCounts: {
  $push: {
   errorCodes: '$_id.errorCodes',
   count: '$count'
  }
 }
}}, {$project: {
 _id: 0,
 executingEntityIdCodeLei: {
  $reduce: {
   input: '$executingEntityIdCodeLei',
   initialValue: [],
   'in': {
    $setUnion: [
     '$$value',
     '$$this'
    ]
   }
  }
 },
 assetClass: '$_id.assetClass',
 nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
 errorCodesCounts: 1
}}]

