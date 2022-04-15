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
 $expr: {
  $and: [
   {
    $eq: [
     '$submissionAccountId',
     100
    ]
   },
   {
    $in: [
     '$executingEntityIdCodeLei',
     [
      'LEY_100_1',
      'LEY_100_2',
      'LEY_100_3'
     ]
    ]
   },
   {
    $gte: [
     '$payloadTs',
     ISODate('2022-03-01T00:00:00.000Z')
    ]
   },
   {
    $lt: [
     '$payloadTs',
     ISODate('2022-03-10T00:00:00.000Z')
    ]
   },
   {
    $gt: [
     {
      $size: '$errors'
     },
     0
    ]
   }
  ]
 }
}}, {$addFields: {
 payloadTs: {
  $dateTrunc: {
   date: '$payloadTs',
   unit: 'day'
  }
 }
}}, {$unwind: {
 path: '$errors'
}}, {$group: {
 _id: {
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  payloadTs: '$payloadTs',
  errorCode: '$errors.errorCode'
 },
 submissionAccountId: {
  $first: '$submissionAccountId'
 },
 count: {
  $count: {}
 }
}}, {$group: {
 _id: {
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
  assetClass: '$_id.assetClass',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
  payloadTs: '$_id.payloadTs'
 },
 submissionAccountId: {
  $first: '$submissionAccountId'
 },
 errorCodeCounts: {
  $push: {
   errorCodes: '$_id.errorCode',
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
    submissionAccountId: '$submissionAccountId',
    errorCodeCounts: '$errorCodesCounts',
    totalCount: '$totalCount'
   }
  ]
 }
}}]



