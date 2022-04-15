// ================================================================
//
// Query: Agg 3 - Mifid Ineligible Aggregation
//
// Description:
// Count of Docs for: nonMiFidFlag=”FALSE” AND
// Status != “AREJ”
// group by submissionAccountId, executingEntityIdCodeLei, assetClass, 
//          payloadTs, nationalCompetentAuthority
// 
//
// To be run against the "agg3-txnVersionsGroupPreAgg" collection
// 
// ================================================================

// ================================================================
// Using preaggs
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
}}, {$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  assetClass: '$assetClass',
  payloadTs: '$payloadTs'
 },
 status: {
  $first: '$status'
 },
 count: {
  $sum: '$count'
 }
}}, {$replaceRoot: {
 newRoot: {
  $mergeObjects: [
   '$_id',
   {
    count: '$count',
    nonMiFidFlag: false,
    status: '$status'
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
   'LEY_2_1',
   'LEY_2_2',
   'LEY_2_3'
  ]
 },
 payloadTs: {
  $gt: ISODate('2022-03-01T00:00:00.000Z'),
  $lt: ISODate('2022-03-10T00:00:00.000Z')
 },
 nonMiFidFlag: false,
 status: {
  $ne: 'AREJ'
 }
}}, {$addFields: {
 payloadTs: {
  $dateTrunc: {
   date: '$payloadTs',
   unit: 'day'
  }
 }
}}, {$group: {
 _id: {
  assetClass: '$assetClass',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  payloadTs: '$payloadTs'
 },
 submissionAccountId: {
  $first: '$submissionAccountId'
 },
 count: {
  $count: {}
 }
}}, {$replaceRoot: {
 newRoot: {
  $mergeObjects: [
   '$_id',
   {
    submissionAccountId: '$submissionAccountId',
    count: '$count'
   }
  ]
 }
}}]






