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
 count: {
  $sum: '$count'
 }
}}, {$replaceRoot: {
 newRoot: {
  $mergeObjects: [
   '$_id',
   {
    count: '$count'
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
 },
 status: 'AREJ'
}}, {$match: {
 nonMiFidFlag: false,
 status: 'AREJ'
}}, {$group: {
 _id: {
  assetClass: '$assetClass',
  nationalCompetentAuthority: '$nationalCompetentAuthority'
 },
 executingEntityIdCodeLei: {
  $addToSet: '$executingEntityIdCodeLei'
 },
 count: {
  $count: {}
 }
}}, {$project: {
 _id: 0,
 executingEntityIdCodeLei: 1,
 assetClass: '$_id.assetClass',
 nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
 count: 1
}}]



