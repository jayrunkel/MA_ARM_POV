// ================================================================
//
// Query: Agg 5 - Regulator Rejection Aggregation
//
// Description:
// Count of regResp.ruleId
// Group by submissionAccountId, executingEntityIdCodeLei, assetClass, 
//          payloadTs, nationalCompetentAuthority
// 
//
// Run this query against TXN_Latest if you only want the latest version
//
// TODO:
//  - NOT TESTED. Needs to be run against test data
// 
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
}}, {$unwind: {
 path: '$regResp'
}}, {$group: {
 _id: {
  assetClass: '$assetClass',
  regRespRuleId: '$regResp.ruleId',
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
 regRespRuleIdCounts: {
  $push: {
   ruleId: '$_id.regRespRuleId',
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
 regRespRuleIdCounts: 1
}}]



