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
// To be run against the "agg5-txnVersionsGroupPreAgg" collection
// 
// ================================================================

// ================================================================
// with preags
//
// To be run against the "agg5-txnVersionsGroupPreAgg" collection
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
 path: '$regRespCounts'
}}, {$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  payloadTs: '$payloadTs',
  assetClass: '$assetClass',
  regRespRuleId: '$regRespCounts.regResp.ruleId'
 },
 regRespRuleDesc: {
  $first: '$regRespCounts.regResp.ruleDesc'
 },
 count: {
  $sum: '$regRespCounts.count'
 }
}}, {$group: {
 _id: {
  submissionAccountId: '$_id.submissionAccountId',
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
  payloadTs: '$_id.payloadTs',
  assetClass: '$_id.assetClass'
 },
 regRespRuleCounts: {
  $push: {
   ruleId: '$_id.regRespRuleId',
   ruleDesc: '$regRespRuleDesc',
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
    regRespRuleCounts: '$regRespRuleCounts',
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
   'LEY_2_1',
   'LEY_2_2',
   'LEY_2_3'
  ]
 },
 payloadTs: {
  $gt: ISODate('2022-03-01T00:00:00.000Z'),
  $lt: ISODate('2022-03-10T00:00:00.000Z')
 }
}}, {$addFields: {
 payloadTs: {
  $dateTrunc: {
   date: '$payloadTs',
   unit: 'day'
  }
 }
}}, {$unwind: {
 path: '$regResp'
}}, {$group: {
 _id: {
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  payloadTs: '$payloadTs',
  regRespRuleId: '$regResp.ruleId'
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
 regRespRuleIdCounts: {
  $push: {
   ruleId: '$_id.regRespRuleId',
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
    regRespRuleIdCounts: '$regRespRuleIdCounts',
    totalCount: '$totalCount'
   }
  ]
 }
}}]



