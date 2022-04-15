// ================================================================
//
// Query: Agg 2 - Trade By Status Aggregation
//
// Description:
//
// Count of status, subStatus 
// Group by submissionAccountId, executingEntityIdCodeLei, assetClass,
//          payloadTs, nationalCompetentAuthority
//
// Use collection "agg2-txnVersionsGroupPreAgg"
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
}}, {$unwind: {
 path: '$statusCounts'
}}, {$unwind: {
 path: '$statusCounts.subStatusCounts'
}}, {$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  payloadTs: '$payloadTs',
  assetClass: '$assetClass',
  status: '$statusCounts.status',
  subStatus: '$statusCounts.subStatusCounts.subStatus'
 },
 count: {
  $sum: '$statusCounts.subStatusCounts.count'
 }
}}, {$group: {
 _id: {
  submissionAccountId: '$_id.submissionAccountId',
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
  payloadTs: '$_id.payloadTs',
  assetClass: '$_id.assetClass',
  status: '$_id.status'
 },
 counts: {
  $push: {
   subStatus: '$_id.subStatus',
   count: '$count'
  }
 },
 totalCount: {
  $sum: '$count'
 }
}}, {$group: {
 _id: {
  submissionAccountId: '$_id.submissionAccountId',
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
  payloadTs: '$_id.payloadTs',
  assetClass: '$_id.assetClass'
 },
 statusCounts: {
  $push: {
   status: '$_id.status',
   subStatusCounts: '$counts',
   count: '$totalCount'
  }
 },
 totalCount: {
  $sum: '$totalCount'
 }
}}, {$replaceRoot: {
 newRoot: {
  $mergeObjects: [
   '$_id',
   {
    statusCounts: '$statusCounts',
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
   'LEY_1_1',
   'LEY_2_1',
   'LEY_3_1'
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
}}, {$group: {
 _id: {
  assetClass: '$assetClass',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  payloadTs: '$payloadTs',
  status: '$status',
  subStatus: '$subStatus'
 },
 submissionAccountId: {
  $first: '$submissionAccountId'
 },
 count: {
  $count: {}
 }
}}, {$group: {
 _id: {
  assetClass: '$_id.assetClass',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
  payloadTs: '$_id.payloadTs',
  status: '$_id.status'
 },
 submissionAccountId: {
  $first: '$submissionAccountId'
 },
 subStatusCounts: {
  $push: {
   subStatus: '$_id.subStatus',
   count: '$count'
  }
 },
 statusCount: {
  $sum: '$count'
 }
}}, {$group: {
 _id: {
  assetClass: '$_id.assetClass',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
  payloadTs: '$_id.payloadTs'
 },
 submissionAccountId: {
  $first: '$submissionAccountId'
 },
 statusCounts: {
  $push: {
   status: '$_id.status',
   statusCount: '$statusCount',
   subStatusCounts: '$subStatusCounts'
  }
 },
 totalCount: {
  $sum: '$statusCount'
 }
}}, {$replaceRoot: {
 newRoot: {
  $mergeObjects: [
   '$_id',
   {
    submissionAccountId: '$submissionAccountId',
    statusCounts: '$statusCounts',
    totalCount: '$totalCount'
   }
  ]
 }
}}]




