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
   'LEY_2_3'
  ]
 }
}}, {$project: {
 _id: 0
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
}}, {$group: {
 _id: {
  assetClass: '$assetClass',
  status: '$status',
  subStatus: '$subStatus',
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
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
  status: '$_id.status'
 },
 executingEntityIdCodeLei: {
  $addToSet: '$executingEntityIdCodeLei'
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
}}, {$addFields: {
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
 }
}}, {$group: {
 _id: {
  assetClass: '$_id.assetClass',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority'
 },
 executingEntityIdCodeLei: {
  $addToSet: '$executingEntityIdCodeLei'
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
 statusCounts: 1,
 totalCount: 1
}}]


