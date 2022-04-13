

let pipeline =
		[{$match: {
 payloadTs: {
  $gt: ISODate('2022-03-01T00:00:00.000Z'),
  $lt: ISODate('2022-03-02T00:00:00.000Z')
 }
}}, {$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  status: '$status',
  nationalCompetentAuthority: '$nationalCompetentAuthority'
 },
 payloadTs: {
  $first: '$payloadTs'
 },
 count: {
  $count: {}
 }
}}, {$group: {
 _id: {
  submissionAccountId: '$_id.submissionAccountId',
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
  assetClass: '$_id.assetClass',
  status: '$_id.status'
 },
 payloadTs: {
  $first: '$payloadTs'
 },
 nationalCompetentAuthorityCounts: {
  $push: {
   nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
   count: '$count'
  }
 },
 count: {
  $sum: '$count'
 }
}}, {$unwind: {
 path: '$nationalCompetentAuthorityCounts'
}}, {$group: {
 _id: {
  submissionAccountId: '$_id.submissionAccountId',
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
  assetClass: '$_id.assetClass'
 },
 payloadTs: {
  $first: '$payloadTs'
 },
 pairCount: {
  $addToSet: {
   nationalCompetentAuthority: '$nationalCompetentAuthorityCounts.nationalCompetentAuthority',
   status: '$_id.status',
   count: {
    $sum: '$nationalCompetentAuthorityCounts.count'
   }
  }
 },
 count: {
  $count: {}
 }
}}, {$unwind: {
 path: '$pairCount'
}}, {$group: {
 _id: {
  submissionAccountId: '$_id.submissionAccountId',
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei'
 },
 counts: {
  $addToSet: {
   nationalCompetentAuthority: '$pairCount.nationalCompetentAuthority',
   status: '$pairCount.status',
   assetClass: '$_id.assetClass',
   count: {
    $sum: '$pairCount.count'
   }
  }
 },
 payloadTs: {
  $first: '$payloadTs'
 }
}}, {$project: {
 _id: 0,
 submissionAccountId: '$_id.submissionAccountId',
 executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei',
 payloadTs: {
  $dateTrunc: {
   date: '$payloadTs',
   unit: 'day'
  }
 },
 counts: 1
}}, {$out: 'txnVersionsGroupPreAgg'}];
