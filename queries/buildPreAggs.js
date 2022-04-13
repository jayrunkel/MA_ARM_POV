const dbName = "marketaxess";
const colName = "txnVersions";


// if true, the collections are not dropped before
const dropAllCollectionsAtStart = true;

const col = db.getSiblingDB(dbName).getCollection(colName);

const aggGroupColNames = [
	"agg1-txnVersionsGroupPreAgg",
	"agg2-txnVersionsGroupPreAgg",
	"agg3-txnVersionsGroupPreAgg",
	"agg4-txnVersionsGroupPreAgg",
	"agg5-txnVersionsGroupPreAgg",
];

const agg1PipelineNoMatch = [{$group: {
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
}}, {$merge: {
	into: 'agg1-txnVersionsGroupPreAgg',
}}];

const agg3PipelineNoMatch =
			[{$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  nationalCompetentAuthority: '$nationalCompetentAuthority'
 },
 status: {
  $addToSet: '$status'
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
  assetClass: '$_id.assetClass'
 },
 status: {
  $first: '$status'
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
  executingEntityIdCodeLei: '$_id.executingEntityIdCodeLei'
 },
 status: {
  $first: '$status'
 },
 counts: {
  $addToSet: {
   nationalCompetentAuthority: '$nationalCompetentAuthorityCounts.nationalCompetentAuthority',
   assetClass: '$_id.assetClass',
   count: {
    $sum: '$nationalCompetentAuthorityCounts.count'
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
 status: 1,
 counts: 1
}}, {$addFields: {
 nonMiFidFlag: false
}}, {$merge: 'agg3-txnVersionsGroupPreAgg'}]

const agg5PipelineNoMatch =
		[{$unwind: {
 path: '$regResp'
}}, {$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  regRespRuleId: '$regResp.ruleId',
  nationalCompetentAuthority: '$nationalCompetentAuthority'
 },
 regRespRuleDesc: {
  $first: '$regResp.ruleDesc'
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
  regRespRuleId: '$_id.regRespRuleId'
 },
 payloadTs: {
  $first: '$payloadTs'
 },
 regRespRuleDesc: {
  $first: '$regRespRuleDesc'
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
 regRespRuleDesc: {
  $first: '$regRespRuleDesc'
 },
 pairCount: {
  $addToSet: {
   nationalCompetentAuthority: '$nationalCompetentAuthorityCounts.nationalCompetentAuthority',
   regRespRuleId: '$_id.regRespRuleId',
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
   regResp: {
    ruleId: '$pairCount.regRespRuleId',
    ruleDesc: '$regRespRuleDesc'
   },
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
}}, {$merge: 'agg5-txnVersionsGroupPreAgg'}];

// Drop all the preagg collections
if (dropAllCollectionsAtStart) {
	for (i=0;i < aggGroupColNames.length; i++) {
		print("Dropping collection: " + aggGroupColNames[i]);
		db.getCollection(aggGroupColNames[i]).drop()
	}
}


for (i=1; i<11; i++) {

	const twoDigitDayStart = i < 10 ? "0" + i : i;
	const twoDigitDayEnd = i < 9 ? "0"+(i+1) : i+1;
	const startDate = `2022-03-${twoDigitDayStart}T00:00:00.000Z`;
	const endDate = `2022-03-${twoDigitDayEnd}T00:00:00.000Z`;

	print("Processing date: ");
	printjson({startDate: startDate, endDate: endDate});

	const dateMatch =
				[{$match: {
					payloadTs: {
						$gte: ISODate(startDate),
						$lt: ISODate(endDate)
					}
				}}];

	// needed for agg3
	const dateMatchNoMiFid =
				[{$match: {
					payloadTs: {
						$gte: ISODate(startDate),
						$lt: ISODate(endDate)
					},
					nonMiFidFlag: false,
					status: {
						$ne: 'AREJ'
					}
				}}];
	
	// needed for agg5
	const dateMatchWithRegResp = [{
		$match: {
			$expr: {
				$and: [{$gte: ['$payloadTs', ISODate(startDate)]},
							 {$lt: ['$payloadTs', ISODate(endDate)]},
							 {$gt: [{$size: '$regResp'}, 0]}]}
		}}];


	print("agg1...");
	col.aggregate(dateMatch.concat(agg1PipelineNoMatch), {allowDiskUse: true});
	print("agg3...")
	col.aggregate(dateMatchNoMiFid.concat(agg3PipelineNoMatch), {allowDiskUse: true});
	print("agg5...");
	col.aggregate(dateMatchWithRegResp.concat(agg5PipelineNoMatch), {allowDiskUse: true});
}
  
