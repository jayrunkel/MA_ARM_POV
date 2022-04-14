const dbName = "marketaxess";
const colName = "txn1B";


// if true, the collections are not dropped before
const dropAllCollectionsAtStart = true;

const col = db.getSiblingDB(dbName).getCollection(colName);

const aggGroupColNames = [
	"agg1-txn1BGroupPreAgg",
	"agg2-txn1BGroupPreAgg",
	"agg3-txn1BGroupPreAgg",
	"agg4-txn1BGroupPreAgg",
	"agg5-txn1BGroupPreAgg",
];

// ================================================================
// Notes on Preaggregation queries:
//
// 1. Each aggregation query is listed below is prepended during query
//    execution with a match stage that selects one days worth of
//    data.
//
// 2. The aggregation queries product documents that preaggregate the
//    data per day. They create documents for each value of
//    submissionAccountId, executingEntityIdCodeLei, assetClass, and
//    nationalCompetentAuthority that occurs on that day. For the
//    count field, each document (each document represents a group) 
//    contains an array listing all the possible values of that field
//    and the counts.

//
// ================================================================


// ================ AGG 1 ================

const agg1PipelineNoMatch =
[{$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  status: '$status'
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
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority'
 },
 payloadTs: {
  $first: '$payloadTs'
 },
 statusCounts: {
  $push: {
   status: '$_id.status',
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
    payloadTs: {
     $dateTrunc: {
      date: '$payloadTs',
      unit: 'day'
     }
    },
    statusCounts: '$statusCounts',
    count: '$totalCount'
   }
  ]
 }
}}, {$merge: aggGroupColNames[0]}];


// ================ AGG 2 ================

const agg2PipelineNoMatch =
[{$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  status: '$status',
  subStatus: '$subStatus'
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
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority',
  status: '$_id.status'
 },
 payloadTs: {
  $first: '$payloadTs'
 },
 subStatusCounts: {
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
  assetClass: '$_id.assetClass',
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority'
 },
 payloadTs: {
  $first: '$payloadTs'
 },
 statusCounts: {
  $push: {
   status: '$_id.status',
   subStatusCounts: '$subStatusCounts',
   count: {
    $sum: '$totalCount'
   }
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
    payloadTs: {
     $dateTrunc: {
      date: '$payloadTs',
      unit: 'day'
     }
    },
    statusCounts: '$statusCounts',
    count: '$totalCount'
   }
  ]
 }
}}, {$merge: aggGroupColNames[1]}];
			
			
// ================ AGG 3 ================

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
}}, {$replaceRoot: {
 newRoot: {
  $mergeObjects: [
   '$_id',
   {
    status: '$status',
    payloadTs: {
     $dateTrunc: {
      date: '$payloadTs',
      unit: 'day'
     }
    },
    count: '$count',
    nonMiFidFlag: false
   }
  ]
 }
}}, {$merge: aggGroupColNames[2]}];


// ================ AGG 4 ================

const agg4PipelineNoMatch =
			[{$unwind: {
 path: '$errors'
}}, {$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  errorCode: '$errors.errorCode'
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
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority'
 },
 payloadTs: {
  $first: '$payloadTs'
 },
 errorCodes: {
  $push: {
   errorCode: '$_id.errorCode',
   count: '$count'
  }
 },
 count: {
  $sum: '$count'
 }
}}, {$replaceRoot: {
 newRoot: {
  $mergeObjects: [
   '$_id',
   {
    errorCodes: '$errorCodes',
    payloadTs: {
     $dateTrunc: {
      date: '$payloadTs',
      unit: 'day'
     }
    },
    count: '$count'
   }
  ]
 }
}}, {$merge: aggGroupColNames[3]}];


// ================ AGG 5 ================

const agg5PipelineNoMatch =
[{$unwind: {
 path: '$regResp'
}}, {$group: {
 _id: {
  submissionAccountId: '$submissionAccountId',
  executingEntityIdCodeLei: '$executingEntityIdCodeLei',
  assetClass: '$assetClass',
  nationalCompetentAuthority: '$nationalCompetentAuthority',
  regRespRuleId: '$regResp.ruleId'
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
  nationalCompetentAuthority: '$_id.nationalCompetentAuthority'
 },
 payloadTs: {
  $first: '$payloadTs'
 },
 regRespCounts: {
  $push: {
   regResp: {
    ruleId: '$_id.regRespRuleId',
    ruleDesc: '$regRespRuleDesc'
   },
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
    payloadTs: {
     $dateTrunc: {
      date: '$payloadTs',
      unit: 'day'
     }
    },
    regRespCounts: '$regRespCounts',
    count: '$totalCount'
   }
  ]
 }
}}, {$merge: aggGroupColNames[4]}];
			

function printDuration(start, end) {
	print("Execution time (s): ", Math.round((end - start) / 1000) );
}

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
	let startTime, endTime;

	print(" ");
	print("================================================================");
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

	//needed for agg4
	const dateMatchWithErrors =
				[{
		$match: {
			$expr: {
				$and: [{$gte: ['$payloadTs', ISODate(startDate)]},
							 {$lt: ['$payloadTs', ISODate(endDate)]},
							 {$gt: [{$size: '$errors'}, 0]}]}
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
	startTime = new Date();
	col.aggregate(dateMatch.concat(agg1PipelineNoMatch), {allowDiskUse: true});
	endTime = new Date();
	printDuration(startTime, endTime);

	print("agg2...");
	startTime = new Date();
	col.aggregate(dateMatch.concat(agg2PipelineNoMatch), {allowDiskUse: true});
	endTime = new Date();
	printDuration(startTime, endTime);

	print("agg3...")
	startTime = new Date();
	col.aggregate(dateMatchNoMiFid.concat(agg3PipelineNoMatch), {allowDiskUse: true});
	endTime = new Date();
	printDuration(startTime, endTime);
	
	print("agg4...")
	startTime = new Date();
	col.aggregate(dateMatchWithErrors.concat(agg4PipelineNoMatch), {allowDiskUse: true});
	endTime = new Date();
	printDuration(startTime, endTime);
	
	print("agg5...");
	startTime = new Date();
	col.aggregate(dateMatchWithRegResp.concat(agg5PipelineNoMatch), {allowDiskUse: true});
	endTime = new Date();
	printDuration(startTime, endTime);

}

if (dropAllCollectionsAtStart) {
	for (i=0;i < aggGroupColNames.length; i++) {
		print("Building Index for: " + aggGroupColNames[i]);
		db.getCollection(aggGroupColNames[i]).createIndex({submissionAccountId: 1, executingEntityIdCodeLei: 1, payloadTs: 1})
	}
}
