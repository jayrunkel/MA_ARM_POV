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
// Run this query against TXN_Latest if you only want the latest version
//
// TODO:
//  - replace 'FALSE' with false, when data is updated
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



