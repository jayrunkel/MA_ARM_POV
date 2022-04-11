// ================================================================
//
// View: TXN_Latest
//
// View used to calculate the latest version of the transaction for a
// given reference number that has the largest values of payloadTS and
// payloadPosition.
//
// Requires index. Need to investigate the best index to add to support
// the $sort followed by a $group
//
//
// ================================================================

[{$sort: {
 payloadTs: 1,
 payloadPosition: 1
}}, {$group: {
 _id: '$transactionReferenceNumber',
 transactions: {
  $last: '$$CURRENT'
 }
}}, {$replaceRoot: {
 newRoot: '$transactions'
}}]
