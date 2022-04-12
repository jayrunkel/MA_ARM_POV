#!/usr/bin/env python

########################################################################
#
# ANYTHING THAT REQUIRES YOUR ATTENTION WILL HAVE A TODO IN THE COMMENTS
# Do not create external files outside of this locust file!
# mLocust only allows you to upload a single python file atm.
# Please keep everything in this 1 file.
#
########################################################################

# Allows us to make many pymongo requests in parallel to overcome the single threaded problem
import gevent
_ = gevent.monkey.patch_all()

########################################################################
# TODO Add any additional imports here.
# TODO Make sure to include in requirements.txt if necessary
########################################################################
import pymongo
from bson import json_util
from bson.json_util import loads
from bson import ObjectId
from locust import User, events, task, constant, tag, between
import time
from datetime import datetime, timedelta
import random
import pprint

########################################################################
# Global Static Variables that can be accessed without referencing self
# The values are initialized with None till they get set from the
# actual locust exeuction when the host param is passed in.
########################################################################
# DO NOT MODIFY! PASS IN VIA HOST PARAM.
client = None
coll = None
audit = None

class MetricsLocust(User):
    ####################################################################
    # All performance POVs measure based on requests/sec (RPS).
    # Throttle all tasks per user to run every second.
    # Do not touch this parameter.
    # If we don't throttle, then each user will run as fast as possible,
    # and will kill the CPU of the machine.
    # We can increase throughput by running more concurrent users.
    ####################################################################
    wait_time = between(1, 1)

    ####################################################################
    # Initialize any env vars from the host parameter
    # Make sure it's a singleton so we only set conn pool once
    # Set the target collections and such here
    ####################################################################
    def __init__(self, parent):
        super().__init__(parent)

        # specify that the following vars are global vars
        global client, coll, audit

        # Singleton
        # TODO Pass in the env vars using the Host field for locust, e.g. srv|db|coll
        # TODO Make sure your srv has the right read and write preference to optimize perf
        if (client is None):
            # Parse out env variables from the host
            # FYI, you can pass in more env vars if you so choose
            vars = self.host.split("|")
            srv = vars[0]
            print("SRV:",srv)
            client = pymongo.MongoClient(srv)

            # Define the target db and coll here
            db = client[vars[1]]
            coll = db[vars[2]]

            # Log all application exceptions (and audits) to the same cluster
            audit = client.mlocust.audit

    ################################################################
    # Example helper function that is not a Locust task.
    # All Locust tasks require the @task annotation
    # You have to pass the self reference for all helper functions
    # TODO Create any additional helper functions here
    ################################################################
    def get_time(self):
        return time.time()

    ################################################################
    # Audit should only be intended for logging errors
    # Otherwise, it impacts the load on your cluster since it's
    # extra work that needs to be performed on your cluster
    ################################################################
    def audit(self, type, msg):
        print("Audit: ", msg)
        audit.insert_one({"type":type, "ts":self.get_time(), "msg":str(msg)})

    ################################################################
    # Description: 
    #    Count of status 
    #    Group by submissionAccountId, executingEntityIdCodeLei, assetClass, 
    #          payloadTs, nationalCompetentAuthority
    ################################################################
    @task(0)
    def reportingVolumeAgg(self):
        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "reportingVolumeAgg";

        try:
            # Reproduce faker fields for our queries
            # Random number of days (1-90 from today) to substract for payloadTs
            ldTradeDT = random.randint(1, 90)
            # submissionAccountId
            lAcctNumSM = random.randint(10, 500)
            lAcctNumLarge = random.randint(1, 6)
            lAcctList = [lAcctNumLarge, lAcctNumSM]
            lAcctNumA = random.choices(lAcctList, weights=(90, 10), k=1)
            lAcctNum = lAcctNumA[0]
            # executingEntityIdCodeLei
            lLeiCodeLg1 = random.randint(1, 10)
            lLeiCodeSm1 = random.randint(1, 5)
            lLeiCode1 = lLeiCodeLg1 if lAcctNum < 6 else lLeiCodeSm1
            lLeiCodeLg2 = random.randint(1, 10)
            lLeiCodeSm2 = random.randint(1, 5)
            lLeiCode2 = lLeiCodeLg2 if lAcctNum < 6 else lLeiCodeSm2
            lLeiCodeLg3 = random.randint(1, 10)
            lLeiCodeSm3 = random.randint(1, 5)
            lLeiCode3 = lLeiCodeLg3 if lAcctNum < 6 else lLeiCodeSm3
            # EK: payloadts offset
            payloadOffset = random.randint(1, 3)
            payloadTsEnd = datetime.now() - timedelta(days=ldTradeDT + payloadOffset)
            payloadTsStart = datetime.now() - timedelta(days=ldTradeDT + payloadOffset + 10)

            # Debug
            # print("lei1:",f'LEY_{lAcctNum}_{lLeiCode1}')
            # print("payloadts start:", payloadTsStart)
            # print("payloadts end:", payloadTsEnd)

            # build up the pipeline
            pipeline = [
                {"$match": {
                    "submissionAccountId": lAcctNum,
                    "executingEntityIdCodeLei": {
                        "$in": [
                            f'LEY_{lAcctNum}_{lLeiCode1}',
                            f'LEY_{lAcctNum}_{lLeiCode2}',
                            f'LEY_{lAcctNum}_{lLeiCode2}'
                        ]
                    },
                    "payloadTs": {
                        "$gt": payloadTsStart,
                        "$lt": payloadTsEnd
                    }
                }},
                {"$group": {
                    "_id": {
                        "executingEntityIdCodeLei": '$executingEntityIdCodeLei',
                        "assetClass": '$assetClass',
                        "status": '$status',
                        "nationalCompetentAuthority": '$nationalCompetentAuthority'
                    },
                    "count": {
                        "$count": {}
                    }
                }},
                {"$group": {
                    "_id": {
                        "assetClass": '$_id.assetClass',
                        "nationalCompetentAuthority": '$_id.nationalCompetentAuthority'
                    },
                    "executingEntityIdCodeLei": {
                        "$addToSet": '$_id.executingEntityIdCodeLei'
                    },
                    "statusCounts": {
                        "$push": {
                            "status": '$_id.status',
                            "count": '$count'
                        }
                    }
                }},
                {"$project": {
                    "_id": 0,
                    "executingEntityIdCodeLei": 1,
                    "assetClass": '$_id.assetClass',
                    "nationalCompetentAuthority": '$_id.nationalCompetentAuthority',
                    "statusCounts": 1
                }}
            ]

            # Get the record from the target collection now
            pprint.pprint(list(coll.aggregate(pipeline)))
            events.request_success.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request_failure.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            self.audit("exception", e)

    ################################################################
    # Description: 
    #    Count of status, subStatus
    #    Group by submissionAccountId, executingEntityIdCodeLei, assetClass,
    #          payloadTs, nationalCompetentAuthority
    ################################################################
    @task(0)
    def tradeByStatusAgg(self):
        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "tradeByStatusAgg";

        try:
            # Reproduce faker fields for our queries
            # Random number of days (1-90 from today) to substract for payloadTs
            ldTradeDT = random.randint(1, 90)
            # submissionAccountId
            lAcctNumSM = random.randint(10, 500)
            lAcctNumLarge = random.randint(1, 6)
            lAcctList = [lAcctNumLarge, lAcctNumSM]
            lAcctNumA = random.choices(lAcctList, weights=(90, 10), k=1)
            lAcctNum = lAcctNumA[0]
            # executingEntityIdCodeLei
            lLeiCodeLg1 = random.randint(1, 10)
            lLeiCodeSm1 = random.randint(1, 5)
            lLeiCode1 = lLeiCodeLg1 if lAcctNum < 6 else lLeiCodeSm1
            lLeiCodeLg2 = random.randint(1, 10)
            lLeiCodeSm2 = random.randint(1, 5)
            lLeiCode2 = lLeiCodeLg2 if lAcctNum < 6 else lLeiCodeSm2
            lLeiCodeLg3 = random.randint(1, 10)
            lLeiCodeSm3 = random.randint(1, 5)
            lLeiCode3 = lLeiCodeLg3 if lAcctNum < 6 else lLeiCodeSm3
            # EK: payloadts offset
            payloadOffset = random.randint(1, 3)
            payloadTsEnd = datetime.now() - timedelta(days=ldTradeDT + payloadOffset)
            payloadTsStart = datetime.now() - timedelta(days=ldTradeDT + payloadOffset + 10)
            laTxnStatus = ["RAC", "RRJ", "AREJ", "REJ"]
            laTxnSelection = random.choices(laTxnStatus, weights=(7, 1, 1, 1), k=1)
            lcTxnStatus = laTxnSelection[0]

            # Debug
            # print("lei1:",f'LEY_{lAcctNum}_{lLeiCode1}')
            # print("payloadts start:", payloadTsStart)
            # print("payloadts end:", payloadTsEnd)

            # build up the pipeline
            pipeline = [
                {"$match": {
                    "submissionAccountId": lAcctNum,
                    "executingEntityIdCodeLei": {
                        "$in": [
                            f'LEY_{lAcctNum}_{lLeiCode1}',
                            f'LEY_{lAcctNum}_{lLeiCode2}',
                            f'LEY_{lAcctNum}_{lLeiCode2}'
                        ]
                    },
                    "payloadTs": {
                        "$gt": payloadTsStart,
                        "$lt": payloadTsEnd
                    },
                    "status": lcTxnStatus
                }},
                {"$group": {
                    "_id": {
                        "assetClass": '$assetClass',
                        "status": '$status',
                        "subStatus": '$subStatus',
                        "nationalCompetentAuthority": '$nationalCompetentAuthority'
                    },
                    "executingEntityIdCodeLei": {
                        "$addToSet": '$executingEntityIdCodeLei'
                    },
                    "count": {
                        "$count": {}
                    }
                }},
                {"$group": {
                    "_id": {
                        "assetClass": '$_id.assetClass',
                        "nationalCompetentAuthority": '$_id.nationalCompetentAuthority',
                        "status": '$_id.status'
                    },
                    "executingEntityIdCodeLei": {
                        "$addToSet": '$_id.executingEntityIdCodeLei'
                    },
                    "subStatusCounts": {
                        "$push": {
                            "subStatus": '$_id.subStatus',
                            "count": '$count'
                        }
                    },
                    "statusCount": {
                        "$sum": '$count'
                    }
                }},
                {"$addFields": {
                    "executingEntityIdCodeLei": {
                        "$reduce": {
                            "input": '$executingEntityIdCodeLei',
                            "initialValue": [],
                            "in": {
                                "$setUnion": [
                                    '$$value',
                                    '$$this'
                                ]
                            }
                        }
                    }
                }},
                {"$group": {
                    "_id": {
                        "assetClass": '$_id.assetClass',
                        "nationalCompetentAuthority": '$_id.nationalCompetentAuthority'
                    },
                    "executingEntityIdCodeLei": {
                        "$addToSet": '$executingEntityIdCodeLei'
                    },
                    "statusCounts": {
                        "$push": {
                            "status": '$_id.status',
                            "statusCount": '$statusCount',
                            "subStatusCounts": '$subStatusCounts'
                        }
                    },
                    "totalCount": {
                        "$sum": '$statusCount'
                    }
                }},
                {"$project": {
                    "_id": 0,
                    "executingEntityIdCodeLei": {
                        "$reduce": {
                            "input": '$executingEntityIdCodeLei',
                            "initialValue": [],
                            "in": {
                               "$setUnion": [
                                   '$$value',
                                   '$$this'
                                ]
                            }
                        }
                    },
                    "assetClass": '$_id.assetClass',
                    "nationalCompetentAuthority": '$_id.nationalCompetentAuthority',
                    "statusCounts": 1,
                    "totalCount": 1
                }} 
            ]

            # Get the record from the target collection now
            pprint.pprint(list(coll.aggregate(pipeline)))
            events.request_success.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request_failure.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            self.audit("exception", e)

    ################################################################
    # Description: 
    #    Count of Docs for: nonMiFidFlag=”FALSE” AND
    #    Status != “AREJ”
    #    group by submissionAccountId, executingEntityIdCodeLei, assetClass, 
    #          payloadTs, nationalCompetentAuthority
    ################################################################
    @task(0)
    def mifidIneligibleAgg(self):
        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "mifidIneligibleAgg";

        try:
            # Reproduce faker fields for our queries
            # Random number of days (1-90 from today) to substract for payloadTs
            ldTradeDT = random.randint(1, 90)
            # submissionAccountId
            lAcctNumSM = random.randint(10, 500)
            lAcctNumLarge = random.randint(1, 6)
            lAcctList = [lAcctNumLarge, lAcctNumSM]
            lAcctNumA = random.choices(lAcctList, weights=(90, 10), k=1)
            lAcctNum = lAcctNumA[0]
            # executingEntityIdCodeLei
            lLeiCodeLg1 = random.randint(1, 10)
            lLeiCodeSm1 = random.randint(1, 5)
            lLeiCode1 = lLeiCodeLg1 if lAcctNum < 6 else lLeiCodeSm1
            lLeiCodeLg2 = random.randint(1, 10)
            lLeiCodeSm2 = random.randint(1, 5)
            lLeiCode2 = lLeiCodeLg2 if lAcctNum < 6 else lLeiCodeSm2
            lLeiCodeLg3 = random.randint(1, 10)
            lLeiCodeSm3 = random.randint(1, 5)
            lLeiCode3 = lLeiCodeLg3 if lAcctNum < 6 else lLeiCodeSm3
            # EK: payloadts offset
            payloadOffset = random.randint(1, 3)
            payloadTsEnd = datetime.now() - timedelta(days=ldTradeDT + payloadOffset)
            payloadTsStart = datetime.now() - timedelta(days=ldTradeDT + payloadOffset + 10)
            laTxnStatus = ["RAC", "RRJ", "AREJ", "REJ"]
            laTxnSelection = random.choices(laTxnStatus, weights=(7, 1, 1, 1), k=1)
            lcTxnStatus = laTxnSelection[0]

            # Debug
            # print("lei1:",f'LEY_{lAcctNum}_{lLeiCode1}')
            # print("payloadts start:", payloadTsStart)
            # print("payloadts end:", payloadTsEnd)

            # build up the pipeline
            # TODO is this query correct?
            pipeline = [
                {"$match": {
                    "submissionAccountId": lAcctNum,
                    "executingEntityIdCodeLei": {
                        "$in": [
                            f'LEY_{lAcctNum}_{lLeiCode1}',
                            f'LEY_{lAcctNum}_{lLeiCode2}',
                            f'LEY_{lAcctNum}_{lLeiCode2}'
                        ]
                    },
                    "payloadTs": {
                        "$gt": payloadTsStart,
                        "$lt": payloadTsEnd
                    },
                    "status": 'AREJ'
                }},
                {"$match": {
                    "nonMiFidFlag": False,
                    "status": 'AREJ'
                }},
                {"$group": {
                    "_id": {
                        "assetClass": '$assetClass',
                        "nationalCompetentAuthority": '$nationalCompetentAuthority'
                    },
                    "executingEntityIdCodeLei": {
                        "$addToSet": '$executingEntityIdCodeLei'
                    },
                    "count": {
                        "$count": {}
                    }
                }},
                {"$project": {
                    "_id": 0,
                    "executingEntityIdCodeLei": 1,
                    "assetClass": '$_id.assetClass',
                    "nationalCompetentAuthority": '$_id.nationalCompetentAuthority',
                    "count": 1
                }}                
            ]

            # Get the record from the target collection now
            pprint.pprint(list(coll.aggregate(pipeline)))
            events.request_success.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request_failure.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            self.audit("exception", e)

    ################################################################
    # Description: 
    #    count of errors.errorCode
    #    Group by submissionAccountId, executingEntityIdCodeLei, assetClass, 
    #         payloadTs, nationalCompetentAuthority
    ################################################################
    @task(0)
    def traxRejectionsAgg(self):
        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "traxRejectionsAgg";

        try:
            # Reproduce faker fields for our queries
            # Random number of days (1-90 from today) to substract for payloadTs
            ldTradeDT = random.randint(1, 90)
            # submissionAccountId
            lAcctNumSM = random.randint(10, 500)
            lAcctNumLarge = random.randint(1, 6)
            lAcctList = [lAcctNumLarge, lAcctNumSM]
            lAcctNumA = random.choices(lAcctList, weights=(90, 10), k=1)
            lAcctNum = lAcctNumA[0]
            # executingEntityIdCodeLei
            lLeiCodeLg1 = random.randint(1, 10)
            lLeiCodeSm1 = random.randint(1, 5)
            lLeiCode1 = lLeiCodeLg1 if lAcctNum < 6 else lLeiCodeSm1
            lLeiCodeLg2 = random.randint(1, 10)
            lLeiCodeSm2 = random.randint(1, 5)
            lLeiCode2 = lLeiCodeLg2 if lAcctNum < 6 else lLeiCodeSm2
            lLeiCodeLg3 = random.randint(1, 10)
            lLeiCodeSm3 = random.randint(1, 5)
            lLeiCode3 = lLeiCodeLg3 if lAcctNum < 6 else lLeiCodeSm3
            # EK: payloadts offset
            payloadOffset = random.randint(1, 3)
            payloadTsEnd = datetime.now() - timedelta(days=ldTradeDT + payloadOffset)
            payloadTsStart = datetime.now() - timedelta(days=ldTradeDT + payloadOffset + 10)
            laTxnStatus = ["RAC", "RRJ", "AREJ", "REJ"]
            laTxnSelection = random.choices(laTxnStatus, weights=(7, 1, 1, 1), k=1)
            lcTxnStatus = laTxnSelection[0]

            # Debug
            # print("lei1:",f'LEY_{lAcctNum}_{lLeiCode1}')
            # print("payloadts start:", payloadTsStart)
            # print("payloadts end:", payloadTsEnd)

            # build up the pipeline
            # TODO is this query correct?
            pipeline = [
                {"$match": {
                    "submissionAccountId": lAcctNum,
                    "executingEntityIdCodeLei": {
                        "$in": [
                            f'LEY_{lAcctNum}_{lLeiCode1}',
                            f'LEY_{lAcctNum}_{lLeiCode2}',
                            f'LEY_{lAcctNum}_{lLeiCode2}'
                        ]
                    },
                    "payloadTs": {
                        "$gt": payloadTsStart,
                        "$lt": payloadTsEnd
                    }
                }},
                {"$group": {
                    "_id": {
                        "assetClass": '$assetClass',
                        "errorCodes": '$errorCodes',
                        "nationalCompetentAuthority": '$nationalCompetentAuthority'
                    },
                    "executingEntityIdCodeLei": {
                        "$addToSet": '$executingEntityIdCodeLei'
                    },
                    "count": {
                        "$count": {}
                    }
                }},
                {"$group": {
                    "_id": {
                        "assetClass": '$_id.assetClass',
                        "nationalCompetentAuthority": '$_id.nationalCompetentAuthority'
                    },
                    "executingEntityIdCodeLei": {
                        "$addToSet": '$executingEntityIdCodeLei'
                    },
                    "errorCodesCounts": {
                        "$push": {
                            "errorCodes": '$_id.errorCodes',
                            "count": '$count'
                        }
                    }
                }},                
                {"$project": {
                    "_id": 0,
                    "executingEntityIdCodeLei": {
                        "$reduce": {
                            "input": '$executingEntityIdCodeLei',
                            "initialValue": [],
                            "in": {
                                "$setUnion": [
                                    '$$value',
                                    '$$this'
                                ]
                            }
                        }
                    },
                    "assetClass": '$_id.assetClass',
                    "nationalCompetentAuthority": '$_id.nationalCompetentAuthority',
                    "errorCodesCounts": 1
                }}                
            ]

            # Get the record from the target collection now
            pprint.pprint(list(coll.aggregate(pipeline)))
            events.request_success.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request_failure.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            self.audit("exception", e)

    ################################################################
    # Description: 
    #    Count of regResp.ruleId
    #    Group by submissionAccountId, executingEntityIdCodeLei, assetClass, 
    #         payloadTs, nationalCompetentAuthority
    ################################################################
    @task(1)
    def regulatorRejectionAgg(self):
        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "regulatorRejectionAgg";

        try:
            # Reproduce faker fields for our queries
            # Random number of days (1-90 from today) to substract for payloadTs
            ldTradeDT = random.randint(1, 90)
            # submissionAccountId
            lAcctNumSM = random.randint(10, 500)
            lAcctNumLarge = random.randint(1, 6)
            lAcctList = [lAcctNumLarge, lAcctNumSM]
            lAcctNumA = random.choices(lAcctList, weights=(90, 10), k=1)
            lAcctNum = lAcctNumA[0]
            # executingEntityIdCodeLei
            lLeiCodeLg1 = random.randint(1, 10)
            lLeiCodeSm1 = random.randint(1, 5)
            lLeiCode1 = lLeiCodeLg1 if lAcctNum < 6 else lLeiCodeSm1
            lLeiCodeLg2 = random.randint(1, 10)
            lLeiCodeSm2 = random.randint(1, 5)
            lLeiCode2 = lLeiCodeLg2 if lAcctNum < 6 else lLeiCodeSm2
            lLeiCodeLg3 = random.randint(1, 10)
            lLeiCodeSm3 = random.randint(1, 5)
            lLeiCode3 = lLeiCodeLg3 if lAcctNum < 6 else lLeiCodeSm3
            # EK: payloadts offset
            payloadOffset = random.randint(1, 3)
            payloadTsEnd = datetime.now() - timedelta(days=ldTradeDT + payloadOffset)
            payloadTsStart = datetime.now() - timedelta(days=ldTradeDT + payloadOffset + 10)
            laTxnStatus = ["RAC", "RRJ", "AREJ", "REJ"]
            laTxnSelection = random.choices(laTxnStatus, weights=(7, 1, 1, 1), k=1)
            lcTxnStatus = laTxnSelection[0]

            # Debug
            # print("lei1:",f'LEY_{lAcctNum}_{lLeiCode1}')
            # print("payloadts start:", payloadTsStart)
            # print("payloadts end:", payloadTsEnd)

            # build up the pipeline
            # TODO is this query correct?
            pipeline = [
                {"$match": {
                    "submissionAccountId": lAcctNum,
                    "executingEntityIdCodeLei": {
                        "$in": [
                            f'LEY_{lAcctNum}_{lLeiCode1}',
                            f'LEY_{lAcctNum}_{lLeiCode2}',
                            f'LEY_{lAcctNum}_{lLeiCode2}'
                        ]
                    },
                    "payloadTs": {
                        "$gt": payloadTsStart,
                        "$lt": payloadTsEnd
                    }
                }},
                {"$unwind": {
                    "path": '$regResp'
                }},
                {"$group": {
                    "_id": {
                        "assetClass": '$assetClass',
                        "regRespRuleId": '$regResp.ruleId',
                        "nationalCompetentAuthority": '$nationalCompetentAuthority'
                    },
                    "executingEntityIdCodeLei": {
                        "$addToSet": '$executingEntityIdCodeLei'
                    },
                    "count": {
                        "$count": {}
                    }
                }},
                {"$group": {
                    "_id": {
                        "assetClass": '$_id.assetClass',
                        "nationalCompetentAuthority": '$_id.nationalCompetentAuthority'
                    },
                    "executingEntityIdCodeLei": {
                        "$addToSet": '$executingEntityIdCodeLei'
                    },
                    "regRespRuleIdCounts": {
                        "$push": {
                            "ruleId": '$_id.regRespRuleId',
                            "count": '$count'
                        }
                    }
                }},                
                {"$project": {
                    "_id": 0,
                    "executingEntityIdCodeLei": {
                        "$reduce": {
                            "input": '$executingEntityIdCodeLei',
                            "initialValue": [],
                            "in": {
                                "$setUnion": [
                                    '$$value',
                                    '$$this'
                                ]
                            }
                        }
                    },
                    "assetClass": '$_id.assetClass',
                    "nationalCompetentAuthority": '$_id.nationalCompetentAuthority',
                    "regRespRuleIdCounts": 1
                }}                
            ]

            # Get the record from the target collection now
            pprint.pprint(list(coll.aggregate(pipeline)))
            events.request_success.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request_failure.fire(request_type="pymongo", name=name, response_time=(time.time()-tic)*1000, response_length=0, exception=e)
            self.audit("exception", e)
