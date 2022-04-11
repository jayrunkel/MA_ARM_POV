from pickle import TRUE
from locust import task, between
from datetime import datetime, timedelta
from mongo_user import MongoUser
from settings import COLLECTION_NAME

import pymongo
import random


# docs to insert per batch insert
DOCS_PER_BATCH = 100

# number of cache entries for queries
NAMES_TO_CACHE = 1000

#Initialize ldTradeDT
ldTradeDT = random.randint(1, 90)

# AssetClass
my_AssetClass = ["BILL", "BOND", "CASH", "CMDTY", "FOP", "FSOPT", "FUT", "FXCFD", "OPT", "STK", "WAR", "CLS12", "CLS13", "CLS14", "CLS15", "CLS16", "CLS17", "CLS18", "CLS19", "CLS20"]

#nationalCompetentAuthority
my_tradeCapacity = ["DEAL", "MTCH", ""]


class MongoSampleUser(MongoUser):
    """
    Generic sample mongodb workload generator
    """
    # no delays between operations
    wait_time = between(0.0, 0.0)

    def __init__(self, environment):
        super().__init__(environment)
        self.name_cache = []

    def generate_new_document(self):
        """
        Generate a new sample document
        """
        # Random number of days (1-90 from today) to substract for payloadTs
        ldTradeDT = random.randint(1, 90)
        # submissionAccountId
        lAcctNumSM = random.randint(10, 500)
        lAcctNumLarge = random.randint(1, 6)
        lAcctList = [lAcctNumLarge, lAcctNumSM]
        lAcctNumA = random.choices(lAcctList, weights=(90, 10), k=1)
        lAcctNum = lAcctNumA[0]
        # executingEntityIdCodeLei
        lLeiCodeLg = random.randint(1, 50)
        lLeiCodeSm = random.randint(1, 5)
        lLeiCode = lLeiCodeLg if lAcctNum < 6 else lLeiCodeSm
        # reportStatus ["REPL", "CANC", "NEWM"] "weights": [1, 1, 8]
        laReportStatus = ["REPL", "CANC", "NEWM"]
        laRptSelection = random.choices(laReportStatus, weights=(1, 1, 8), k=1)
        lcRptStatus = laRptSelection[0]
        # status ["RAC", "RRJ", "AREJ", "REJ"] "weights": [1, 1, 1, 1]
        laTxnStatus = ["RAC", "RRJ", "AREJ", "REJ"]
        laTxnSelection = random.choices(laTxnStatus, weights=(7, 1, 1, 1), k=1)
        lcTxnStatus = laTxnSelection[0]
        # errors.errorCode
        ErrsubDoc = {"errors": [
            {
                "fieldRejected": "BUYER_FIRST_NAMES",
                "errorCode": "E1018"
            },
            {
                "fieldRejected": "BUYER_DATE_OF_BIRTH",
                "errorCode": "E1020"
            }
            ]}
        ErrsubDocEmpty = {"errors": []}
        lcErrors = ErrsubDoc if lcTxnStatus != "RAC" else ErrsubDocEmpty
        # regResp.ruleId
        RegRespDoc = {"regResp": [
            {
            "ruleId": f'CON-{random.randint(1, 200)}',
            "ruleDesc": "Transaction report desription123..."
            },
            {
            "ruleId": f'CON-{random.randint(201, 500)}',
            "ruleDesc": "Instrument is not a valid XYZ..."
            }
        ]}
        RegRespDocDocEmpty = {"regResp": []}
        lcRegResponse = RegRespDocDocEmpty if lcTxnStatus != "RRJ" else RegRespDoc

        document = {
            'submissionAccountName': f'Account{lAcctNum}',
            'submissionAccountId': lAcctNum,
            'executingEntityIdCode': self.faker.pystr(min_chars=3, max_chars=5),
            'executingEntityIdCodeType': self.faker.pystr(min_chars=5, max_chars=8),
            'executingEntityIdCodeLei': f'LEY_{lAcctNum}_{lLeiCode}',
            "transactionReferenceNumber": f'TRNXREF_{lAcctNum}_{random.randint(10000, 10000000)}',
            "instructionSetKey": "TRXREFNR123635400BDQCJNMOGTBB61",
            "regulator": f'BaFIN{random.randint(1, 25)}',
            "reportStatus": lcRptStatus,
            "status": lcTxnStatus,
            "subStatus": "",
            'payloadTs': datetime.now() - timedelta(days=ldTradeDT + random.randint(1, 3)),
            "payloadPosition": random.randint(1, 5),
            "assetClass": my_AssetClass[random.randint(0, 19)],
            "tradingVenueTransactionIdCode": "TRXREFNR123",
            "mifidInvestmentFirm": self.faker.boolean(),
            "nationalCompetentAuthority": f'NCADE{random.randint(1, 25)}',
            "buyers": {
                        "buyerId": random.randint(1000, 1000000),
                        "buyerCode": f'BrCode{random.randint(1, 50)}',
                        "buyerCodeType": "LEI",
                        "buyerBranchCountry": "DE"
                      },
            "sellers": {
                        "sellerId": random.randint(1000, 1000000),
                        "sellerCode": f'SrCode{random.randint(1, 50)}',
                        "sellerCodeType": "LEI",
                        "sellerBranchCountry": "DE"
                      },
            "tradePrice": self.faker.pydecimal(min_value=10, max_value=10000000, right_digits=2),
            "tradeQuantity": self.faker.pydecimal(min_value=10, max_value=10000000, right_digits=2),
            "tradeDateTime": datetime.now() - timedelta(days=ldTradeDT),
            "tradeDateTimeForSearch": 1643184780000,
            "tradeCapacity": my_tradeCapacity[random.randint(0, 2)],
            "tradePriceCurrency": "EUR",
            "tradeVenueId": "MFXR",
            "instrumentCode": f'InstrCode{random.randint(1, 100000)}',
            "executionWithinFirmCodeType": f'eFMCode{random.randint(1, 30)}',
            "supervisingBranchCountry": "DE",
            "securitiesFinancingTransactionIndicator": self.faker.boolean(),
            "sentToRegulator": self.faker.boolean(),
            "responseFromRegulator": self.faker.boolean(),
            "investmentDecisionWithinFirmCodeType": "",
            "investmentDecisionWithinFirmCode": f'iFMCode{random.randint(1, 30)}',
            "investmentDecisionWithinFirmCodeVersion": "",
            "fileNameId": "PRO_635400BDQCDHUOGTBB59_20220216T145725001_MIXXXXXXT.XML",
            "nonMiFidFlag": self.faker.boolean(),
            "warnings": "W3009",
            "warningDescriptions": "Reported - non-MiFID eligible",
            "traxWarningAdvice": "",
            "submissionSource": "Host",
            "errros": [lcErrors],
            "userDefinedFields": [
                {
                "userDefined": f'UserFld{lAcctNum}',
                "userDefinedValue": "value1"
                },
                {
                "userDefined": f'DeptFld{lAcctNum}',
                "userDefinedValue": "value2"
                }
            ],
            "orderIdentifier": f'ORDERID_{lAcctNum}_{random.randint(1000, 10000000)}',
            "processingEntity": "Trax NL",
            "reports": {
                "RegRep": "RefererenceRepID",
                "RegRep2": "RefererenceRepID",
                "regResp": [lcRegResponse]
            },
            "events": [
                {
                "event_timestamp": 1646217628062,
                "status": "HLD",
                "subStatus": "Previous transaction queued for NCA transmission",
                "narrative": "Transaction Report received"
                },
                {
                "event_timestamp": 1646217628064,
                "status": "RAC",
                "subStatus": "",
                "narrative": "NCA Feedback"
                }
            ]
        }
        return document

    def run_aggregation_pipeline(self):
        """
        Run an aggregation pipeline on a secondary node
        """
        # count number of inhabitants per city
        group_by = {
            '$group': {
                '_id': '$submissionAccountId',
                'total_submissionAccountId': {'$sum': 1}
            }
        }


        # sort by the number of inhabitants desc
        order_by = {'$sort': {'total_submissionAccountId': pymongo.ASCENDING}}

        pipeline = [group_by, order_by]
        #Override for different usecase
        pipeline = [{ '$match': { 'submissionAccountId': random.randint(1, 6), 
                    'payloadTs': {'$gt': datetime.now() - timedelta(days=ldTradeDT + random.randint(1, 2)), '$lt': datetime.now() - timedelta(days=ldTradeDT + random.randint(2, 3)) } } }]

        # make sure we fetch everything by explicitly casting to list
        # use self.collection instead of self.collection_secondary to run the pipeline on the primary
        return list(self.collection_secondary.aggregate(pipeline))

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        # prepare the collection
        index1 = pymongo.IndexModel([('submissionAccountId', pymongo.ASCENDING), ("payloadTs", pymongo.ASCENDING)],
                                    name="idx_submissionAccountId")
        self.collection, self.collection_secondary = self.ensure_collection(COLLECTION_NAME, [index1])
        self.name_cache = []

    def insert_single_document(self):
        document = self.generate_new_document()

        self.collection.insert_one(document)

    def find_document(self):
        # at least one insert needs to happen
        if not self.name_cache:
            return

        # find a random document using an index
        self.collection.find_one({'submissionAccountId': random.randint(1, 6)})

    @task(weight=3)
    def do_find_document(self):
        self._process('find-document', self.find_document)

    @task(weight=1)
    def do_insert_document(self):
        self._process('insert-document', self.insert_single_document)

    @task(weight=1)
    def do_insert_document_bulk(self):
         self._process('insert-document-bulk', lambda: self.collection.insert_many(
             [self.generate_new_document() for _ in
              range(DOCS_PER_BATCH)], ordered=False))

    @task(weight=1)
    def do_run_aggregation_pipeline(self):
        self._process('run-aggregation-pipeline', self.run_aggregation_pipeline)
