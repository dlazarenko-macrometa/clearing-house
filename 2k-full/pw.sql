@App:name("PW")
@App:description("This app validates a payment")
@App:qlVersion('2')

-- DEFINITIONS --

-- trigger to load all banks in cache
CREATE TRIGGER StartTrigger WITH (expression='start');

-- cached banks store
CREATE STORE BanksCache WITH (type='inMemory') (_key string, region string);

CREATE SOURCE Payments WITH (type = 'stream', stream.list = "Payments", map.type='json', subscription.name='sub1')
(source_bank string, target_bank string, amount double, currency string, timestamp long, txnID string);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE STORE Banks2 WITH (type='database', collection="Banks", replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE SINK PaymentQueryWorkerRequest WITH (type='query-worker', query="FOR doc IN Banks FILTER (doc._key == @source_bank )  UPDATE doc WITH { reserved: doc.reserved +@amount } IN Banks RETURN {txnID:@txnID , source_bank: @source_bank, target_bank: @target_bank, source_region: @source_region, target_region: @target_region, amount: @amount, currency: @currency, timestamp: @timestamp}", sink.id="test") (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

CREATE SOURCE PaymentQueryWorkerResponse WITH (type='query-worker', sink.id="test", map.type="json") (txnID string, source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long);

CREATE SINK PaymentQueryWorkerRequest2 WITH (type='query-worker', query="INSERT  {'_txnID': 1234567, 'amount': 100.0, 'currency': 'USD', 'source_bank': @source_bank, 'target_bank': @target_bank, 'timestamp': 1694114365199} INTO PaymentRequests RETURN {source_bank: @source_bank, target_bank: @target_bank, source_region: @source_region, target_region: @target_region, amount: @amount, currency: @currency, timestamp: @timestamp, txnID: @txnID}", sink.id="test2") (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

CREATE SOURCE PaymentQueryWorkerResponse2 WITH (type='query-worker', sink.id="test2", map.type="json") (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

CREATE SINK Settlements WITH (type='stream', stream='Settlements', replication.type='global', map.type='json')
(source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

-- Payment Worker

INSERT INTO BanksCache
SELECT b._key, b.region
FROM StartTrigger as s JOIN Banks2 as b;

-- get cahced duplicate
INSERT INTO PaymentGetCachedDuplicate
SELECT source_bank, target_bank, amount, currency, timestamp, txnID, memcache:get(txnID) as duplicate
FROM Payments;

-- payment doesn't have duplicates
INSERT INTO DistinctPayment
SELECT source_bank, target_bank, amount, timestamp, currency, txnID
FROM PaymentGetCachedDuplicate [
    duplicate IS NULL
];

-- save payment to memory
INSERT INTO DistinctPaymentSaved
SELECT source_bank, target_bank, amount, currency, timestamp, txnID, memcache:put(txnID, "+", 5000L) as saved
FROM DistinctPayment;

-- get target region from cache
INSERT INTO PaymentWithTargetRegion
SELECT p.source_bank, p.target_bank, p.amount, p.currency, p.timestamp, b.region as target_region, p.txnID
FROM DistinctPaymentSaved as p LEFT OUTER JOIN BanksCache as b
ON b._key == p.target_bank;

INSERT INTO PaymentWithDevicePresence
SELECT source_bank, target_bank, amount, currency, timestamp, target_region, txnID, 
       /*clientPresence:consumerExists("c8locals.Transfers", "%", target_region, "%-sub1")*/ true as isDevicePresent
FROM PaymentWithTargetRegion;

-- device is present
INSERT INTO DeviceIsPresent
SELECT source_bank, target_bank, amount, currency, timestamp, target_region, txnID
FROM PaymentWithDevicePresence [
    isDevicePresent
];

-- device is not present 
INSERT INTO PaymentFailed
SELECT txnID, source_bank, target_bank, amount, currency, timestamp, "Target bank is not present" as message
FROM PaymentWithDevicePresence [
    not(isDevicePresent)
];

-- retrieve current balance and reserved funds
@Transaction(group='TxnSuccess', uid.field='txnID', mode='Banks:read')
INSERT INTO PaymentWithSourceBank
SELECT p.source_bank, p.target_bank, p.amount, p.currency, p.timestamp, b.region as source_region, p.target_region, p.txnID, b.balance as source_balance, b.reserved as source_total_reserved 
FROM DeviceIsPresent as p LEFT OUTER JOIN Banks as b
ON b._key == source_bank;

-- pre validate transaction
INSERT INTO PaymentWithSourceBankValidated
SELECT source_bank, target_bank, amount, currency, timestamp, source_balance, source_total_reserved, source_region, target_region, txnID, 
    ifThenElse(source_region IS NULL AND source_total_reserved IS NULL AND source_balance IS NULL, getNull('string'), 
        ifThenElse(source_bank IS NULL, "Source bank parameter is not set", 
            ifThenElse(source_region != context:getVar('region'), str:concat("Source bank ", source_bank, " belongs to another region"), 
                ifThenElse(target_bank IS NULL, "Target bank parameter is not set", 
                    ifThenElse(source_region IS NULL, str:concat("Source bank with name ", source_bank, " parameter is not set"), 
                        ifThenElse(amount IS NULL, "Amount parameter is not set", 
                            ifThenElse(amount <= 0 OR amount > 100000, "Amount of money should be more than 0 and less or equall to 100000", 
                                ifThenElse(currency IS NULL OR currency != "USD", "Only USD currency is acceptable", 
                                    ifThenElse(source_balance - source_total_reserved < amount, "No sufficient funds on source bank",
                                        getNull('string')
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    ) as failedMsg
FROM PaymentWithSourceBank;

-- the main flow 2: check if Bank A belongs to the current region and it has enough balance for the requested amount, having in mind the total reserved money within the last 15 seconds 
INSERT INTO ValidatedPayments
SELECT source_bank, target_bank, amount, currency, timestamp, source_region, target_region, txnID
FROM PaymentWithSourceBankValidated [
    failedMsg is null
];

INSERT INTO PaymentFailed
SELECT txnID, source_bank, target_bank, amount, currency, timestamp, failedMsg as message
FROM PaymentWithSourceBankValidated [
    not(failedMsg is null)
];

--
@Transaction(group='TxnSuccess', uid.field='txnID', mode='Banks:write')
INSERT INTO PaymentQueryWorkerRequest
SELECT source_bank, target_bank, source_region, target_region, amount, currency, timestamp, txnID 
FROM ValidatedPayments;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='PaymentRequests:write')
INSERT INTO PaymentQueryWorkerRequest2
SELECT source_bank, target_bank, source_region, target_region, amount, currency, timestamp, txnID
FROM PaymentQueryWorkerResponse;

INSERT INTO Settlements
SELECT source_bank, target_bank, source_region, target_region, 1.0 as amount, currency, timestamp, txnID
FROM PaymentQueryWorkerResponse2;

INSERT INTO PayerBankConfirmations
SELECT txnID, source_bank, target_bank, amount, currency, "RJCT" as status, timestamp, message
FROM PaymentFailed;

