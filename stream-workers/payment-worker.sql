@App:name("PaymentWorker")
@App:description("This app validates a payment")
@App:qlVersion('2')
@App:instances("8")

-- DEFINITIONS --

CREATE TRIGGER StartTrigger WITH (expression='start');

-- define input stream Payments, with expected message format, generate transaction ID and put it into _txnID field
CREATE SOURCE Payments WITH (type = 'stream', stream.list = "Payments", map.type='json', subscription.name='sub1', transaction.uid.field='_txnID')
(source_bank string, target_bank string, amount double, currency string, _txnID long, timestamp long);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

-- create bank cache
CREATE STORE BanksCache WITH (type='inMemory') (_key string, region string);

-- define PaymentRequests collection in database, where we will store the payment requests
CREATE STORE PaymentRequests WITH (type = 'database', replication.type="global", collection.type="doc")
(source_bank string, target_bank string, amount double, currency string, timestamp long, _txnID long);

-- define PayerBankConfirmations stream, on which we confirm that payment was accepted
CREATE SINK PayerBankConfirmations WITH (type='stream', stream='PayerBankConfirmations', replication.type='local', map.type='json')
(_txnID long, source_bank string, target_bank string, amount double, currency string, status string, timestamp long, message string);

-- define Settlements stream, which will be used in Leg-2
CREATE SINK Settlements WITH (type='stream', stream='Settlements', replication.type='global', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, source_region string, target_region string, _txnID long);

-- QUERIES --

-- load to cache
INSERT INTO BanksCache
SELECT b._key, b.region
FROM StartTrigger as s JOIN Banks as b;

-- get payment request if exists by _txnID 
INSERT INTO PaymentWithDuplicate
SELECT p.source_bank, p.target_bank, p.amount, p.currency, p.timestamp, p._txnID, r._txnID as duplicate
FROM Payments as p LEFT OUTER JOIN PaymentRequests as r
ON r._txnID == p._txnID;

-- stop processing message if it has duplicate
INSERT INTO DistinctPayment
SELECT source_bank, target_bank, amount, currency, timestamp, _txnID
FROM PaymentWithDuplicate [
    duplicate is null
];

-- get region of Bank B, here we do not need transaction because region is fixed value.
INSERT INTO PaymentWithTargetBank
SELECT p.source_bank, p.target_bank, p.amount, p.currency, p.timestamp, b.region as target_region, p._txnID
FROM DistinctPayment as p LEFT OUTER JOIN BanksCache as b
ON b._key == p.target_bank;

INSERT INTO PaymentWithDevicePresentce
SELECT source_bank, target_bank, amount, currency, timestamp, target_region, _txnID, 
       clientPresence:consumerExists("c8locals.Transfers", "%", target_region, "production-demo_macrometa.team-_system-sub1") as isDevicePresent
FROM PaymentWithTargetBank;


INSERT INTO DeviceIsPresent
SELECT source_bank, target_bank, amount, currency, timestamp, target_region, _txnID
FROM PaymentWithDevicePresentce [
    isDevicePresent
];

-- device is not present 
INSERT INTO PaymentFailed
SELECT _txnID, source_bank, target_bank, amount, currency, timestamp, "Target bank is not present" as message
FROM PaymentWithDevicePresentce [
    not(isDevicePresent)
];

-- start transaction with name 'TxnSuccess', so we do not make a reserve if balance is not enough
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='read')
-- the main flow 1: get Bank A balance from Banks account, and push it on the internal stream PaymentWithBank with a current timestamp
INSERT INTO PaymentWithSourceBank
SELECT p.source_bank, p.target_bank, p.amount, p.currency, p.timestamp, b.balance as source_balance, b.reserved as source_total_reserved, b.region as source_region, p.target_region, p._txnID 
FROM DeviceIsPresent as p LEFT OUTER JOIN Banks as b
ON b._key == p.source_bank;

-- pre validate transaction
INSERT INTO PaymentWithSourceBankValidated
SELECT source_bank, target_bank, amount, currency, timestamp, source_balance, source_total_reserved, source_region, target_region, _txnID, 
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
SELECT source_bank, target_bank, amount, currency, timestamp, source_region, target_region, _txnID
FROM PaymentWithSourceBankValidated [
    failedMsg is null
];

INSERT INTO PaymentFailed
SELECT _txnID, source_bank, target_bank, amount, currency, timestamp, failedMsg as message
FROM PaymentWithSourceBankValidated [
    not(failedMsg is null)
];

-- the main flow 3: reserve funds from Banks account, within the same DB transaction
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='write')
UPDATE Banks
SET Banks.reserved = Banks.reserved + amount
ON Banks._key == source_bank
SELECT amount, source_bank, _txnID
FROM ValidatedPayments;

-- the main flow 4: saves payment request to PaymentRequests collection
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='write')
INSERT INTO PaymentRequests
SELECT source_bank, target_bank, amount, currency, timestamp, _txnID
FROM ValidatedPayments;

-- the main flow 6: Send message to the next step (Leg-2). Transaction with name 'TxnSuccess' ends here 
@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Settlements
SELECT source_bank, target_bank, amount, currency, timestamp, source_region, target_region, _txnID
FROM ValidatedPayments;

-- the error flow: validation failed, send failed message to the Bank A
@Transaction(name='TxnFailed', uid.field='_txnID')
INSERT INTO PayerBankConfirmations
SELECT _txnID, source_bank, target_bank, amount, currency, "RJCT" as status, timestamp, message
FROM PaymentFailed;