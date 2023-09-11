@App:name("PaymentWorker")
@App:description("This app validates a payment")
@App:qlVersion('2')

-- DEFINITIONS --

-- define input stream Payments, with expected message format, generate transaction ID and put it into _txnID field
CREATE SOURCE Payments WITH (type = 'stream', stream.list = "Payments", map.type='json', transaction.uid.field='_txnID')
(source_bank string, target_bank string, amount double, currency string, _txnID long, timestamp long);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

-- define PaymentRequests collection in database, where we will store the payment requests
CREATE STORE PaymentRequests WITH (type = 'database', replication.type="global", collection.type="doc")
(source_bank string, target_bank string, amount double, currency string, timestamp long, _txnID long);

-- define PayerBankConfirmations stream, on which we confirm that payment was accepted
CREATE SINK PayerBankConfirmations WITH (type='stream', stream='PayerBankConfirmations', replication.type='local', map.type='json')
(_txnID long, source_bank string, target_bank string, amount double, currency string, status string, timestamp long, message string);

-- define Settlements stream, which will be used in Leg-2
CREATE SINK Settlements WITH (type='stream', stream='Settlements', replication.type='global', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, source_region string, target_region string, _txnID long);

-- User Defined Functions in JavaScript that returns prpper pessage for failed response
CREATE FUNCTION validate[javascript] return string {
    var source_bank = data[0];
    var target_bank = data[1];
    var source_balance = data[2];
    var amount = data[3];
    var source_reserved = data[4];
    var source_region = data[5];
    var current_region = data[6];
    var currency = data[7];
    if (!source_region && !source_reserved && !source_balance) {
        return null;
    }
    if (!source_bank) {
        return "Source bank parameter is not set";
    }
    if (source_region != current_region) {
        return "Source bank " + source_bank + " belongs to another region";
    }
    if (!target_bank) {
        return "Target bank parameter is not set";
    }
    if (!source_region) {
        return "Source bank with name " + source_bank + " parameter is not set";
    }
    if (amount === undefined || amount === null) {
        return "Amount parameter is not set";
    }
    if (amount <= 0 || amount > 100000) {
        return "Amount of money should be more than 0 and less or equall to 10000";
    }
    if (!currency || currency != "USD") {
        return "Only USD currency is acceptable";
    }
    if (Number(source_balance) - Number(source_reserved) < Number(amount)) {
        return "No sufficient funds on source bank"
    }
    return null;
};

-- QUERIES --

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
FROM DistinctPayment as p LEFT OUTER JOIN Banks as b
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
SELECT p.source_bank, p.target_bank, p.amount, p.currency, p.timestamp, b.balance as source_balance, b.reserved as source_total_reserved, b.region as source_region, p.target_region, p._txnID, 
    validate(p.source_bank, p.target_bank, b.balance, p.amount, b.reserved, b.region, context:getVar('region'), p.currency) as failedMsg
FROM DeviceIsPresent as p LEFT OUTER JOIN Banks as b
ON b._key == p.source_bank;

-- the main flow 2: check if Bank A belongs to the current region and it has enough balance for the requested amount, having in mind the total reserved money within the last 15 seconds 
INSERT INTO ValidatedPayments
SELECT source_bank, target_bank, amount, currency, timestamp, source_region, target_region, _txnID
FROM PaymentWithSourceBank [
    failedMsg is null
];

INSERT INTO PaymentFailed
SELECT _txnID, source_bank, target_bank, amount, currency, timestamp, failedMsg as message
FROM PaymentWithSourceBank [
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