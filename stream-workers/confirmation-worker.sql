@App:name("ConfirmationWorker")
@App:qlVersion("2")
@App:instances("8")

-- DEFINITIONS --

CREATE TRIGGER StartTrigger WITH (expression='start');

-- define input stream Confirmations, with expected message format from Bank B
CREATE SOURCE Confirmations WITH (type='stream', stream.list='Confirmations', replication.type='global', map.type='json', subscription.name='sub1', transaction.uid.field='_txnID')
(settlement_id long, source_bank string, target_bank string, amount double, currency string, timestamp long, source_region string, status string, _txnID long);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string,  balance long, reserved long, currency string, region string);

-- create bank cache
CREATE STORE BanksCache WITH (type='inMemory') (_key string, region string);

-- define PaymentRequests collection in database, where we will store the accepted payments
CREATE STORE Ledger WITH (type = 'database', replication.type="global", collection.type="doc") 
(settlement_id long, source_bank string, target_bank string, amount double, currency string, timestamp long, status string, _txnID long);

-- define PayerBankConfirmations stream, on which we confirm that payment was accepted bu Bank B
CREATE SINK PayerBankConfirmations WITH (type='stream', stream='PayerBankConfirmations', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, status string, _txnID long);

-- QUERIES --

-- load to cache
INSERT INTO BanksCache
SELECT b._key, b.region
FROM StartTrigger as s JOIN Banks as b;

-- get region of Bank B, here we do not need transaction because region is fixed value.
INSERT INTO ConfirmationsWithSourceBank
SELECT c.settlement_id, c.source_bank, c.target_bank, c.amount, c.currency, c.timestamp, b.region as source_region, c.status, c._txnID
FROM Confirmations as c LEFT OUTER JOIN BanksCache as b
ON b._key == c.source_bank;

-- the main flow 1: validate on correct region
INSERT INTO ValidatedConfirmation
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM ConfirmationsWithSourceBank [
    source_region == context:getVar('region')
];

-- the accepted flow 2: validate on status == 'settled'
INSERT INTO AcceptedConfirmation
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM ValidatedConfirmation [
    status == 'settled'
];

-- the rejected flow 2:  validate on status != 'settled'
INSERT INTO RejectedConfirmation
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM ValidatedConfirmation [
    status != 'settled'
];

-- the accepted flow 1: Release and deduct funds the Banks account
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='write')
UPDATE Banks
SET Banks.reserved = Banks.reserved - reserved, Banks.balance = Banks.balance - reserved
ON Banks._key == source_bank
SELECT amount AS reserved, source_bank, _txnID
FROM AcceptedConfirmation;

-- the accepted flow 2: Add funds to the Bank B account
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='write')
UPDATE Banks
SET Banks.balance = Banks.balance + reserved
ON Banks._key == target_bank
SELECT amount AS reserved, target_bank, _txnID
FROM AcceptedConfirmation;

-- the accepted flow 3: Save transaction yo Ledger collection
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='write')
INSERT INTO Ledger
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM AcceptedConfirmation;

-- the accepted flow 4: Publish Settlement request to stream
@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO PayerBankConfirmations
SELECT source_bank, target_bank, amount, currency, timestamp, 'ACCP' as status, _txnID
FROM AcceptedConfirmation;

-- the rejected flow 1: Release funds the Banks account
@Transaction(name='TxnRejected', uid.field='_txnID', mode='write')
UPDATE Banks
SET Banks.reserved = Banks.reserved - reserved
ON Banks._key == source_bank
SELECT amount AS reserved, source_bank, _txnID
FROM RejectedConfirmation;

-- the rejected flow 2: Save transaction yo Ledger collection
@Transaction(name='TxnRejected', uid.field='_txnID', mode='write')
INSERT INTO Ledger
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM RejectedConfirmation;

-- the rejected flow 3: Publish Settlement request to stream
@Transaction(name='TxnRejected', uid.field='_txnID')
INSERT INTO PayerBankConfirmations
SELECT source_bank, target_bank, amount, currency, timestamp, 'RJCT' as status, _txnID
FROM RejectedConfirmation;