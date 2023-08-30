@App:name("ConfirmationWorker")
@App:qlVersion("2")

-- DEFINITIONS --

-- define input stream ConfirmationsGlobal for receiving confirmations from remote regions
CREATE SOURCE ConfirmationsGlobal WITH (type='stream', stream.list='ConfirmationsGlobal', replication.type='global', map.type='json', transaction.uid.field='_txnID')
(settlement_id long, source_bank string, target_bank string, amount double, currency string, timestamp long, source_region string, status string, _txnID long);

-- define input stream Confirmations, with expected message format from Bank B
CREATE SINK Confirmations WITH (type='stream', stream='Confirmations', replication.type='local', map.type='json', transaction.uid.field='_txnID')
(settlement_id long, source_bank string, target_bank string, amount double, currency string, timestamp long, source_region string, status string, _txnID long);

-- define internal stream ValidatedConfirmation, on which we will post validated messages
CREATE STREAM ValidatedConfirmation (settlement_id long, source_bank string, target_bank string, amount double, currency string, timestamp long, status string, _txnID long);

-- define internal stream ValidatedConfirmation, on which we will post validated messages
CREATE STREAM AcceptedConfirmation (settlement_id long, source_bank string, target_bank string, amount double, currency string, timestamp long, status string, _txnID long);

-- define internal stream ValidatedConfirmation, on which we will post validated messages
CREATE STREAM RejectedConfirmation (settlement_id long, source_bank string, target_bank string, amount double, currency string, timestamp long, status string, _txnID long);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (uuid string, name string,  balance long, reserved long, currency string, region string);

-- define PaymentRequests collection in database, where we will store the accepted payments
CREATE STORE Ledger WITH (type = 'database', replication.type="global", collection.type="doc") 
(settlement_id long, source_bank string, target_bank string, amount double, currency string, timestamp long, status string, _txnID long);

-- define PayerBankConfirmations stream, on which we confirm that payment was accepted bu Bank B
CREATE SINK PayerBankConfirmations WITH (type='stream', stream='PayerBankConfirmations', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, status string, _txnID long);

-- QUERIES --

-- replicating from remote regions
INSERT INTO Confirmations
SELECT *
FROM ConfirmationsGlobal;

-- the main flow 1: validate on correct region
INSERT INTO ValidatedConfirmation
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM Confirmations [
    source_region == context:getVar('region')
];

-- the main flow 2: validate on status == 'settled'
INSERT INTO AcceptedConfirmation
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM ValidatedConfirmation [
    status == 'settled'
];

-- the main flow 2:  validate on status != 'settled'
INSERT INTO RejectedConfirmation
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM ValidatedConfirmation [
    status != 'settled'
];

-- the main flow 1: Release and deduct funds the Banks account
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='write')
UPDATE Banks
SET Banks.reserved = Banks.reserved - reserved, Banks.balance = Banks.balance - reserved
ON Banks.name == name
SELECT amount AS reserved, source_bank as name, _txnID
FROM AcceptedConfirmation;

-- the main flow 1: Add funds to the Bank B account
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='write')
UPDATE Banks
SET Banks.balance = Banks.balance + reserved
ON Banks.name == name
SELECT amount AS reserved, target_bank as name, _txnID
FROM AcceptedConfirmation;

-- the failed flow 1: Release funds the Banks account
@Transaction(name='TxnRejected', uid.field='_txnID', mode='write')
UPDATE Banks
SET Banks.reserved = Banks.reserved - reserved
ON Banks.name == name
SELECT amount AS reserved, source_bank as name, _txnID
FROM RejectedConfirmation;

-- the accepted flow 2: Save transaction yo Ledger collection
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='write')
INSERT INTO Ledger
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM AcceptedConfirmation;

-- the rejected flow 2: Save transaction yo Ledger collection
@Transaction(name='TxnRejected', uid.field='_txnID', mode='write')
INSERT INTO Ledger
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM RejectedConfirmation;

-- the main flow 2: Publish Settlement request to stream
@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO PayerBankConfirmations
SELECT source_bank, target_bank, amount, currency, timestamp, 'ACCP' as status, _txnID
FROM AcceptedConfirmation;

-- the rejected flow 2: Publish Settlement request to stream
@Transaction(name='TxnRejected', uid.field='_txnID')
INSERT INTO PayerBankConfirmations
SELECT source_bank, target_bank, amount, currency, timestamp, 'RJCT' as status, _txnID
FROM RejectedConfirmation;