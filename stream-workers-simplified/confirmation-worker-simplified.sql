@App:name("ConfirmationWorkerSimplified")
@App:qlVersion("2")
@App:instances("8")

CREATE SOURCE Confirmations WITH (type='stream', stream.list='Confirmations', replication.type='local', subscription.name='sub1', map.type='json', transaction.uid.field='_txnID', subscription.initial.position='Latest')
(settlement_id long, source_bank string, target_bank string, amount double, currency string, timestamp long, source_region string, status string, _txnID long);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (uuid string, name string,  balance long, reserved long, currency string, region string);

-- define PaymentRequests collection in database, where we will store the accepted payments
CREATE STORE Ledger WITH (type = 'database', replication.type="global", collection.type="doc") 
(settlement_id long, source_bank string, target_bank string, amount double, currency string, timestamp long, status string, _txnID long);

-- define PayerBankConfirmations stream, on which we confirm that payment was accepted bu Bank B
CREATE SINK PayerBankConfirmations WITH (type='stream', stream='PayerBankConfirmations', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, status string, _txnID long);

-- QUERIES --

-- the failed flow 1: Release funds the Banks account
@Transaction(name='TxnRejected', uid.field='_txnID', mode='write')
UPDATE Banks
SET Banks.reserved = Banks.reserved - reserved
ON Banks.name == name
SELECT amount AS reserved, source_bank as name, _txnID
FROM Confirmations;

-- the rejected flow 2: Save transaction yo Ledger collection
@Transaction(name='TxnRejected', uid.field='_txnID', mode='write')
INSERT INTO Ledger
SELECT settlement_id, source_bank, target_bank, amount, currency, timestamp, status, _txnID
FROM Confirmations;

-- the rejected flow 2: Publish Settlement request to stream
@Transaction(name='TxnRejected', uid.field='_txnID')
INSERT INTO PayerBankConfirmations
SELECT source_bank, target_bank, amount, currency, timestamp, 'RJCT' as status, _txnID
FROM Confirmations;