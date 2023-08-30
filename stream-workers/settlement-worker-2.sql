@App:name("SettlementWorker2")
@App:qlVersion("2")

-- define input stream PayeeBankConfirmations, with expected message format from Bank B
CREATE SOURCE PayeeBankConfirmations WITH (type='stream', stream.list = 'PayeeBankConfirmations', map.type='json', transaction.uid.field='_txnID')
(source_bank string, target_bank string, amount double, currency string, status string, _txnID long);

-- define Settlement collection in database, where we will store the settled requests
CREATE STORE Settlement WITH (type = 'database', replication.type="global", collection.type="doc") 
(settlement_id long, source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, status string, _txnID long);

-- define Confirmations stream, which will be used for sending accepted payments back to Bank A
CREATE SINK Confirmations WITH (type='stream', stream='Confirmations', replication.type='local', map.type='json')
(settlement_id long, source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, status string, _txnID long);

-- define ConfirmationsGlobal stream, which will be used for replicating confirmations
CREATE SINK ConfirmationsGlobal WITH (type='stream', stream='ConfirmationsGlobal', replication.type='global', map.type='json')
(settlement_id long, source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, status string, _txnID long);

-- the main flow 1: Add settlement info to Payee message
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='read')
INSERT INTO PayeeWithSettlement
SELECT s.settlement_id, s.source_bank, s.target_bank, s.source_region, s.amount, s.currency, s.timestamp, ifThenElse(c.status == 'ACCP', 'settled', 'failed') as status, c._txnID
FROM PayeeBankConfirmations as c JOIN Settlement as s
ON c._txnID == s._txnID and s.status == 'active';

-- the main flow 2: update status of payment in Settlement collection
@Transaction(name='TxnSuccess', uid.field='_txnID', mode='write')
UPDATE Settlement
SET Settlement.status = status
ON Settlement._txnID == _txnID
SELECT status, _txnID
FROM PayeeWithSettlement;

-- the main flow 3: Send accepted payment to Confirmations stream 
@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Confirmations
SELECT *
FROM PayeeWithSettlement;

-- replicating to remote regions
INSERT INTO ConfirmationsGlobal
SELECT *
FROM Confirmations;