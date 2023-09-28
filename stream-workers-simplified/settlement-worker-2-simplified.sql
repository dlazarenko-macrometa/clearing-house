@App:name("SettlementWorker2Simplified")
@App:qlVersion("2")
@App:instances("2")

-- define input stream PayeeBankConfirmations, with expected message format from Bank B
CREATE SOURCE PayeeBankConfirmations WITH (type='stream', stream.list = 'PayeeBankConfirmations', subscription.name='sub1', map.type='json', transaction.uid.field='_txnID', subscription.initial.position='Latest')
(source_bank string, target_bank string, amount double, currency string, status string, _txnID long);

-- define Settlement collection in database, where we will store the settled requests
CREATE STORE Settlement WITH (type = 'database', replication.type="global", collection.type="doc") 
(settlement_id long, source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, status string, _txnID long);

-- define Confirmations stream, which will be used for sending accepted payments back to Bank A
CREATE SINK Confirmations WITH (type='stream', stream='Confirmations', replication.type='global', map.type='json')
(settlement_id long, source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, status string, _txnID long);

-- the main flow 1: Add settlement info to Payee message
INSERT INTO PayeeWithSettlement
SELECT _txnID as settlement_id, source_bank, target_bank, amount, currency, eventTimestamp() as timestamp, ifThenElse(status == 'ACCP', 'settled', 'failed') as status, _txnID
FROM PayeeBankConfirmations;

-- the main flow 2: update status of payment in Settlement collection
--@Transaction(name='TxnSuccess', uid.field='_txnID', mode='write')
UPDATE Settlement
SET Settlement.status = status
ON Settlement._txnID == _txnID
SELECT status, _txnID
FROM PayeeWithSettlement;

-- the main flow 3: Send accepted payment to Confirmations stream 
--@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Confirmations
SELECT settlement_id, source_bank, target_bank, "cleaninghouse-us-west-1" as source_region, amount, currency, timestamp, status, _txnID
FROM PayeeWithSettlement;