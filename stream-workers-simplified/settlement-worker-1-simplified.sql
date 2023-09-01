@App:name("SettlementWorker1Simplified")
@App:qlVersion("2")
@App:instances("8")

-- define input stream Settlements, with expected message format
CREATE SOURCE Settlements WITH (type='stream', stream.list='Settlements', replication.type='local', subscription.name='sub1',  map.type='json', transaction.uid.field='_txnID', subscription.initial.position='Latest')
(source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, _txnID long);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (uuid string, name string,  balance long, reserved long, currency string, region string);

CREATE SINK Transfers WITH (type='stream', stream='Transfers', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, _txnID long);

CREATE STORE Settlement WITH (type = 'database', replication.type="global", collection.type="doc") 
(settlement_id long, source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, status string, _txnID long);

-- 
INSERT INTO SettlementWithBank
SELECT s.source_bank, s.target_bank, s.source_region, b.region as target_region, s.amount, s.currency, s.timestamp, s._txnID
FROM Settlements as s LEFT OUTER JOIN Banks as b
ON s.target_bank == b.name;

INSERT INTO ValidatedSettlement
SELECT convert(math:rand() * 1000000, 'long') as settlement_id, source_bank, target_bank, source_region, amount, currency, timestamp, 'active' as status, _txnID
FROM SettlementWithBank;

@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Settlement
SELECT settlement_id, source_bank, target_bank, source_region, amount, currency, timestamp, status, _txnID
FROM ValidatedSettlement;

-- the main flow 5: send payment request to Bank B
@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Transfers
SELECT source_bank, target_bank, amount, currency, timestamp, _txnID
FROM ValidatedSettlement;
