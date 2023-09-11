@App:name("SettlementWorker1")
@App:qlVersion("2")

-- define input stream Settlements, with expected message format
CREATE SOURCE Settlements WITH (type='stream', stream.list='Settlements', replication.type='global', subscription.name='sub1',  map.type='json', transaction.uid.field='_txnID')
(source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, _txnID long);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (uuid string, name string,  balance long, reserved long, currency string, region string);

CREATE SINK Transfers WITH (type='stream', stream='Transfers', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, _txnID long);

CREATE STORE Settlement WITH (type = 'database', replication.type="global", collection.type="doc") 
(settlement_id long, source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, status string, _txnID long);

-- the main flow 3: check if Bank A belongs to the current region
INSERT INTO ValidatedSettlement
SELECT convert(math:rand() * 1000000, 'long') as settlement_id, source_bank, target_bank, source_region, amount, currency, timestamp, 'active' as status, _txnID
FROM Settlements [
    target_region == context:getVar('region')
    -- here can be added other conditions
];

@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Settlement
SELECT settlement_id, source_bank, target_bank, source_region, amount, currency, timestamp, status, _txnID
FROM ValidatedSettlement;

-- the main flow 5: send payment request to Bank B
@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Transfers
SELECT source_bank, target_bank, amount, currency, timestamp, _txnID
FROM ValidatedSettlement;
