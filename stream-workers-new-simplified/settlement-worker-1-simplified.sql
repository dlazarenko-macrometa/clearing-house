@App:name("SW1New")
@App:qlVersion("2")
@App:instances("2")

-- define input stream Settlements, with expected message format
CREATE SOURCE Settlements WITH (type='stream', stream.list='Settlements', replication.type='global', subscription.name='sub1',  map.type='json', subscription.initial.position='Latest')
(source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, txnID string);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string,  balance long, reserved long, currency string, region string);

CREATE SINK Transfers WITH (type='stream', stream='Transfers', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, txnID string);

CREATE STORE Settlement WITH (type = 'database', replication.type="global", collection.type="doc") 
(_key string, source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, status string, txnID string);

-- 
INSERT INTO SettlementWithBank
SELECT source_bank, target_bank, source_region, ifThenElse(str:contains(target_bank, "Chase"), "cleaninghouse-us-west-1" , "cleaninghouse-us-east-1" ) as target_region, amount, currency, timestamp, txnID
FROM Settlements;

INSERT INTO ValidatedSettlement
SELECT source_bank, target_bank, source_region, amount, currency, timestamp, 'active' as status, txnID
FROM SettlementWithBank[
    target_region == context:getVar('region')
];

--@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Settlement
SELECT txnID as _key, source_bank, target_bank, source_region, amount, currency, timestamp, status, txnID
FROM ValidatedSettlement;

-- the main flow 5: send payment request to Bank B
--@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Transfers
SELECT source_bank, target_bank, amount, currency, timestamp, txnID
FROM ValidatedSettlement;
