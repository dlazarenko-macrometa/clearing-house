@App:name("Perf2SW")
@App:description("This app validates a payment")
@App:qlVersion('2')
@App:instances('25')

-- DEFINITIONS --

CREATE SOURCE Settlements WITH (type='stream', stream.list='Settlements', replication.type='global', subscription.name='sub1',  map.type='json', subscription.initial.position='Latest')
(source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

CREATE SINK SettlementQueryWorkerRequest WITH (type='query-worker', query="INSERT  {'_txnID': 1234567, 'amount': 100.0, 'settlement_id': 12234345343, 'source_region': 'chouse-us-west', 'currency': 'USD', '_key': @txnID, 'source_bank': @source_bank, 'target_bank': @target_bank, 'timestamp': 1694114365199, 'status': 'active'} INTO Settlement RETURN {source_bank: @source_bank, target_bank: @target_bank, source_region: @source_region, target_region: @target_region, amount: @amount, currency: @currency, timestamp: @timestamp, txnID: @txnID}", sink.id="test3") (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

CREATE SOURCE SettlementQueryWorkerResponse WITH (type='query-worker', sink.id="test3", map.type="json") (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

CREATE STORE PaymentRequests WITH (type = 'database', replication.type="global", collection.type="doc")
(_key string, source_bank string, target_bank string, amount double, currency string, timestamp long);

CREATE STORE Settlement WITH (type = 'database', replication.type="global", collection.type="doc")
(_key string, source_bank string, target_bank string, amount double, currency string, settlement_id long, source_region string, _txnID string, timestamp long, status string);

CREATE SINK SettlementQueryWorkerRequest2 WITH (type='query-worker', query="FOR doc IN Settlement FILTER (doc._key == @txnID ) UPDATE doc WITH { status: 'settled' } IN Settlement RETURN { source_bank: @source_bank, target_bank: @target_bank, source_region: @source_region, target_region: @target_region, amount: @amount, currency: @currency, timestamp: @timestamp, txnID: @txnID}", sink.id="test4") (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

CREATE SOURCE SettlementQueryWorkerResponse2 WITH (type='query-worker', sink.id="test4", map.type="json") (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

CREATE STORE Banks2 WITH (type='database', collection="Banks", replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE SINK Confirmations WITH (type='stream', stream='Confirmations', replication.type='global', map.type='json')
(source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, status string, txnID string);

-- Settlement Worker 1 & 2 ---

--@Transaction(group='TxnSuccess2', uid.field='txnID', mode='write')
INSERT INTO SettlementQueryWorkerRequest
SELECT source_bank, target_bank, source_region, target_region, amount, currency, timestamp, txnID 
FROM Settlements[
    target_region == context:getVar('region')
];

INSERT INTO SettlementWithStatus
SELECT p.source_bank, p.target_bank, p.source_region, p.target_region, p.amount, p.currency, p.timestamp, p.txnID, b.status
FROM SettlementQueryWorkerResponse as p LEFT OUTER JOIN Settlement as b
ON b._key == p.txnID;

INSERT INTO SettlementQueryWorkerRequest2
SELECT source_bank, target_bank, source_region, target_region, amount, currency, timestamp, txnID 
FROM SettlementWithStatus;

INSERT INTO Confirmations
SELECT source_bank, target_bank, source_region, target_region, amount, currency, timestamp, 'settled' as status, str:concat('pw_', txnID) as txnID
FROM SettlementQueryWorkerResponse2;
