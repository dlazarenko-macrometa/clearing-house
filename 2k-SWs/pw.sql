@App:name("Perf2PW")
@App:description("This app validates a payment")
@App:qlVersion('2')
@App:instances('25')

-- DEFINITIONS --

--CREATE TRIGGER MyTrigger WITH ( interval = 10 millisec );

CREATE SOURCE Payments WITH (type = 'stream', stream.list = "Payments", map.type='json', subscription.name='sub1')
(source_bank string, target_bank string, amount double, currency string, txnID string);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE STORE Banks2 WITH (type='database', collection='Banks', replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE SINK PaymentQueryWorkerRequest WITH (type='query-worker', query="FOR doc IN Banks FILTER (doc._key == @source_bank )  UPDATE doc WITH { reserved: doc.reserved +@amount } IN Banks RETURN {txnID:@txnID , source_bank: @source_bank, target_bank: @target_bank, source_region: @source_region, amount: @amount, currency: @currency}", sink.id="test") (source_bank string, target_bank string, source_region string, amount double, currency string, txnID string);

CREATE SOURCE PaymentQueryWorkerResponse WITH (type='query-worker', sink.id="test", map.type="json") (txnID string, source_bank string, target_bank string, source_region string, amount double, currency string);

CREATE SINK PaymentQueryWorkerRequest2 WITH (type='query-worker', query="INSERT  {'_txnID': 1234567, 'amount': 100.0, 'currency': 'USD', 'source_bank': @source_bank, 'target_bank': @target_bank, 'timestamp': 1694114365199} INTO PaymentRequests RETURN {source_bank: @source_bank, target_bank: @target_bank, source_region: @source_region, txnID: @txnID}", sink.id="test2") (source_bank string, target_bank string, source_region string, txnID string);

CREATE SOURCE PaymentQueryWorkerResponse2 WITH (type='query-worker', sink.id="test2", map.type="json") (source_bank string, target_bank string, source_region string, txnID string);

CREATE SINK Settlements WITH (type='stream', stream='Settlements', replication.type='global', map.type='json')
(source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

-- Payment Worker

@Transaction(group='TxnSuccess', uid.field='txnID', mode='Banks:read')
INSERT INTO PaymentWithRegion
SELECT p.source_bank, p.target_bank, p.amount, p.currency, p.txnID, b.region as source_region
FROM Payments as p LEFT OUTER JOIN Banks as b
ON b._key == source_bank;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='Banks:write')
INSERT INTO PaymentQueryWorkerRequest
SELECT source_bank, target_bank, source_region, amount, currency, txnID 
FROM PaymentWithRegion;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='PaymentRequests:write')
INSERT INTO PaymentQueryWorkerRequest2
SELECT source_bank, target_bank, source_region, txnID
FROM PaymentQueryWorkerResponse;

INSERT INTO SettlementWithRegion
SELECT p.source_bank, p.target_bank, p.source_region, b.region as target_region, p.txnID
FROM PaymentQueryWorkerResponse2 as p LEFT OUTER JOIN Banks2 as b
ON b._key == p.target_bank;

INSERT INTO Settlements
SELECT source_bank, target_bank, source_region, target_region, 1.0 as amount, "USD" as currency, 123456778L as timestamp, txnID
FROM SettlementWithRegion;

