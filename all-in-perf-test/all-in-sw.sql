@App:name("AllPerf")
@App:description("This app validates a payment")
@App:qlVersion('2')

-- DEFINITIONS --

--CREATE TRIGGER MyTrigger WITH ( interval = 10 millisec );

CREATE SOURCE Payments WITH (type = 'stream', stream.list = "Payments", map.type='json', subscription.name='sub1')
(source_bank string, target_bank string, amount double, currency string, txnID string);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE STORE Banks2 WITH (type='database', collection="Banks", replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE SINK PaymentQueryWorkerRequest WITH (type='query-worker', query="FOR doc IN Banks FILTER (doc._key == @source_bank )  UPDATE doc WITH { reserved: doc.reserved +@amount } IN Banks RETURN {txnID:@txnID , source_bank: @source_bank, target_bank: @target_bank, amount: @amount, currency: @currency}", sink.id="test") (source_bank string, target_bank string, amount double, currency string, txnID string);

CREATE SOURCE PaymentQueryWorkerResponse WITH (type='query-worker', sink.id="test", map.type="json") (txnID string, source_bank string, target_bank string, amount double, currency string);

CREATE SINK PaymentQueryWorkerRequest2 WITH (type='query-worker', query="INSERT  {'_txnID': 1234567, 'amount': 100.0, 'currency': 'USD', 'source_bank': @source_bank, 'target_bank': @target_bank, 'timestamp': 1694114365199} INTO PaymentRequests RETURN {source_bank: @source_bank, target_bank: @target_bank, txnID: @txnID}", sink.id="test2") (source_bank string, target_bank string, txnID string);

CREATE SOURCE PaymentQueryWorkerResponse2 WITH (type='query-worker', sink.id="test2", map.type="json") (source_bank string, target_bank string, txnID string);

CREATE SINK SettlementQueryWorkerRequest WITH (type='query-worker', query="INSERT  {'_txnID': 1234567, 'amount': 100.0, 'settlement_id': 12234345343, 'source_region': 'chouse-us-west', 'currency': 'USD', '_key': @txnID, 'source_bank': @source_bank, 'target_bank': @target_bank, 'timestamp': 1694114365199, 'status': 'active'} INTO Settlement RETURN {source_bank: @source_bank, target_bank: @target_bank, txnID: @txnID}", sink.id="test3") (source_bank string, target_bank string, txnID string);

CREATE SOURCE SettlementQueryWorkerResponse WITH (type='query-worker', sink.id="test3", map.type="json") (source_bank string, target_bank string, txnID string);

CREATE STORE PaymentRequests WITH (type = 'database', replication.type="global", collection.type="doc")
(_key string, source_bank string, target_bank string, amount double, currency string, timestamp long);

CREATE STORE Settlement WITH (type = 'database', replication.type="global", collection.type="doc")
(_key string, source_bank string, target_bank string, amount double, currency string, settlement_id long, source_region string, _txnID string, timestamp long, status string);

CREATE SINK SettlementQueryWorkerRequest2 WITH (type='query-worker', query="FOR doc IN Settlement FILTER (doc._key == @txnID ) UPDATE doc WITH { status: 'settled' } IN Settlement RETURN { source_bank: @source_bank, target_bank: @target_bank, txnID: @txnID}", sink.id="test4") (source_bank string, target_bank string, txnID string);

CREATE SOURCE SettlementQueryWorkerResponse2 WITH (type='query-worker', sink.id="test4", map.type="json") (source_bank string, target_bank string, txnID string);

CREATE SINK TargetBankQueryWorkerRequest WITH (type='query-worker', query="FOR doc IN Banks FILTER (doc._key == @target_bank ) UPDATE doc WITH { balance: doc.balance +@amount } IN Banks RETURN {txnID: @txnID , source_bank: @source_bank, target_bank: @target_bank, amount: @amount, currency: @currency}", sink.id="targetBank") 
(source_bank string, target_bank string, amount double, currency string, txnID string);

CREATE SOURCE TargetBankQueryWorkerResponse WITH (type='query-worker', sink.id="targetBank", map.type="json") 
(txnID string, source_bank string, target_bank string, amount double, currency string);

CREATE SINK SourceBankQueryWorkerRequest WITH (type='query-worker', query="FOR doc IN Banks FILTER (doc._key == @source_bank ) UPDATE doc WITH { balance: doc.balance -@amount, reserved: doc.reserved -@amount } IN Banks RETURN {txnID:@txnID , source_bank: @source_bank, target_bank: @target_bank, amount: @amount, currency: @currency}", sink.id="sourceBank") 
(source_bank string, target_bank string, amount double, currency string, txnID string);

CREATE SOURCE SourceBankQueryWorkerResponse WITH (type='query-worker', sink.id="sourceBank", map.type="json") 
(txnID string, source_bank string, target_bank string, amount double, currency string);

CREATE SINK LedgerQueryWorkerRequest WITH (type='query-worker', query="INSERT {'_txnID': 1234567, 'amount': 100.0, 'settlement_id': 4702603, 'currency': 'USD', 'source_bank': @source_bank, 'target_bank': @target_bank, 'timestamp': 1694114365199, 'status': 'settled'} INTO Ledger RETURN {source_bank: @source_bank, target_bank: @target_bank, txnID: @txnID}", sink.id="ledger") 
(source_bank string, target_bank string, txnID string);

CREATE SOURCE LedgerQueryWorkerResponse WITH (type='query-worker', sink.id="ledger", map.type="json") 
(source_bank string, target_bank string, txnID string);

CREATE SINK OutputPerf WITH (type='stream', stream='OutputPerf', replication.type='local', map.type='json')
(source_bank string, target_bank string, txnID string);


-- QUERIES --
-- generate Payments --
/*INSERT INTO Payments
SELECT str:concat("Chase_", convert(count() % 2000, 'string')) as source_bank, str:concat("Chase_", convert(count() % 2000 + 12000, 'string')) as target_bank, 
ifThenElse(math:rand() > 0.5, 1.0, -1.0) as amount, "USD" as currency, convert(currentTimeMillis() * 1000 + (count() % 1000), 'string') as txnID
FROM MyTrigger;*/

-- Payment Worker

@Transaction(group='TxnSuccess', uid.field='txnID', mode='Banks:read')
INSERT INTO PaymentWithRegion
SELECT p.source_bank, p.target_bank, p.amount, p.currency, p.txnID, b.region
FROM Payments as p LEFT OUTER JOIN Banks as b
ON b._key == source_bank;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='Banks:write')
INSERT INTO PaymentQueryWorkerRequest
SELECT source_bank, target_bank, amount, currency, txnID 
FROM PaymentWithRegion;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='PaymentRequests:write')
INSERT INTO PaymentQueryWorkerRequest2
SELECT source_bank, target_bank, txnID
FROM PaymentQueryWorkerResponse;

INSERT INTO SettlementsInMemory
SELECT source_bank, target_bank, txnID
FROM PaymentQueryWorkerResponse2;

-- Settlement Worker 1 & 2 ---

--@Transaction(group='TxnSuccess2', uid.field='txnID', mode='read')
INSERT INTO SettlementWithRegion
SELECT p.source_bank, p.target_bank, p.txnID, b.region
FROM SettlementsInMemory as p LEFT OUTER JOIN Banks2 as b
ON b._key == p.target_bank;

--@Transaction(group='TxnSuccess2', uid.field='txnID', mode='write')
INSERT INTO SettlementQueryWorkerRequest
SELECT source_bank, target_bank, txnID 
FROM SettlementWithRegion;

INSERT INTO SettlementWithStatus
SELECT p.source_bank, p.target_bank, p.txnID, b.status
FROM SettlementQueryWorkerResponse as p LEFT OUTER JOIN Settlement as b
ON b._key == p.txnID;

INSERT INTO SettlementQueryWorkerRequest2
SELECT source_bank, target_bank, txnID 
FROM SettlementWithStatus;

INSERT INTO ConfirmationsInMemory
SELECT source_bank, target_bank, str:concat('pw_', txnID) as txnID
FROM SettlementQueryWorkerResponse2;

-- Confirmation Worker --

@Transaction(group='TxnSuccess2', uid.field='txnID', mode='Banks:write')
INSERT INTO TargetBankQueryWorkerRequest
SELECT source_bank, target_bank, ifThenElse(math:rand() > 0.5, 1.0, -1.0) as amount, 'USD' as currency, txnID 
FROM ConfirmationsInMemory;

@Transaction(group='TxnSuccess2', uid.field='txnID', mode='Banks:write')
INSERT INTO SourceBankQueryWorkerRequest
SELECT source_bank, target_bank, amount, currency, txnID
FROM TargetBankQueryWorkerResponse;

@Transaction(group='TxnSuccess2', uid.field='txnID', mode='Ledger:write')
INSERT INTO LedgerQueryWorkerRequest
SELECT source_bank, target_bank, txnID
FROM TargetBankQueryWorkerResponse;


INSERT INTO OutputPerf
SELECT source_bank, target_bank, txnID 
FROM LedgerQueryWorkerResponse;



