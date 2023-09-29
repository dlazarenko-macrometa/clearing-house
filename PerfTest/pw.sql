@App:name("PWPerf")
@App:description("This app validates a payment")
@App:qlVersion('2')

-- DEFINITIONS --

CREATE TRIGGER MyTrigger WITH ( interval = 5000 millisec );

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE SINK PaymentQueryWorkerRequest WITH (type='query-worker', query.worker.name="reserve-money", sink.id="test") (source_bank string, target_bank string, amount double, currency string, txnID string);

CREATE SOURCE PaymentQueryWorkerResponse WITH (type='query-worker', sink.id="test", map.type="json") (txnID string, source_bank string, target_bank string, amount double, currency string);

CREATE STORE PaymentRequests WITH (type = 'database', replication.type="global", collection.type="doc")
(_key string, source_bank string, target_bank string, amount double, currency string, timestamp long);


-- QUERIES --

INSERT INTO Payments
SELECT str:concat("Chase_", convert(count() % 2000, 'string')) as source_bank, str:concat("Chase_", convert(count() % 2000 + 24000, 'string')) as target_bank, 
ifThenElse(math:rand() > 0.5, 1.0, -1.0) as amount, "USD" as currency, convert(math:rand() * 999999999999999L, 'string') as txnID
FROM MyTrigger;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='read')
INSERT INTO PaymentWithRegion
SELECT p.source_bank, p.target_bank, p.amount, p.currency, p.txnID, b.region
FROM Payments as p LEFT OUTER JOIN Banks as b
ON b._key == source_bank;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='write')
INSERT INTO PaymentQueryWorkerRequest
SELECT source_bank, target_bank, amount, currency, txnID 
FROM PaymentWithRegion;
/*UPDATE Banks
SET Banks.reserved = Banks.reserved + amount
ON Banks._key == source_bank
SELECT amount, source_bank, txnID
FROM PaymentWithRegion;*/

@Transaction(group='TxnSuccess', uid.field='_key', mode='write')
INSERT INTO PaymentRequests
SELECT txnID as _key , source_bank, target_bank, amount, currency, eventTimestamp() as timestamp
FROM PaymentQueryWorkerResponse;