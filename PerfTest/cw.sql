@App:name("CWPerf")
@App:description("This app validates a payment")
@App:qlVersion('2')

-- DEFINITIONS --

CREATE TRIGGER MyTrigger WITH ( interval = 5000 millisec );

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE SINK TargetBankQueryWorkerRequest WITH (type='query-worker', query.worker.name="cw-target-bank", sink.id="targetBank") 
(source_bank string, target_bank string, amount double, currency string, txnID string);

CREATE SOURCE TargetBankQueryWorkerResponse WITH (type='query-worker', sink.id="targetBank", map.type="json") 
(txnID string, source_bank string, target_bank string, amount double, currency string);

CREATE SINK SourceBankQueryWorkerRequest WITH (type='query-worker', query.worker.name="cw-source-bank", sink.id="sourceBank") 
(source_bank string, target_bank string, amount double, currency string, txnID string);

CREATE SOURCE SourceBankQueryWorkerResponse WITH (type='query-worker', sink.id="sourceBank", map.type="json") 
(txnID string, source_bank string, target_bank string, amount double, currency string);

CREATE STORE Ledger WITH (type = 'database', replication.type="global", collection.type="doc")
(_key string, source_bank string, target_bank string, amount double, currency string, timestamp long, settlement_id long, status string);

-- QUERIES --

INSERT INTO Payments
SELECT str:concat("Chase_", convert(count() % 2000, 'string')) as source_bank, str:concat("Chase_", convert(count() % 2000 + 12000, 'string')) as target_bank, 
ifThenElse(math:rand() > 0.5, 1.0, -1.0) as amount, "USD" as currency, convert(math:rand() * 999999999999999L, 'string') as txnID
FROM MyTrigger;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='write')
INSERT INTO TargetBankQueryWorkerRequest
SELECT source_bank, target_bank, amount, currency, txnID 
FROM Payments;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='write')
INSERT INTO SourceBankQueryWorkerRequest
SELECT source_bank, target_bank, amount, currency, txnID
FROM TargetBankQueryWorkerResponse;

@Transaction(group='TxnSuccess', uid.field='_key', mode='write')
INSERT INTO Ledger
SELECT txnID as _key , source_bank, target_bank, amount, currency, 122333565L as timestamp, 4702603L as settlement_id, 'settled' as status
FROM SourceBankQueryWorkerResponse;



