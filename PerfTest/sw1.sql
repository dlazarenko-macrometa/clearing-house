@App:name("SW1Perf")
@App:description("This app validates a payment")
@App:qlVersion('2')

-- DEFINITIONS --

CREATE TRIGGER MyTrigger WITH ( interval = 5000 millisec );

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE STORE Settlement WITH (type = 'database', replication.type="global", collection.type="doc")
(_key string, source_bank string, target_bank string, amount double, currency string, settlement_id long, source_region string, _txnID string, timestamp long, status string);


-- QUERIES --

INSERT INTO Payments
SELECT str:concat("Chase_", convert(count() % 2000, 'string')) as source_bank, str:concat("Chase_", convert(count() % 2000 + 12000, 'string')) as target_bank, 
ifThenElse(math:rand() > 0.5, 1.0, -1.0) as amount, "USD" as currency, convert(math:rand() * 999999999999999L, 'string') as txnID
FROM MyTrigger;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='read')
INSERT INTO PaymentWithRegion
SELECT p.source_bank, p.target_bank, p.amount, p.currency, p.txnID, b.region
FROM Payments as p LEFT OUTER JOIN Banks as b
ON b._key == target_bank;

@Transaction(group='TxnSuccess', uid.field='_key', mode='write')
INSERT INTO Settlement
SELECT txnID as _key , source_bank, target_bank, amount, currency, 1694114365199L as settlement_id, 'chouse-us-west' as source_region, txnID as _txnID, 1694114365199L as timestamp, 'active' as status
FROM PaymentWithRegion;