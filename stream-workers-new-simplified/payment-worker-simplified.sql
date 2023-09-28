@App:name("PWNew")
@App:description("This app validates a payment")
@App:qlVersion('2')
@App:instances("6")

-- DEFINITIONS --

-- define input stream Payments, with expected message format, generate transaction ID and put it into _txnID field
CREATE SOURCE Payments WITH (type = 'stream', stream.list = "Payments", subscription.name='sub1', map.type='json', subscription.initial.position='Latest')
(source_bank string, target_bank string, amount double, currency string, txnID string);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string, balance long, reserved long, currency string, region string);

CREATE STORE PaymentRequests WITH (type = 'database', replication.type="global", collection.type="doc")
(_key string, source_bank string, target_bank string, amount double, currency string, timestamp long);

-- define Settlements stream, which will be used in Leg-2
CREATE SINK Settlements WITH (type='stream', stream='Settlements', replication.type='global', map.type='json')
(source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, txnID string);
 
-- QUERIES --

-- the main flow 3: reserve funds from Banks account, within the same DB transaction
@Transaction(group='TxnSuccess', uid.field='txnID', mode='write')
UPDATE Banks
SET Banks.reserved = Banks.reserved + amount
ON Banks._key == source_bank
SELECT amount, source_bank, txnID
FROM Payments;

@Transaction(group='TxnSuccess', uid.field='_key', mode='write')
INSERT INTO PaymentRequests
SELECT txnID as _key , source_bank, target_bank, amount, currency, eventTimestamp() as timestamp
FROM Payments;

-- the main flow 6: Send message to the next step (Leg-2). Transaction with name 'TxnSuccess' ends here 
--@Transaction(group='TxnSuccess', uid.field='_txnID')
INSERT INTO Settlements
SELECT source_bank, target_bank, ifThenElse(str:contains(source_bank, "Chase"), "cleaninghouse-us-west-1" , "cleaninghouse-us-east-1") as source_region, amount, currency, eventTimestamp() as timestamp, txnID
FROM Payments;