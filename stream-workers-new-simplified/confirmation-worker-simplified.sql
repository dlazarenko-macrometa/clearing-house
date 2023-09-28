@App:name("CWNew")
@App:qlVersion("2")
@App:instances("8")

CREATE SOURCE Confirmations WITH (type='stream', stream.list='Confirmations', replication.type='global', subscription.name='sub1', map.type='json', subscription.initial.position='Latest')
(source_bank string, target_bank string, amount double, currency string, timestamp long, source_region string, status string, txnID string);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string,  balance long, reserved long, currency string, region string);

-- define PaymentRequests collection in database, where we will store the accepted payments
CREATE STORE Ledger WITH (type = 'database', replication.type="global", collection.type="doc") 
(_key string, source_bank string, target_bank string, amount double, currency string, timestamp long, status string);

-- define PayerBankConfirmations stream, on which we confirm that payment was accepted bu Bank B
CREATE SINK PayerBankConfirmations WITH (type='stream', stream='PayerBankConfirmations', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, status string, txnID string);

-- QUERIES --

INSERT INTO ValidatedConfirmations
SELECT source_bank, target_bank, amount, currency, timestamp, status, txnID
FROM Confirmations[
    source_region == context:getVar('region')
];


@Transaction(group='TxnSuccess', uid.field='txnID', mode='write')
UPDATE Banks
SET Banks.reserved = Banks.reserved - reserved, Banks.balance = Banks.balance - reserved
ON Banks._key == source_bank
SELECT amount AS reserved, source_bank, txnID
FROM ValidatedConfirmations;

-- the accepted flow 2: Add funds to the Bank B account
@Transaction(group='TxnSuccess', uid.field='txnID', mode='write')
UPDATE Banks
SET Banks.balance = Banks.balance + reserved
ON Banks._key == target_bank
SELECT amount AS reserved, target_bank, txnID
FROM ValidatedConfirmations;

@Transaction(group='TxnSuccess', uid.field='txnID', mode='write')
INSERT INTO Ledger
SELECT txnID as _key, source_bank, target_bank, amount, currency, timestamp, status
FROM ValidatedConfirmations;

-- the accepted flow 4: Publish Settlement request to stream
@Transaction(group='TxnSuccess', uid.field='txnID')
INSERT INTO PayerBankConfirmations
SELECT source_bank, target_bank, amount, currency, timestamp, 'ACCP' as status, txnID
FROM ValidatedConfirmations;