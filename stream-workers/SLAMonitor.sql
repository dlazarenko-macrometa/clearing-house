@App:name("SLAMonitor")
@App:qlVersion("2")

CREATE SOURCE PaymentsSLA WITH (type = 'stream', stream.list = "PaymentsSLA", map.type='json')
(source_bank string, target_bank string, amount double, currency string, _txnID long);

CREATE SINK Payments WITH (type='stream', stream='Payments', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, _txnID long, timestamp long);

CREATE SOURCE PayerBankConfirmations WITH (type = 'stream', stream.list = "PayerBankConfirmations", map.type='json')
(_txnID long, source_bank string, target_bank string, amount double, currency string, status string, timestamp long, message string);

CREATE SINK PayerBankConfirmationsSLA WITH (type='stream', stream='PayerBankConfirmationsSLA', replication.type='local', map.type='json')
(_txnID long, source_bank string, target_bank string, amount double, currency string, status string, timestamp long, message string, latencyMs long);

INSERT INTO Payments
SELECT source_bank, target_bank, amount, currency, _txnID, currentTimeMillis() as timestamp
FROM PaymentsSLA;

INSERT INTO PayerBankConfirmationsSLA
SELECT _txnID, source_bank, target_bank, amount, currency, status, timestamp, message, currentTimeMillis() - timestamp as latencyMs
FROM PayerBankConfirmations;