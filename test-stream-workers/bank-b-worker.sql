@App:name("BankB")
@App:qlVersion("2")

CREATE SOURCE Transfers WITH (type='stream', stream.list = 'Transfers', replication.type='local', map.type='json', subscription.initial.position='Latest')
(source_bank string, target_bank string, amount double, currency string, timestamp long, _txnID long);


CREATE SINK PayeeBankConfirmations  WITH (type='stream', stream='PayeeBankConfirmations', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, status string, _txnID long);


INSERT INTO PayeeBankConfirmations
SELECT source_bank, target_bank, amount, currency, timestamp, 'ACCP' as status, _txnID 
FROM Transfers;