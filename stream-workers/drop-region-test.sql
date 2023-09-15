@App:name("DropRegionTest")
@App:qlVersion("2")

CREATE TRIGGER MyTrigger WITH ( interval = 3 sec );

CREATE SINK Payments WITH (type='stream', stream='Payments', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, _txnID long, timestamp long);

-- 'eventTimestamp()' returns the timestamp of the processed/passed event.
INSERT INTO Payments
SELECT "BankA-west_0" as source_bank, 
    "BankB-central_0" as target_bank, 
    1.0 as amount, 
    "USD" as currency,
    convert(math:rand() * 999999999999999999L, 'long') as _txnID,
    currentTimeMillis() as timestamp
FROM MyTrigger;