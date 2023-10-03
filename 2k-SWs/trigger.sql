@App:name("TriggerAllIn")
@App:description("This app will produce an event after every 5 seconds")
@App:qlVersion('2')

CREATE TRIGGER MyTrigger WITH ( interval = 1 millisec );

CREATE SINK Payments WITH (type='stream', stream='Payments', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, txnID string);

INSERT INTO Payments
SELECT str:concat("Chase_", convert(count() % 20000, 'string')) as source_bank, str:concat("Fargo_", convert(count() % 20000, 'string')) as target_bank, 1.0 as amount, "USD" as currency, convert((currentTimeMillis() * 100000 + (count() % 100000)) * 30, 'string') as txnID
FROM MyTrigger;

