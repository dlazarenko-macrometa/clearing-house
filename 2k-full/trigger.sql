@App:name("TriggerAllIn")
@App:description("This app will produce an event after every 5 seconds")
@App:qlVersion('2')

CREATE TRIGGER MyTrigger WITH ( interval = 5000 millisec );

CREATE SINK Payments WITH (type='stream', stream='Payments', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, txnID string);

INSERT INTO Payments
SELECT str:concat("Chase_", convert(count() % 12000, 'string')) as source_bank, str:concat("Chase_", convert(count() % 12000 + 12000, 'string')) as target_bank, 
1.0 as amount, "USD" as currency, currentTimeMillis() as timestamp, convert(currentTimeMillis() * 10000 + (count() % 10000), 'string') as txnID
FROM MyTrigger;

