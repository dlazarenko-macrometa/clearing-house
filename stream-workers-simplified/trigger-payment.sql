@App:name("TriggerPayment")
@App:description("This app will produce an event after every 5 seconds")
@App:qlVersion('2')

CREATE TRIGGER MyTrigger WITH ( interval = 1 millisec );

CREATE SINK Settlements WITH (type='stream', stream='Payments', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string);

-- 'eventTimestamp()' returns the timestamp of the processed/passed event.
INSERT INTO Settlements
SELECT str:concat("Chase_", convert(count() % 8000, 'string')) as source_bank, str:concat("Fargo_", convert(count() % 8000, 'string')) as target_bank, 
ifThenElse(math:rand() > 0.5, 1.0,-1.0) as amount, "USD" as currency
FROM MyTrigger[
    'cleaninghouse-us-west-1' == context:getVar('region')
];


INSERT INTO Settlements
SELECT str:concat("Fargo_", convert(count() % 8000 + 12000, 'string')) as source_bank, str:concat("Chase_", convert(count() % 8000 + 12000, 'string')) as target_bank, 
ifThenElse(math:rand() > 0.5, 1.0,-1.0) as amount, "USD" as currency
FROM MyTrigger[
    'cleaninghouse-us-east-1' == context:getVar('region')
];