@App:name("PerformanceTest")
@App:qlVersion("2")

CREATE TRIGGER MyTrigger WITH ( interval = 1000 millisec );

CREATE SINK Test WITH (type='stream', stream='Test', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, _txnID long, timestamp long);

CREATE SINK Payments WITH (type='stream', stream='Payments', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, _txnID long, timestamp long);

INSERT INTO InMemory
SELECT count() as count 
FROM MyTrigger;

-- TEST --
/*
INSERT INTO Payments
SELECT str:concat("BankA-west_", convert(count - 1, 'string')) as source_bank,  
       str:concat("BankB-central_", convert(count - 1, 'string')) as target_bank, 
       10.0 as amount, 
       "USD" as currency,
       count  as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1
];
*/

-- US-WEST --

-- from west to central send 833 payments
INSERT INTO Payments
SELECT "BankA-west_1000" as source_bank,  
       "BankB-central_1000" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-west" == context:getVar('region')
];

-- from west to east send 833 payments
INSERT INTO Payments
SELECT "BankA-west_2000" as source_bank,  
       "BankB-east_1000" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       833 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-west" == context:getVar('region')
];

-- from west to southeast send 834 payments
INSERT INTO Payments
SELECT "BankA-west_2499" as source_bank,  
       "BankB-southeast_1000" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       1666 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-west" == context:getVar('region')
];

-- US-CENTRAL --

-- from central to west send 833 payments
INSERT INTO Payments
SELECT "BankA-central_1000" as source_bank,  
       "BankB-west_1000" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       2500 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-central" == context:getVar('region')
];

-- from central to east send 833 payments
INSERT INTO Payments
SELECT "BankA-central_2000" as source_bank,  
       "BankB-east_2000" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       2500 + 833 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-central" == context:getVar('region')
];

-- from central to southeast send 834 payments
INSERT INTO Payments
SELECT "BankA-central_2499" as source_bank,  
       "BankB-southeast_2000" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       2500 + 1666 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-central" == context:getVar('region')
];

-- US-EAST --

-- from east to central send 833 payments
INSERT INTO Payments
SELECT "BankA-east_1000" as source_bank,  
       "BankB-central_2000" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       5000 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-east" == context:getVar('region')
];

-- from east to west send 833 payments
INSERT INTO Payments
SELECT "BankA-east_2000" as source_bank,  
       "BankB-west_2000" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       5000 + 833 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-east" == context:getVar('region')
];

-- from east to southeast send 834 payments
INSERT INTO Payments
SELECT "BankA-east_2499" as source_bank,  
       "BankB-southeast_2499" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       5000 + 1666 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-east" == context:getVar('region')
];

-- US-SOUTHEAST --

-- from southeast to central send 833 payments
INSERT INTO Payments
SELECT "BankA-southeast_1000" as source_bank,  
       "BankB-central_2499" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       7500 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-southeast" == context:getVar('region')
];

-- from east to west send 833 payments
INSERT INTO Payments
SELECT "BankA-southeast_2000" as source_bank,  
       "BankB-west_2499" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       7500 + 833 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-southeast" == context:getVar('region')
];

-- from southeast to east send 834 payments
INSERT INTO Payments
SELECT "BankA-southeast_2499" as source_bank,  
       "BankB-east_2499" as target_bank, 
       100.0 as amount, 
       "USD" as currency,
       7500 + 1666 + count as _txnID,
       currentTimeMillis() as timestamp
FROM InMemory [
    count <= 1 AND 
    "clearinghouse-us-southeast" == context:getVar('region')
];