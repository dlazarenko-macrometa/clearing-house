@App:name("CreateBanks")
@App:qlVersion("2")

CREATE TRIGGER MyTrigger WITH ( interval = 1 millisec );

CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string,  balance long, reserved long, currency string, region string);

INSERT INTO countstream 
SELECT count() as c 
from MyTrigger;

INSERT INTO Banks
SELECT str:concat("Chase_", convert(c % 24000, 'string')) as _key, convert(c % 24000, 'string') as uuid, str:concat("Fargo_", convert(c % 24000, 'string')) as name, 10000L as balance, 0L as reserved, "USD" as currency, "cleaninghouse-us-west-1" as region
FROM countstream;

INSERT INTO Banks
SELECT str:concat("Fargo_", convert(c % 24000, 'string')) as _key, convert(c % 24000, 'string') as uuid, str:concat("Fargo_", convert(c % 24000, 'string')) as name, 10000L as balance, 0L as reserved, "USD" as currency, "cleaninghouse-us-east-1" as region
FROM countstream;