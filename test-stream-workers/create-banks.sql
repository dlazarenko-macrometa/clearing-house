@App:name("CreateBanks")
@App:qlVersion("2")

CREATE TRIGGER MyTrigger WITH ( interval = 1 sec );

CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (_key string, uuid string, name string,  balance long, reserved long, currency string, region string);

INSERT INTO countstream 
SELECT count() as c 
from MyTrigger;

-- 'eventTimestamp()' returns the timestamp of the processed/passed event.
INSERT INTO Banks
SELECT str:concat("Chase_", convert(c % 24000, 'string')) as _key, convert(c % 24000, 'string') as uuid, str:concat("Chase_", convert(c % 24000, 'string')) as name, 10000L as balance, 0L as reserved, "USD" as currency, "ch64mn-us-west" as region
FROM countstream;
