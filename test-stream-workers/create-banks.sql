@App:name("CreateBanks")
@App:qlVersion("2")

CREATE TRIGGER MyTrigger WITH ( interval = 1 sec );

CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (uuid string, name string,  balance long, reserved long, currency string, region string);

INSERT INTO countstream 
SELECT count() as c 
from MyTrigger;

-- 'eventTimestamp()' returns the timestamp of the processed/passed event.
INSERT INTO Banks
SELECT convert(c % 100, 'string') as uuid, str:concat("Chase_", convert(c % 100, 'string')) as name, 10000L as balance, 0L as reserved, "USD" as currency, "dmytro-us-west" as region
FROM countstream;