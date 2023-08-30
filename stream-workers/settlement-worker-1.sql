@App:name("SettlementWorker1")
@App:description("This app settles a payment with confirmation from Bank B")
@App:qlVersion("2")

-- DEFINITIONS --

-- define input stream Settlements, with expected message format for receiving from remote regions
CREATE SOURCE SettlementsGlobal WITH (type='stream', stream.list='SettlementsGlobal', replication.type='global', map.type='json', transaction.uid.field='_txnID')
(source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, _txnID long);

-- define input stream Settlements, with expected message format
CREATE SINK Settlements WITH (type='stream', stream='Settlements', replication.type='local', map.type='json', transaction.uid.field='_txnID')
(source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, _txnID long);

-- define Banks collection in database, where we will store banks information
CREATE STORE Banks WITH (type='database', replication.type="global", collection.type="doc") (uuid string, name string,  balance long, reserved long, currency string, region string);

-- define internal stream SettlementWithBank which joins data from Payment stream and Banks collection
CREATE STREAM SettlementWithBank (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, _txnID long);

-- define internal stream ValidatedSettlement which generates Settlement ID and validated messages
CREATE STREAM ValidatedSettlement (settlement_id long, source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, status string, _txnID long);

-- define Transfers stream, which will be used for sending payment requests to Bank B
CREATE SINK Transfers WITH (type='stream', stream='Transfers', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, _txnID long);

-- define Settlement collection in database, where we will store the settled requests
CREATE STORE Settlement WITH (type = 'database', replication.type="global", collection.type="doc") 
(settlement_id long, source_bank string, target_bank string, source_region string, amount double, currency string, timestamp long, status string, _txnID long);

-- User Defined Functions in JavaScript that generates settlementID, the script can be changed to any unique generation of ID
CREATE FUNCTION generateSettlementId[javascript] return long {
	function getRandomInt(min, max) {
		return Math.floor(Math.random() * (max - min)) + min;
	}
	
     return getRandomInt(1, 99999999);
};

--- QUERIES --

-- replicating from remote regions 
INSERT INTO Settlements
SELECT source_bank, target_bank, source_region, amount, currency, timestamp, _txnID
FROM SettlementsGlobal;

-- the main flow 1: get region of Bank B, here we do not need transaction because region is fixed value.
INSERT INTO SettlementWithBank
SELECT s.source_bank, s.target_bank, s.source_region, b.region as target_region, s.amount, s.currency, s.timestamp, s._txnID
FROM Settlements as s JOIN Banks as b
ON s.target_bank == b.name;

-- the main flow 3: check if Bank A belongs to the current region
INSERT INTO ValidatedSettlement
SELECT generateSettlementId() as settlement_id, source_bank, target_bank, source_region, amount, currency, timestamp, 'active' as status, _txnID
FROM SettlementWithBank [
    target_region == context:getVar('region')
    -- here can be added other conditions
];

-- the main flow 4: Save to Settlement collection with status `active`
@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Settlement
SELECT settlement_id, source_bank, target_bank, source_region, amount, currency, timestamp, status, _txnID
FROM ValidatedSettlement;

-- the main flow 5: send payment request to Bank B
@Transaction(name='TxnSuccess', uid.field='_txnID')
INSERT INTO Transfers
SELECT source_bank, target_bank, amount, currency, timestamp, _txnID
FROM ValidatedSettlement;