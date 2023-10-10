@App:name("SW")
@App:description("This app validates a payment")
@App:qlVersion('2')

-- DEFINITIONS --

CREATE SOURCE Settlements WITH (type='stream', stream.list='Settlements', replication.type='global', subscription.name='sub1',  map.type='json', subscription.initial.position='Latest')
(source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string);

CREATE SINK SettlementQueryWorkerRequest WITH (type='query-worker', query="INSERT  {'settlement_id': @settlement_id, 'source_bank': @source_bank, 'target_bank': @target_bank, 'source_region': @source_region, target_region: @target_region, 'amount': @amount, 'currency': @currency, 'timestamp': @timestamp, 'status': @status, 'txnID': @txnID, '_key': @txnID} INTO Settlement RETURN NEW", sink.id="test3") (settlement_id string, source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, status string, txnID string);

CREATE SOURCE SettlementQueryWorkerResponse WITH (type='query-worker', sink.id="test3", map.type="json") (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, txnID string, status string);

CREATE STORE PaymentRequests WITH (type = 'database', replication.type="global", collection.type="doc")
(_key string, source_bank string, target_bank string, amount double, currency string, timestamp long);

CREATE STORE Settlement WITH (type = 'database', replication.type="global", collection.type="doc")
(_key string, source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, settlement_id long, _txnID string, timestamp long, status string);

CREATE SINK SettlementQueryWorkerRequest2 WITH (type='query-worker', query="FOR doc IN Settlement FILTER (doc._key == @txnID ) UPDATE doc WITH { status: @status } IN Settlement RETURN { source_bank: @source_bank, target_bank: @target_bank, source_region: @source_region, target_region: @target_region, amount: @amount, currency: @currency, timestamp: @timestamp, status: @status, txnID: @txnID}", sink.id="test4") (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, status string, txnID string);

CREATE SOURCE SettlementQueryWorkerResponse2 WITH (type='query-worker', sink.id="test4", map.type="json") (source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, status string, txnID string);

CREATE SINK Transfers WITH (type='stream', stream='Transfers', replication.type='local', map.type='json')
(source_bank string, target_bank string, amount double, currency string, timestamp long, txnID string);

CREATE SOURCE PayeeBankConfirmations WITH (type='stream', stream.list = 'PayeeBankConfirmations', map.type='json', subscription.name='sub1')
(source_bank string, target_bank string, amount double, currency string, timestamp long, status string, txnID string);

CREATE SINK Confirmations WITH (type='stream', stream='Confirmations', replication.type='global', map.type='json')
(source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, status string, txnID string);

CREATE SINK Test WITH (type='stream', stream='Test', replication.type='local', map.type='json')
(cachedSettlement object, loaded_timestamp long);

-- Settlement Worker 1 & 2 ---

--@Transaction(group='TxnSuccess2', uid.field='txnID', mode='write')
INSERT INTO SettlementQueryWorkerRequest
SELECT convert(math:rand() * 999999999999999L, 'string') as settlement_id, source_bank, target_bank, source_region, target_region, amount, currency, timestamp, 'active' as status, txnID 
FROM Settlements[
    target_region == context:getVar('region')
];

-- Save settlement to memory cache
INSERT INTO SettlementToCache
SELECT source_bank, target_bank, amount, currency, timestamp, txnID, 
memcache:put(txnID, str:concat(
    "{'sb':'", source_bank,
    "','tb':'", target_bank,
    "','sr':'", source_region,
    "','tr':'", target_region,
    "','a':", convert(amount,'string'),
    ",'c':'", currency,
    "','s':'active'}"), 5000L) as json
FROM SettlementQueryWorkerResponse;

-- send request to Bank B
INSERT INTO Transfers
SELECT source_bank, target_bank, amount, currency, timestamp, txnID 
FROM SettlementToCache;

-- Bank B --

-- get cached settlement by ID
INSERT INTO PayeeWithCachedJsonSettlement
SELECT source_bank, target_bank, amount, currency, timestamp, txnID, status as payee_status, json:toObject(memcache:get(txnID)) as cachedSettlement
FROM PayeeBankConfirmations;

-- check if it exists
INSERT INTO PayeeWithExistsCache
SELECT source_bank, target_bank, amount, currency, timestamp, payee_status, txnID, cachedSettlement
FROM PayeeWithCachedJsonSettlement [
    not(cachedSettlement IS NULL)
];

-- parse cached settlement
INSERT INTO SettlementToValidate
SELECT source_bank, target_bank, amount, currency, timestamp, payee_status, txnID, 
    json:getString(cachedSettlement, '$.sb') as loaded_source_bank, 
    json:getString(cachedSettlement, '$.tb') as loaded_target_bank,
    json:getString(cachedSettlement, '$.sr') as loaded_source_region, 
    json:getString(cachedSettlement, '$.tr') as loaded_target_region,
    json:getDouble(cachedSettlement, '$.a') as loaded_amount,
    json:getString(cachedSettlement, '$.c') as loaded_currency,
    timestamp as loaded_timestamp,
    json:getString(cachedSettlement, '$.s') as loaded_status
FROM PayeeWithExistsCache;

/*INSERT INTO Test
SELECT cachedSettlement, json:getLong(cachedSettlement, '$.t') as loaded_timestamp
FROM PayeeWithExistsCache;*/

-- cache is empty
INSERT INTO PayeeWithEmptyCache
SELECT source_bank, target_bank, amount, currency, timestamp, payee_status, txnID
FROM PayeeWithCachedJsonSettlement [
    cachedSettlement IS NULL
];

-- retrieve settlement from collection
INSERT INTO SettlementToValidate
SELECT 
    p.source_bank, 
    p.target_bank, 
    p.amount, 
    p.currency, 
    p.timestamp, 
    p.payee_status,
    p.txnID,
    s.source_bank as loaded_source_bank, 
    s.target_bank as loaded_target_bank, 
    s.source_region as loaded_source_region, 
    s.target_region as loaded_target_region, 
    s.amount as loaded_amount, 
    s.currency as loaded_currency, 
    s.timestamp as loaded_timestamp, 
    s.status as loaded_status
FROM PayeeWithEmptyCache as p LEFT OUTER JOIN Settlement as s
ON s._key == p.txnID;

-- validate settlement
INSERT INTO PayeeValidated
SELECT loaded_source_bank as source_bank, 
    loaded_target_bank as target_bank,
    loaded_target_region as target_region,
    loaded_source_region as source_region,
    loaded_amount as amount,
    loaded_currency as currency,
    loaded_timestamp as timestamp, 
    payee_status,
    txnID, 
    ifThenElse(loaded_source_bank is null, "Transaction has not been found",
        ifThenElse(loaded_source_bank != source_bank, "Source bank is different for this transaction",
            ifThenElse(loaded_target_bank != target_bank, "Target bank is different for this transaction", 
                ifThenElse(loaded_amount != amount, "Amount is different for this transaction", 
                    ifThenElse(loaded_currency != currency, "Currency is different for this transaction", 
                        /*ifThenElse(currentTimeMillis() - timestamp > 5000, "Response is older than 5 seconds",
                            ifThenElse(loaded_status != 'active', "Transaction already completed", */getNull('string')/*) 
                        )*/
                    )
                )
            )
        )   
    ) as failedMsg
FROM SettlementToValidate;

-- settlement validation passed
INSERT INTO PayeePassedValidation
SELECT source_bank, target_bank, source_region, target_region, amount, currency, timestamp, payee_status, txnID
FROM PayeeValidated [
    failedMsg IS NULL
];

-- update status of a setlement
INSERT INTO SettlementQueryWorkerRequest2
SELECT source_bank, target_bank, source_region, target_region, amount, currency, timestamp, ifThenElse(payee_status == 'ACCP', 'settled', 'failed') as status, txnID 
FROM PayeePassedValidation;

-- send payment to the Bank A region
INSERT INTO Confirmations
SELECT source_bank, target_bank, source_region, target_region, amount, currency, timestamp, status, txnID
FROM SettlementQueryWorkerResponse2;
