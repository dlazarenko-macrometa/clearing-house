@App:name("Perf2CW")
@App:description("This app validates a payment")
@App:qlVersion('2')
@App:instances('25')

-- DEFINITIONS --

-- Confirmation Woprker ---

CREATE SOURCE Confirmations WITH (type='stream', stream.list='Confirmations', replication.type='global', subscription.name='sub1', map.type='json', subscription.initial.position='Latest')
(source_bank string, target_bank string, source_region string, target_region string, amount double, currency string, timestamp long, status string, txnID string);

CREATE SINK TargetBankQueryWorkerRequest WITH (type='query-worker', query="FOR doc IN Banks FILTER (doc._key == @target_bank ) UPDATE doc WITH { balance: doc.balance +@amount } IN Banks RETURN {txnID: @txnID , source_bank: @source_bank, target_bank: @target_bank, amount: @amount, currency: @currency}", sink.id="targetBank") 
(source_bank string, target_bank string, amount double, currency string, txnID string);

CREATE SOURCE TargetBankQueryWorkerResponse WITH (type='query-worker', sink.id="targetBank", map.type="json") 
(txnID string, source_bank string, target_bank string, amount double, currency string);

CREATE SINK SourceBankQueryWorkerRequest WITH (type='query-worker', query="FOR doc IN Banks FILTER (doc._key == @source_bank ) UPDATE doc WITH { balance: doc.balance -@amount, reserved: doc.reserved -@amount } IN Banks RETURN {txnID:@txnID , source_bank: @source_bank, target_bank: @target_bank, amount: @amount, currency: @currency}", sink.id="sourceBank") 
(source_bank string, target_bank string, amount double, currency string, txnID string);

CREATE SOURCE SourceBankQueryWorkerResponse WITH (type='query-worker', sink.id="sourceBank", map.type="json") 
(txnID string, source_bank string, target_bank string, amount double, currency string);

CREATE SINK LedgerQueryWorkerRequest WITH (type='query-worker', query="INSERT {'_txnID': 1234567, 'amount': 100.0, 'settlement_id': 4702603, 'currency': 'USD', 'source_bank': @source_bank, 'target_bank': @target_bank, 'timestamp': 1694114365199, 'status': 'settled'} INTO Ledger RETURN {source_bank: @source_bank, target_bank: @target_bank, txnID: @txnID}", sink.id="ledger") 
(source_bank string, target_bank string, txnID string);

CREATE SOURCE LedgerQueryWorkerResponse WITH (type='query-worker', sink.id="ledger", map.type="json") 
(source_bank string, target_bank string, txnID string);

CREATE SINK OutputPerf WITH (type='stream', stream='OutputPerf', replication.type='local', map.type='json')
(source_bank string, target_bank string, txnID string);

@Transaction(group='TxnSuccess2', uid.field='txnID', mode='Banks:write')
INSERT INTO TargetBankQueryWorkerRequest
SELECT source_bank, target_bank, 1.0 as amount, 'USD' as currency, txnID 
FROM Confirmations[
    source_region == context:getVar('region')
];

@Transaction(group='TxnSuccess2', uid.field='txnID', mode='Banks:write')
INSERT INTO SourceBankQueryWorkerRequest
SELECT source_bank, target_bank, amount, currency, txnID
FROM TargetBankQueryWorkerResponse;

@Transaction(group='TxnSuccess2', uid.field='txnID', mode='Ledger:write')
INSERT INTO LedgerQueryWorkerRequest
SELECT source_bank, target_bank, txnID
FROM TargetBankQueryWorkerResponse;

INSERT INTO OutputPerf
SELECT source_bank, target_bank, txnID 
FROM LedgerQueryWorkerResponse;
