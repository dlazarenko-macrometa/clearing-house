import exec from 'k6/execution';
import http from 'k6/http';
import { check, sleep } from 'k6';

// Constants
const API_KEY = 'd_m.com.perftest.44Co8yg0l09Ueonxu2VTx0wIQFHkuDeqyAHPfrTtWynHzMn1AyIEmwr6MmtEsamA39ce97';
const HOST = 'https://api-cleaninghouse-us-west-1.paas.macrometa.io:9443';
const HEADERS = {
    'Authorization': `apikey ${API_KEY}`,
    'Content-Type': 'application/json',
};

// Collection API
function createCollection(name) {
    const payload = {
        cacheEnabled: false,
        enableShards: false,
        isLocal: false,
        name: name,
        type: 2,
        stream: false
    };
    const res = http.post(`${HOST}/_fabric/_system/_api/collection`, JSON.stringify(payload), { headers: HEADERS });
    check(res, {
        'collection created': (r) => r.status === 200,
    });
    return res.status === 200;
}

function removeCollection(name) {
    const res = http.del(`${HOST}/_fabric/_system/_api/collection/${name}`, null, { headers: HEADERS });
    check(res, {
        'collection removed': (r) => r.status === 200,
    });
    return res.status === 200;
}

// Document API
function createDocument(collection, document, trxId) {
    var headers = HEADERS;
    if (trxId != null) {
        headers = Object.assign({ 'x-gdn-trxid': trxId }, HEADERS);
    }
    const res = http.post(`${HOST}/_fabric/_system/_api/document/${collection}`, JSON.stringify(document), { headers: headers });
    check(res, {
        'document created': (r) => r.status === 202,
    });
    return res.status === 202;
}

function getDocument(key, collection, trxId) {
    var headers = Object.assign({ 'x-gdn-trxid': trxId }, HEADERS);
    // var headers = HEADERS;
    const res = http.get(`${HOST}/_api/document/${collection}/${key}`, { headers: headers });
    check(res, {
        'document got': (r) => r.status === 200,
    });
}

// Index API
function createIndex(collection, fields, type) {
    const payload = {
        'fields': fields,
        'type': type,
        'deduplicate': false,
        'sparse': false,
        'unique': false
    };
    const res = http.post(`${HOST}/_fabric/_system/_api/index?collection=${collection}`, JSON.stringify(payload), { headers: HEADERS });
    check(res, {
        'index created': (r) => r.status === 201,
    });
    return res.status === 201;
}

// Transactions API
function createTransaction(writeCollections) {
    const payload = {
        collections: {
            // read: writeCollections
            write: writeCollections
        }
    };
    const res = http.post(`${HOST}/_fabric/_system/_api/transaction/begin`, JSON.stringify(payload), { headers: HEADERS });
    check(res, {
        'transaction created': (r) => r.status === 201,
    });
    if (res.status != 201) {
        return null;
    }
    const body = JSON.parse(res.body);
    const trxId = body.result.id;
    return trxId;
}

function commitTransaction(trxId) {
    const res = http.put(`${HOST}/_fabric/_system/_api/transaction/${trxId}`, null, { headers: HEADERS });
    check(res, {
        'transaction committed': (r) => r.status === 200,
    });
}

function abortTransaction(trxId) {
    const res = http.del(`${HOST}/_fabric/_system/_api/transaction/${trxId}`, null, { headers: HEADERS });
    check(res, {
        'transaction aborted': (r) => r.status === 200,
    });
}

// Query API
function runQuery(query, bindVars, trxId) {
    const payload = {
        query: query,
        bindVars: bindVars
    };
    var headers = HEADERS;
    if (trxId != null) {
        headers = Object.assign({ 'x-gdn-trxid': trxId }, HEADERS);
    }
    const res = http.post(`${HOST}/_fabric/_system/_api/cursor`, JSON.stringify(payload), { headers: headers });
    check(res, {
        'query execute': (r) => r.status === 201,
    });
    if (res.status !== 201) {
        console.log(res);
    }
    return res.status === 201;
}

// Version API
function getVersion() {
    const res = http.get(`${HOST}/_api/version`, { headers: HEADERS });
    check(res, {
        'version got': (r) => r.status === 200,
    });
}

export const options = {
    vus: 100,
    duration: '30s',
};

// Workers

function paymentWorker() {
    const trxId = createTransaction(['Banks', 'PaymentRequests']);
    if (trxId === null) {
        return;
    }
    
    const query1 = 'FOR doc IN @@collection FILTER (doc._key == @p_source_bank ) RETURN { "uuid" : doc.uuid, "name" : doc.name, "balance" : doc.balance, "reserved" : doc.reserved, "currency" : doc.currency, "region" : doc.region }';
    if (!runQuery(query1, { 'p_source_bank': `Chase_${exec.vu.idInTest}`, '@collection': 'Banks' }, trxId)) {
        abortTransaction(trxId);
        return;
    }
    
    const query2 = 'FOR doc IN @@collection FILTER (doc._key == @source_bank )  UPDATE doc WITH { reserved: doc.reserved +@amount } IN @@collection';
    if (!runQuery(query2, { 'amount': 100.0, '@collection': 'Banks', 'source_bank': `Chase_${exec.vu.idInTest}` }, trxId)) {
        abortTransaction(trxId);
        return;
    }
    
    const id = exec.vu.iterationInInstance * options.vus + exec.vu.idInTest;
    createDocument('PaymentRequests', {'_txnID': id, 'amount': 100.0, 'currency': 'USD', 'source_bank': `Chase_${exec.vu.idInTest}`, 'target_bank': `Fargo_${exec.vu.idInTest}`, 'timestamp': 1694114365199}, trxId);
    
    commitTransaction(trxId);
}

function settlementWorker1() {
    const query1 = 'FOR doc IN @@collection FILTER (doc._key == @s_target_bank) RETURN { "uuid" : doc.uuid, "name" : doc.name, "balance" : doc.balance, "reserved" : doc.reserved, "currency" : doc.currency, "region" : doc.region }';
    runQuery(query1, { 's_target_bank': `Fargo_${exec.vu.idInTest}`, '@collection': 'Banks' }, null);
    
    const id = exec.vu.iterationInInstance * options.vus + exec.vu.idInTest;
    createDocument('Settlement', {'_txnID': id, 'amount': 100.0, 'settlement_id': id, 'source_region': 'chouse-us-west', 'currency': 'USD', '_key': `${id}`, 'source_bank': `Chase_${exec.vu.idInTest}`, 'target_bank': `Fargo_${exec.vu.idInTest}`, 'timestamp': 1694114365199, 'status': 'active'}, null);
}

function settlementWorker2() {
    const id = exec.vu.iterationInInstance * options.vus + exec.vu.idInTest;
    const query1 = 'FOR doc IN @@collection FILTER (doc._key == @c_txnIdStr ) RETURN { "_key" : doc._key, "settlement_id" : doc.settlement_id, "source_bank" : doc.source_bank, "target_bank" : doc.target_bank, "source_region" : doc.source_region, "amount" : doc.amount, "currency" : doc.currency, "timestamp" : doc.timestamp, "status" : doc.status, "_txnID" : doc._txnID }';
    runQuery(query1, { 'c_txnIdStr': id, '@collection': 'Settlement' }, null);
    
    const query2 = 'FOR doc IN @@collection FILTER (doc._key == @_txnID )  UPDATE doc WITH { status: @status } IN @@collection';
    runQuery(query2, { '_txnID': id, '@collection': 'Settlement', 'status': 'settled' }, null);
}

function confirmationWorker() {
    const trxId = createTransaction(['Banks', 'Ledger']);
    if (trxId === null) {
        return;
    }
    
    const id = exec.vu.iterationInInstance * options.vus + exec.vu.idInTest;
    const query1 = 'FOR doc IN @@collection FILTER (doc._key == @target_bank )  UPDATE doc WITH { balance: doc.balance +@amount} IN @@collection';
    if (!runQuery(query1, { 'amount': 100.0, '@collection': 'Banks', 'target_bank': `Fargo_${exec.vu.idInTest}` }, trxId)) {
        abortTransaction(trxId);
        return;
    }
    
    const query2 = 'FOR doc IN @@collection FILTER (doc._key == @source_bank )  UPDATE doc WITH { balance: doc.balance -@amount, reserved: doc.reserved -@amount } IN @@collection';
    if (!runQuery(query2, { 'amount': 100.0, '@collection': 'Banks', 'source_bank': `Chase_${exec.vu.idInTest}` }, trxId)) {
        abortTransaction(trxId);
        return;
    }
    
    createDocument('Ledger', {'_txnID': id, 'amount': 100.0, 'settlement_id': 4702603, 'currency': 'USD', 'source_bank': `Chase_${exec.vu.idInTest}`, 'target_bank': `Fargo_${exec.vu.idInTest}`, 'timestamp': 1694114365199, 'status': 'settled'}, trxId);
    
    commitTransaction(trxId);
}

export function setup() {
    createCollection('Banks');
    for (let i = 0; i < options.vus; i++) {
        let id = i + 1;
        let bank1 = {
            "balance": 10000,
            "currency": "USD",
            "name": `Chase_${id}`,
            "region": "chouse-us-west",
            "reserved": 0,
            "uuid": "1"
        };
        createDocument('Banks', bank1);
        
        let bank2 = {
            "balance": 10000,
            "currency": "USD",
            "name": `Fargo_${id}`,
            "region": "chouse-us-west",
            "reserved": 0,
            "uuid": "1"
        };
        createDocument('Banks', bank2);
    }
    createIndex('Banks', ['name'], 'persistent');
    
    createCollection('Settlement');
    createIndex('Settlement', ['_txnID'], 'persistent');
    
    createCollection('Ledger');
    createCollection('PaymentRequests');
}

export function teardown(data) {
    /*removeCollection('Banks');
    removeCollection('Settlement');
    removeCollection('Ledger');
    removeCollection('PaymentRequests');*/
}

export default function () {
    paymentWorker();
    settlementWorker1();
    settlementWorker2();
    confirmationWorker();
}

