@App:name("HandleTableErrors")
@App:qlVersion('2')
/**
    Sample for Using the Error Stream with an Event Table:

    - In this example, we employ both `OnError.action='stream'` with the `SampleTable`.
    - If an error occurs while interacting with the SampleTable, the event that encountered the error will be directed to the corresponding fault stream (`!SampleTable`).
    - When running this stream worker, attempt to delete the `SampleTable` document collection. This action will generate error events for every trigger event, and you will observe these errored events in the `ErrorStream`.
*/

CREATE TRIGGER EventTrigger WITH ( interval = 5 sec );
CREATE STORE SampleTable WITH (type='database', replication.type="global", collection.type="doc", OnError.action='STREAM') (status string, count long, time long);
CREATE SINK SampleTableErrorStream WITH (type='stream', stream='ErrorStream', map.type='json') (status string, count long, time long, error string);

INSERT INTO SampleTable
SELECT "Processed" as status, count() as count, eventTimestamp() as time
FROM EventTrigger;

INSERT INTO SampleTableErrorStream
SELECT status, count, time, convert(_error, 'string') as error
FROM !SampleTable;
