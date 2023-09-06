@App:name("HandleSinkErrors")
@App:qlVersion('2')

/**
    Sample for Using the Error Stream with a Event Sinks:

    - In this example, we utilize `OnError.action='stream'` with the `HTTPSink`.
    - If an error occurs during the event transmission to the HTTPSink (stream or at actual sink), the event that encountered the error will be directed to the corresponding fault stream (`!HTTPSink`).
    - Due to an incorrect `publisher.url` in the `HTTPSink` configuration, it will generate error events for every trigger event, and you will observe these errored events in the `ErrorStream`.
*/

CREATE TRIGGER EventTrigger WITH ( interval = 2 sec );
CREATE SINK HTTPSink WITH (
    type='http-call',
    sink.id='echo-service',
    publisher.url='http://grainier-san.eng.macrometa.io/',
    map.type='json',
    map.payload = '{{status}}',
    OnError.action='stream'
) (status string, count long, time long);
CREATE SINK HTTPSinkErrorStream WITH (
    type='stream',
    stream='ErrorStream',
    map.type='json'
) (status string, count long, time long, error string);

INSERT INTO HTTPSink
SELECT "Processed" as status, count() as count, eventTimestamp() as time
FROM EventTrigger;

INSERT INTO HTTPSinkErrorStream
SELECT status, count, time, convert(_error, 'string') as error
FROM !HTTPSink;
