@App:name("HandleStreamErrors")
@App:qlVersion('2')

/**
    Sample for Using the Error Stream with a Stream:
    - In this example, we employ `OnError.action='stream'` on the `CountStream`.
    - If an error occurs while sending events that arrive on the `CountStream` to one of its connected processes (in this case, `validate()`), the event that encountered the error will be directed to the corresponding fault stream (`!CountStream`).
    - Because the `validate()` method generates an error for every even count, you will observe these errored events in the `ErrorStream`.
    - This method captures errors occuered in Stream Processes (i.e. Windows), Stream Functions and Function Executors (i.e JS fuctions).
*/
CREATE TRIGGER EventTrigger WITH ( interval = 2 sec );
CREATE SOURCE CountStream WITH (type='inMemory', map.type='passthrough', OnError.action='stream') (c long, t long);
CREATE SINK CountStreamErrorStream WITH (type='stream', stream='ErrorStream', map.type='json') (c long, t long, error string);

CREATE FUNCTION validate[javascript] return string {
    var count = parseInt(data[0]);
if (count % 2 == 0) {
        throw new Error("Count is Even.")
    }
	return "Count is Odd.";
};

INSERT INTO CountStream
SELECT count() as c, eventTimestamp() as t
FROM EventTrigger;

INSERT INTO SampleStream
SELECT validate(c) as status, c, t
FROM CountStream;

INSERT INTO CountStreamErrorStream
SELECT c, t, convert(_error, 'string') as error
FROM !CountStream;
