@App:name("ErrorHandler")
@App:qlVersion('2')

/**
    Utility Stream Worker for reading errors from `ErrorStream`, formatting them and Persisting Error Events in the `ErrorTable`.
*/

CREATE SOURCE ErrorStream WITH (type='stream', stream.list='ErrorStream', map.type='json', map.attributes.evt = "$") (evt string);
CREATE TABLE ErrorTable (event object, error object);

INSERT INTO ErrorTable
SELECT map:remove(map:createFromJSON(evt), 'error') as event, json:toObject(json:getString(evt,'$.error')) as error
FROM ErrorStream;
