const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const Rx = require('rxjs/Rx');
const _ = require('lodash');
const formatObject = (fields) => {
  return (object) => {
    const newObject = _.clone(object);
    _.each(fields, field => {
      newObject[field] = (newObject[field] || '').replace(/["']/g, '').trim();
    });
    return newObject;
  };
};

// Connection URL
const url = 'xxx';
// Use connect method to connect to the Server
MongoClient.connect(url, {uri_decode_auth : true}, (errCNX, db) => {
  // eslint-disable-next-line no-console
  console.time('format');
  assert.equal(null, errCNX);
  // eslint-disable-next-line no-console
  console.log('Connected correctly to server');

  const collection = db.collection('xxx');
  const cursor = collection.find({}).stream();
  let count = 0;
  setInterval(() => {
    // eslint-disable-next-line no-console
    console.log(count);
  }, 5000);
  Rx.Observable.fromEvent(cursor, 'data')
    .takeUntil(Rx.Observable.fromEvent(cursor, 'close'))
    .bufferTime(1000)
    .subscribe(
      (docs) => {
        count += docs.length;
        if (_.inRange(count, 510101, 10000000)) {
          const formattedDocs = _.map(docs, formatObject([
            'xxx',
          ]));
          _.each(formattedDocs, (doc) => {
            collection.updateOne({_id : doc._id}, doc);
          });
        }
      },
      err => {
        assert.equal(null, err);
        // eslint-disable-next-line
        console.log("Error: %s", err)
        db.close();
      },
      () => {
        // eslint-disable-next-line no-console
        console.timeEnd('format');
        // eslint-disable-next-line no-console
        console.log('collection successfully saved.');
        db.close();
      }
    );
});
