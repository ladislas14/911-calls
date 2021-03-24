//const elasticsearch = require('elasticsearch');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

const ELASTIC_SEARCH_URI = 'http://localhost:9200';
const INDEX_NAME = '911-calls';

async function run() {
  const client = new Client({ node: ELASTIC_SEARCH_URI});

  // Drop index if exists
  await client.indices.delete({
    index: INDEX_NAME,
    ignore_unavailable: true
  });

  await client.indices.create({
    index: INDEX_NAME,
    body : {
      mappings: {
        properties: {
          location: {type: "geo_point"},
          desc: {
            type: "text", 
            fields : {
              keyword : {
                type : "keyword",
                ignore_above : 256
              }
            }
          },
          zip: {
            type: "text", 
            fields : {
              keyword : {
                type : "keyword",
                ignore_above : 256
              }
            }
          },
          title: {
            type: "text", 
            fields : {
              keyword : {
                type : "keyword",
                ignore_above : 256
              }
            }
          },
          timeStamp: {type: "date"},
          twp: {
            type: "text", 
            fields : {
              keyword : {
                type : "keyword",
                ignore_above : 256
              }
            }
          },
          addr: {
            type: "text", 
            fields : {
              keyword : {
                type : "keyword",
                ignore_above : 256
              }
            }
          },
          category: {
            type: "text", 
            fields : {
              keyword : {
                type : "keyword",
                ignore_above : 256
              }
            }
          }
        }
      }
    }
  });

  let calls = [];

  fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      const call = { 
        location: {
          lat: data.lat,
          lon: data.lng
        },
        desc: data.desc,
        zip: data.zip,
        title: data.title,
        timeStamp: new Date(data.timeStamp).toISOString(),
        twp: data.twp,
        addr: data.addr,
        category: data.title.split(':')[0]
      };
      calls.push(call)
    })
    .on('end', async () => {
      client.bulk(createBulkInsertQuery(calls), (err, resp) => {
        if (err) console.trace(err.message);
        else console.log(`Inserted ${resp.body.items.length} calls 🔥`);
        client.close();
      });
    });
  

}

function createBulkInsertQuery(calls) {
  const body = calls.reduce((acc, call) => {
    acc.push({ index: { _index: INDEX_NAME, _type: '_doc'} })
    acc.push({...call})
    return acc
  }, []);

  return { body };
}

run().catch((e) => {
  console.log(JSON.stringify(e))
});


