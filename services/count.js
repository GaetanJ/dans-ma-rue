const config = require("config");
const indexName = config.get("elasticsearch.index_name");

exports.count = (client, from, to, callback) => {
  // TODO Compter le nombre d'anomalies entre deux dates

  callback({
    count: 0
  });
};

exports.countAround = (client, lat, lon, radius, callback) => {
  // TODO Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné
  client
    .count({
      index: indexName,
      body: {
        query: {
          range: {
            location: {
              distance: radius,
              "pin.location": {
                lat: lat,
                lon: lon
              }
            }
          }
        }
      }
    })
    .then(resp => {
      callback({
        count: resp.body.count
      });
    });
};
