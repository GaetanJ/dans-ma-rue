const config = require("config");
const csv = require("csv-parser");
const fs = require("fs");
const { Client } = require("@elastic/elasticsearch");
const indexName = config.get("elasticsearch.index_name");
const _ = require("lodash");

async function run() {
  // Create Elasticsearch client
  const client = new Client({ node: config.get("elasticsearch.uri") });
  let dataset = [];

  client.indices.create({ index: "dans_ma_rue" }, (err, resp) => {
    if (err) console.trace(err.message);
  });

  // TODO il y a peut être des choses à faire ici avant de commencer ...

  // Read CSV file
  fs.createReadStream("dataset/dans-ma-rue.csv")
    .pipe(
      csv({
        separator: ";"
      })
    )
    .on("data", data => {
      // TODO ici on récupère les lignes du CSV ...
      dataset.push({
        "@timestamp": data.DATEDECL,
        object_id: data.OBJECTID,
        annee_declaration: data["ANNEE DECLARATION"],
        mois_declaration: data["MOIS DECLARATION"],
        type: data.TYPE,
        sous_type: data.SOUSTYPE,
        code_postal: data.CODE_POSTAL,
        ville: data.VILLE,
        arrondissement: data.ARRONDISSEMENT,
        prefixe: data.PREFIXE,
        intervenant: data.INTERVENANT,
        conseil_de_quartier: data["CONSEIL DE QUARTIER"],
        location: JSON.parse(data.geo_shape).coordinates.toString()
      });
    })
    .on("end", () => {
      // TODO il y a peut être des choses à faire à la fin aussi ?
      dataset = _.chunk(dataset, 5000);
      dataset.forEach(subArray => {
        client.bulk(createBulkInsertQuery(subArray), (err, resp) => {
          if (err) console.trace(err.message);
          else console.log(`Inserted ${resp.body.items.length} anomaly`);
          client.close();
        });
      });

      console.log("Terminated!");
    });
}

function createBulkInsertQuery(anomalies) {
  const body = anomalies.reduce((acc, anomaly) => {
    acc.push({
      index: { _index: indexName, _type: "_doc", _id: anomaly.object_id }
    });
    acc.push(anomaly);
    return acc;
  }, []);

  return { body };
}

run().catch(console.error);
