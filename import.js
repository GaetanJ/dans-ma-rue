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

  await client.indices.create(
    {
      index: indexName
    },
    (err, resp) => {
      if (err) console.trace(err.message);
    }
  );

  await client.indices.putMapping({
    index: indexName,
    body: {
      properties: {
        location: {
          type: "geo_point",
          fields: {
            raw_location: {
              type: "keyword"
            }
          }
        }
      }
    }
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
        location: data.geo_point_2d,
        month_year: data["MOIS DECLARATION"] + "/" + data["ANNEE DECLARATION"]
      });
    })
    .on("end", async () => {
      // TODO il y a peut être des choses à faire à la fin aussi ?
      dataset = _.chunk(dataset, 20000);

      for (let i = 0; i < dataset.length; i++) {
        try {
          await client.bulk(createBulkInsertQuery(dataset[i]));
        } catch {
          console.log("errors");
        }
      }
      client.close();
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
