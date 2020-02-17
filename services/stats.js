const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    // TODO Compter le nombre d'anomalies par arondissement
    client.search({
        index: indexName,
        size: 0,
        body: {
            aggs: {
                arrondissements: {
                    terms: {
                        field: 'arrondissement.keyword',
                        size: 20
                    }
                }
            }
        }
    })
    .then(resp => {
        
        let res = resp.body.aggregations.arrondissements.buckets.map(elem => {
            return {
                arrondissement: elem.key,
                count: elem.doc_count
            }
        })
        callback(res);
    })
    
}

exports.statsByType = (client, callback) => {
    // TODO Trouver le top 5 des types et sous types d'anomalies
    client.search({
        index: indexName,
        size: 0,
        body: {
            aggs: {
                types: {
                    terms: {
                        field: 'type.keyword',
                        size: 5
                    },
                    aggs: {
                        soustypes: {
                            terms: {
                                field: 'sous_type.keyword',
                                size: 5
                            }
                        }
                    }
                }
            }
        }
    })
    .then(resp => {
        let res = resp.body.aggregations.types.buckets.map(elem => {
            return {
                type: elem.key,
                count: elem.doc_count,
                sous_types: elem.soustypes.buckets.map(sstype => {
                    return {
                        sous_type: sstype.key,
                        count: sstype.doc_count
                    }
                })
            }
        });
        callback(res);
    })
}

exports.statsByMonth = (client, callback) => {
    client.search({
        index: indexName,
        size: 0,
        body: {
            aggs: {
                month_year: {
                    terms: {
                        field: 'month_year.keyword',
                        size: 10
                    }
                }
            }
        }
    })
    .then(resp => {
        
        let res = resp.body.aggregations.month_year.buckets.map(elem => {
            return {
                month: elem.key,
                count: elem.doc_count
            }
        })
        callback(res);
    })
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    client.search({
        index: indexName,
        size: 0,
        body: {
            query: {
                bool: { 
                    must: {
                        match: { type:   "Propreté" }
                    },
                }
            },
            aggs: {
                arrondissements: {
                    terms: {
                        field: 'arrondissement.keyword',
                        size: 3
                    }
                }
            }
        }
    }).then(resp => {
        callback(resp.body.aggregations.arrondissements.buckets.map(elem => {
            return {
                arrondissement: elem.key,
                count: elem.doc_count
            }
        }));
    })
    
}
