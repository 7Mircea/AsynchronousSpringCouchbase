package com.example.p_restapi_couchbase_webflux.repository;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.ReactiveBucket;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.ReactiveScope;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.example.p_restapi_couchbase_webflux.exceptions.RepositoryException;
import com.example.p_restapi_couchbase_webflux.model.Characteristic;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


@Repository
public class CharacteristicRepository {
    private ReactiveCluster cluster;
    private ReactiveBucket bucket;
    private ReactiveScope scope;
    private ReactiveCollection collection;
    private String COLLECTION_NAME = "characteristic";

    public CharacteristicRepository(ReactiveCluster cluster, ReactiveBucket bucket, ReactiveScope scope) {
        this.cluster = cluster;
        this.bucket = bucket;
        this.scope = scope;
        this.collection = scope.collection(COLLECTION_NAME);
    }

    /*
    simple query 2
     */
    public Flux<Characteristic> findCharacteristicByProduct(int idProd) {
        String query = "select " + COLLECTION_NAME + ".* from " + COLLECTION_NAME + " where " + COLLECTION_NAME + ".id_prod = " + idProd;
        try {
            Mono<ReactiveQueryResult> result = scope.query(query);
            return result.flux().flatMap(e -> e.rowsAs(Characteristic.class));
        } catch (DocumentNotFoundException ex) {
            throw new RepositoryException("Unable to locate documents with key before: " + idProd, ex);
        }
    }

}
