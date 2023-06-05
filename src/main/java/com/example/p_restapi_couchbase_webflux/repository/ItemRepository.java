package com.example.p_restapi_couchbase_webflux.repository;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.example.p_restapi_couchbase_webflux.exceptions.RepositoryException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class ItemRepository {
    /*@Query(value = "select p.prod_name,idProdQ.quantity from products p inner join (select i.id_prod id_prod,sum(i.quantity) quantity\n" +
            "from item i inner join invoice on i.invoice_nr = invoice.nr and i.invoice_Date = invoice.invoice_Date\n" +
            "where EXTRACT(month from invoice.invoice_Date) = 10 and EXTRACT(year from invoice.invoice_Date) = 2022 and invoice.type='c'\n" +
            "group by i.id_prod) idProdQ on p.id_prod=idProdQ.id_prod;",
            nativeQuery = true)
    Set<ProdQuantity1> findItemsBetweenDates();

    */

    private ReactiveCluster cluster;
    private ReactiveBucket bucket;
    private ReactiveScope scope;
    private ReactiveCollection collection;
    private String COLLECTION_NAME = "item";

    public ItemRepository(ReactiveCluster cluster, ReactiveBucket bucket, ReactiveScope scope) {
        this.cluster = cluster;
        this.bucket = bucket;
        this.scope = scope;
        this.collection = scope.collection(COLLECTION_NAME);
    }

    /**
     * Simple Query 4
     * @return
     */
    public Flux<ProdQuantity1> findItemsBetweenDates() {
        String query = "select p.prod_name, idProdQ.quantity from products p inner join (select i.id_prod as id_prod,sum(i.quantity) quantity\n" +
                "from " + COLLECTION_NAME + " i inner join invoice inv on i.invoice_nr = inv.nr and i.invoice_date = inv.invoice_date\n" +
                "where DATE_PART_STR(inv.invoice_date, 'month') = 10 and DATE_PART_STR(inv.invoice_date,'year') = 2022 and inv.type='c'\n" +
                "group by i.id_prod) idProdQ on p.id_prod=idProdQ.id_prod;";
        try {
            Mono<ReactiveQueryResult> result = scope.query(query);
            return result.flux().flatMap(e ->e.rowsAs(ProdQuantity1.class));
        } catch (DocumentNotFoundException ex) {
            throw new RepositoryException(" unable to find max value in invoice", ex);
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProdQuantity1 {
        String prod_name;
        int quantity;
    }
}
