package com.example.p_restapi_couchbase_webflux.repository;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.ReactiveBucket;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.ReactiveScope;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.example.p_restapi_couchbase_webflux.exceptions.RepositoryException;
import com.example.p_restapi_couchbase_webflux.model.Products;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository
public class ProductsRepository {

    /*@Query(value = "select distinct on (SS.category) category, SS.Product_name, SS.Profit\n" +
            "from (select S.category, S.prod_name Product_name, (S.sales - C.costs) Profit\n" +
            "    from (select p.category category, p.prod_name prod_name, sum(i.quantity * i.unit_price) sales\n" +
            "        from item i inner join products p on i.id_prod = p.id_prod\n" +
            "            inner join invoice inv on inv.nr = i.invoice_nr and inv.invoice_date = i.invoice_date\n" +
            "            where inv.type = 'c'\n" +
            "            group by i.id_prod,p.prod_name,p.category) S \n" +
            "        inner join \n" +
            "        (select p.prod_name prod_name, sum(i.quantity * i.unit_price) costs\n" +
            "        from item i inner join products p on i.id_prod = p.id_prod\n" +
            "            inner join invoice f on f.nr = i.invoice_nr and f.invoice_date = i.invoice_date\n" +
            "            where f.type = 's'\n" +
            "            group by i.id_prod,p.prod_name) C \n" +
            "        on S.prod_name = C.prod_name \n" +
            "    order by S.prod_name) SS\n" +
            "where SS.Profit = (select Max((S.sales - C.costs)) Profit\n" +
            "                    from (select p.prod_name prod_name, p.category category, sum(i.quantity * i.unit_price) sales\n" +
            "                        from item i inner join products p on i.id_prod = p.id_prod\n" +
            "                            inner join invoice f on f.nr = i.invoice_nr and f.invoice_date = i.invoice_date\n" +
            "                            where f.type = 'c' and p.category = SS.category\n" +
            "                            group by i.id_prod,p.prod_name,p.category) S \n" +
            "                        inner join \n" +
            "                        (select p.prod_name prod_name, sum(i.quantity * i.unit_price) costs\n" +
            "                        from item i inner join products p on i.id_prod = p.id_prod\n" +
            "                            inner join invoice f on f.nr = i.invoice_nr and f.invoice_date = i.invoice_date\n" +
            "                            where f.type = 's' and p.category = SS.category\n" +
            "                            group by i.id_prod,p.prod_name) C \n" +
            "                        on S.prod_name = C.prod_name \n" +
            "                    group by S.category);",nativeQuery = true)
    Set<CategoryProductProfit> findProductWithGreatestProfitInCategory();

    @Query(value = "select S.category, sum(S.sales - C.costs) Profit\n" +
            "from (select p.category category, p.prod_name prod_name, sum(i.quantity * i.unit_price) sales\n" +
            "    from item i inner join products p on i.id_prod = p.id_prod\n" +
            "        inner join invoice f on f.nr = i.invoice_nr and f.invoice_date = i.invoice_date\n" +
            "        where f.type = 'c'\n" +
            "        group by i.id_prod, p.prod_name, p.category) S \n" +
            "    inner join \n" +
            "    (select p.prod_name prod_name, sum(i.quantity * i.unit_price) costs\n" +
            "    from item i inner join products p on i.id_prod = p.id_prod\n" +
            "        inner join invoice inv on inv.nr = i.invoice_nr and inv.invoice_date = i.invoice_date\n" +
            "        where inv.type = 's'\n" +
            "        group by i.id_prod,p.prod_name) C \n" +
            "    on S.prod_name = C.prod_name \n" +
            "group by category\n" +
            "order by category;",nativeQuery = true)
    Set<CategoryProfit> findProfitForEachCategory();

    Set<Products> findProductsByIdProdIsBefore(int N);

    interface CategoryProductProfit {
        String getCategory();
        String getProduct_Name();
        float getProfit();
    }

    interface CategoryProfit {
        String getCategory();
        float getProfit();
    }*/
    private ReactiveCluster cluster;
    private ReactiveBucket bucket;
    private ReactiveScope scope;
    private ReactiveCollection collection;
    private String COLLECTION_NAME = "products";

    public ProductsRepository(ReactiveCluster cluster, ReactiveBucket bucket, ReactiveScope scope) {
        this.cluster = cluster;
        this.bucket = bucket;
        this.scope = scope;
        this.collection = scope.collection(COLLECTION_NAME);
    }

    public Mono<String> findByIdAsString(String id) {
        try {
            Mono<GetResult> result = collection.get(id);
            return result.map(e -> e.contentAsObject().toString());
        } catch (DocumentNotFoundException ex) {
            throw new RepositoryException("Unable to locate document for key: " + id, ex);
        }
    }

    public Flux<Products> findProductsByIdProdIsBefore(int n) {
        String query = "select " + COLLECTION_NAME + ".* from " + COLLECTION_NAME + " where products.id_prod < " + n;
        try {
            Mono<ReactiveQueryResult> result = scope.query(query);
            return result.flux().flatMap(e -> e.rowsAs(Products.class));
        } catch (DocumentNotFoundException ex) {
            throw new RepositoryException("Unable to locate documents with key before: " + n, ex);
        }
    }

    public Flux<CategoryProductProfit> findProductWithGreatestProfitInCategory() {
        String query = "select distinct (SS.category) category, SS.Product_name, SS.Profit  \n" +
                "              from (select S.category, S.prod_name Product_name, (S.sales - C.costs) Profit  \n" +
                "                  from (select p.category category, p.prod_name prod_name, sum(i.quantity * i.unit_price) sales  \n" +
                "                      from item i inner join products p on i.id_prod = p.id_prod  \n" +
                "                          inner join invoice inv on inv.nr = i.invoice_nr and inv.invoice_date = i.invoice_date  \n" +
                "                          where inv.type = 'c'  \n" +
                "                          group by i.id_prod,p.prod_name,p.category) S   \n" +
                "                      inner join   \n" +
                "                      (select p.prod_name prod_name, sum(i.quantity * i.unit_price) costs  \n" +
                "                      from item i inner join products p on i.id_prod = p.id_prod  \n" +
                "                          inner join invoice f on f.nr = i.invoice_nr and f.invoice_date = i.invoice_date  \n" +
                "                          where f.type = 's'  \n" +
                "                          group by i.id_prod,p.prod_name) C   \n" +
                "                      on S.prod_name = C.prod_name   \n" +
                "                  order by S.prod_name) SS\n" +
                "where SS.Profit = (select Max((S.sales - C.costs)) as Profit\n" +
                "            from (select p.prod_name as prod_name, p.category as category, sum(i.quantity * i.unit_price) as sales  \n" +
                "            from item i inner join products p on i.id_prod = p.id_prod  \n" +
                "            inner join invoice f on f.nr = i.invoice_nr and f.invoice_date = i.invoice_date  \n" +
                "            where f.type = 'c' and p.category = SS.category  \n" +
                "            group by i.id_prod,p.prod_name,p.category) S   \n" +
                "            inner join   \n" +
                "            (select p.prod_name prod_name, sum(i.quantity * i.unit_price) costs  \n" +
                "            from item i inner join products p on i.id_prod = p.id_prod  \n" +
                "            inner join invoice f on f.nr = i.invoice_nr and f.invoice_date = i.invoice_date  \n" +
                "            where f.type = 's' and p.category = SS.category  \n" +
                "            group by i.id_prod,p.prod_name) as C\n" +
                " on S.prod_name = C.prod_name group by S.category)";
        try {
            Mono<ReactiveQueryResult> result = scope.query(query);
            return result.flux().flatMap(e->e.rowsAs(CategoryProductProfit.class));
        } catch (DocumentNotFoundException ex) {
            throw new RepositoryException("Unable to locate documents with key before: ", ex);
        }
    }

    public Flux<CategoryProfit> findProfitForEachCategory() {
        String query = "select S.category as category, sum(S.sales - C.costs) as profit  \n" +
                "              from (select p.category as category, p.prod_name as prod_name, sum(i.quantity * i.unit_price) as sales  \n" +
                "                  from item i inner join products p on i.id_prod = p.id_prod  \n" +
                "                      inner join invoice f on f.nr = i.invoice_nr and f.invoice_date = i.invoice_date  \n" +
                "                      where f.type = 'c'  \n" +
                "                      group by i.id_prod, p.prod_name, p.category) S   \n" +
                "                  inner join   \n" +
                "                  (select p.prod_name as prod_name, sum(i.quantity * i.unit_price) as costs  \n" +
                "                  from item i inner join products p on i.id_prod = p.id_prod  \n" +
                "                      inner join invoice inv on inv.nr = i.invoice_nr and inv.invoice_date = i.invoice_date  \n" +
                "                      where inv.type = 's'  \n" +
                "                      group by i.id_prod,p.prod_name) C   \n" +
                "                  on S.prod_name = C.prod_name   \n" +
                "              group by S.category  \n" +
                "              order by S.category;";
        try {
            Mono<ReactiveQueryResult> result = scope.query(query);
            return result.flux().flatMap(e->e.rowsAs(CategoryProfit.class));
        } catch (DocumentNotFoundException ex) {
            throw new RepositoryException("Unable to locate documents with key before: ", ex);
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CategoryProfit {
        private String category;
        private float profit;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CategoryProductProfit {
        private String category;
        private String product_name;
        private float profit;
    }
}
