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

import java.util.List;

@Repository
public class InvoiceRepository {// extends JpaRepository<Invoice, InvoiceId> {
    /*@Query(value = "select max(value) from invoice;",nativeQuery = true)
    public Float findMaxValue();

    @Query(value="select S.sales sales, Ch.costs costs,S.month month_,S.year year_\n" +
            "from (select SUM(value) as sales,\n" +
            "        EXTRACT(month from invoice_date) as month,\n" +
            "        EXTRACT(year from invoice_date) as year \n" +
            "        from invoice \n" +
            "        where type='c'\n" +
            "        group by EXTRACT(month from invoice_date),EXTRACT(year from invoice_date)) S\n" +
            "    full join (select SUM(value) as costs,\n" +
            "        EXTRACT(month from invoice_date) as month,\n" +
            "        EXTRACT(year from invoice_date) as year\n" +
            "        from invoice \n" +
            "        where type='s'\n" +
            "        group by EXTRACT(month from invoice_date), EXTRACT(year from invoice_date)) Ch\n" +
            "    on S.month = Ch.month and S.year=Ch.year\n" +
            "    order by S.year, S.month;",nativeQuery = true)
    public Set<SalesCostMonthYear> findSalesCostMonthYear();

    @Query(value="select S.prod_name, (S.sales - C.costs) Profit\n" +
            "from (select p.prod_name prod_name, sum(i.quantity * i.unit_price) sales\n" +
            "    from item i inner join products p on i.id_prod = p.id_prod\n" +
            "        inner join invoice inv on inv.nr = i.invoice_nr and inv.invoice_date = i.invoice_date\n" +
            "        where inv.type = 'c'\n" +
            "        group by i.id_prod,p.prod_name) S \n" +
            "    inner join \n" +
            "    (select p.prod_name prod_name, sum(i.quantity * i.unit_price) costs\n" +
            "    from item i inner join products p on i.id_prod = p.id_prod\n" +
            "        inner join invoice inv on inv.nr = i.invoice_nr and inv.invoice_date = i.invoice_date\n" +
            "        where inv.type = 's'\n" +
            "        group by i.id_prod,p.prod_name) C \n" +
            "    on S.prod_name = C.prod_name \n" +
            "order by S.prod_name;",nativeQuery = true)
    public Set<ProfitOnEachProduct> findProfitOnEachProduct();

    @Query(value = "select S.month, S.sales \n" +
            "from (select SUM(value) as sales,\n" +
            "            EXTRACT(month from invoice_date) as month,\n" +
            "            EXTRACT(year from invoice_date) as year\n" +
            "        from invoice \n" +
            "        where type='c'\n" +
            "        group by EXTRACT(month from invoice_date),EXTRACT(year from invoice_date)) S\n" +
            "    left join (select SUM(value) as sales,\n" +
            "            EXTRACT(month from invoice_date) as month,\n" +
            "            EXTRACT(year from invoice_date) as year\n" +
            "        from invoice \n" +
            "        where type='c'\n" +
            "        group by EXTRACT(month from invoice_date),EXTRACT(year from invoice_date)) S2\n" +
            "on S.sales < S2.sales\n" +
            "where S2.month is null;",nativeQuery = true)
    Set<MonthSale> getMonthWithGreatestSales();

    interface SalesCostMonthYear {
        float getSales();
        float getCosts();
        int getMonth_();
        int getYear_();
    }

    interface ProfitOnEachProduct {
        String getProd_Name();
        float getProfit();
    }

    interface MonthSale {
        String getMonth();
        float getSales();
    }*/

    private ReactiveCluster cluster;
    private ReactiveBucket bucket;
    private ReactiveScope scope;
    private ReactiveCollection collection;
    private  String COLLECTION_NAME = "invoice";

    public InvoiceRepository(ReactiveCluster cluster, ReactiveBucket bucket, ReactiveScope scope) {
        this.cluster = cluster;
        this.bucket = bucket;
        this.scope = scope;
        this.collection = scope.collection(COLLECTION_NAME);
    }

    public Flux<Float> findMaxValue(){
        String query =  "select max("+COLLECTION_NAME+".value_) as v from "+COLLECTION_NAME;
        try {
            Mono<ReactiveQueryResult> result = scope.query(query);
            Flux<FloatWrapper> answerList = result.flux().flatMap(e -> e.rowsAs(FloatWrapper.class));
            return answerList.map(e -> e.v);
        } catch (DocumentNotFoundException ex) {
            throw new RepositoryException(" unable to find max value in invoice" , ex);
        }
    }
    public Flux<SalesCostMonthYear> findSalesCostMonthYear() {
        String query =  "select S.sales as sales, Ch.costs as costs, S.month as month_, S.year as year_ from \n" +
                "(select SUM(value_) as sales,\n" +
                "DATE_PART_STR(invoice_date,'month') as month,\n" +
                "DATE_PART_STR(invoice_date,'year') as year \n" +
                "from invoice \n" +
                "where type='c'\n" +
                "group by DATE_PART_STR(invoice_date,'month'), DATE_PART_STR(invoice_date,'year')) as S\n" +
                "join\n" +
                "(select SUM(value_) as costs,\n" +
                "DATE_PART_STR(invoice_date,'month') as month,\n" +
                "DATE_PART_STR(invoice_date,'year') as year \n" +
                "from invoice \n" +
                "where type='s'\n" +
                "group by DATE_PART_STR(invoice_date,'month'),DATE_PART_STR(invoice_date,'year')) as Ch\n" +
                "on S.month = Ch.month and S.year=Ch.year\n" +
                " order by S.year, S.month;";
        try {
            Mono<ReactiveQueryResult> result = scope.query(query);
            return result.flux().flatMap(e->e.rowsAs(SalesCostMonthYear.class));
        } catch (DocumentNotFoundException ex) {
            throw new RepositoryException(" unable to find max value in invoice" , ex);
        }
    }

    /**
     * Complex Q3
     * @return
     */
    public Flux<ProfitOnEachProduct> findProfitOnEachProduct() {
        String query =  "select S.prod_name, (S.sales - C.costs) profit from \n" +
                "(select p.prod_name as prod_name, sum(i.quantity * i.unit_price) as sales\n" +
                "from item as i inner join products as p on i.id_prod = p.id_prod\n" +
                "inner join invoice inv on inv.nr = i.invoice_nr and inv.invoice_date = i.invoice_date\n" +
                "where inv.type = 'c'\n" +
                "group by i.id_prod,p.prod_name) as S\n" +
                "inner join\n" +
                "(select p.prod_name as prod_name, sum(i.quantity * i.unit_price) as costs\n" +
                "from item as i inner join products as p on i.id_prod = p.id_prod\n" +
                "inner join invoice inv on inv.nr = i.invoice_nr and inv.invoice_date = i.invoice_date\n" +
                "where inv.type = 's'\n" +
                "group by i.id_prod,p.prod_name) as C\n" +
                "on S.prod_name = C.prod_name\n" +
                "order by S.prod_name;";
        try {
            Mono<ReactiveQueryResult> result = scope.query(query);
            return result.flux().flatMap(e->e.rowsAs(ProfitOnEachProduct.class)) ;
        } catch (DocumentNotFoundException ex) {
            throw new RepositoryException(" unable to find max value in invoice" , ex);
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    private static class FloatWrapper {
        private Float v;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SalesCostMonthYear {
        private float sales;
        private float costs;
        private int year_;
        private int month_;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProfitOnEachProduct {

        private float profit;
        private String prod_name;
    }
}
