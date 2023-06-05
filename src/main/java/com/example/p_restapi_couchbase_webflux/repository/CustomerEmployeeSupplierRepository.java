package com.example.p_restapi_couchbase_webflux.repository;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.example.p_restapi_couchbase_webflux.exceptions.RepositoryException;
import com.example.p_restapi_couchbase_webflux.model.Customer_Employee_Supplier;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class CustomerEmployeeSupplierRepository {
    /*Set<CustomerEmployeeSupplier> findCustomerEmployeeSupplierByTypeCES(char c);

    @Query(value = "select CES.Name\n" +
            "from (select count(*) invoice_number, id_employee\n" +
            "    from invoice\n" +
            "    group by id_employee) S inner join CUSTOMER_EMPLOYEE_SUPPLIER CES on S.id_employee = CES.id_ces\n" +
            "where invoice_number = (select max(S2.invoice_number) \n" +
            "                    from (select count(*) invoice_number, id_employee\n" +
            "                        from invoice\n" +
            "                        group by id_employee) S2);\n", nativeQuery = true)
    Set<String> findEmployeeWithGreatestNrOfInvoices();*/

    private ReactiveCluster cluster;
    private ReactiveBucket bucket;
    private ReactiveScope scope;
    private ReactiveCollection collection;
    private String COLLECTION_NAME = "customer_employee_supplier";

    public CustomerEmployeeSupplierRepository(ReactiveCluster cluster, ReactiveBucket bucket, ReactiveScope scope) {
        this.cluster = cluster;
        this.bucket = bucket;
        this.scope = scope;
        this.collection = scope.collection(COLLECTION_NAME);
    }

    public Flux<Customer_Employee_Supplier> findCustomerEmployeeSupplierByTypeCES(char c) {
        String query =  "select "+COLLECTION_NAME+".* from "+COLLECTION_NAME+" where "+COLLECTION_NAME+".type_ces=\'"+c + '\'';
        try {
            Mono<ReactiveQueryResult> result = scope.query(query);
            return result.flux().flatMap(e->e.rowsAs(Customer_Employee_Supplier.class));//rowsAs(Customer_Employee_Supplier.class);
        } catch (DocumentNotFoundException ex) {
            throw new RepositoryException("Unable to locate documents with key before: " + c, ex);
        }
    }
}
