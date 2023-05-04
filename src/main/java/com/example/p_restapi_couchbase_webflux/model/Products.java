package com.example.p_restapi_couchbase_webflux.model;

import lombok.*;

import java.util.Objects;

@Setter
@Getter
@RequiredArgsConstructor
@AllArgsConstructor
public class Products {
    private Integer id_prod; //NUMBER(10) in Oracle
    private String prod_name;
    private Integer id_supplier;
    private String availability;
    private String category;
    private String add_info;


    public Products(Integer idProd) {
        this.id_prod = idProd;
    }

    @Override
    public int hashCode() {
        return id_prod;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!obj.getClass().getName().equals(this.getClass().getName())) return false;
        return Objects.equals(id_prod, ((Products) obj).id_prod);
    }
}
