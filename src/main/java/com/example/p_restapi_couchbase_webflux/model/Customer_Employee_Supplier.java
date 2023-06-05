package com.example.p_restapi_couchbase_webflux.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Customer_Employee_Supplier {
    private int id_ces;
    private String name;
    private String ein;
    private char type_ces;
    private String address;
    private String iban;
    private String ssn;
}
