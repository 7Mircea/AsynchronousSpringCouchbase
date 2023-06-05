package com.example.p_restapi_couchbase_webflux.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Characteristic {
    private int id_prod;
    private int id_characteristic;
    private String name;
    private String value_;

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Characteristic : ")
                .append(id_prod)
                .append(',')
                .append(id_characteristic)
                .append(',')
                .append(name)
                .append(',')
                .append(value_)
                .append('\n')
                .toString();
    }
}


