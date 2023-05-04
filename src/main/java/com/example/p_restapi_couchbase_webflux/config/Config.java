package com.example.p_restapi_couchbase_webflux.config;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveBucket;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {
    @Bean
    public Cluster cluster(@Value("${couchbase.clusterHost}") String hostname, @Value("${couchbase.username}") String username,
                                   @Value("${couchbase.password}") String password) {
        return Cluster.connect(hostname, username, password);
    }

    @Bean
    public ReactiveCluster reactiveCluster(Cluster cluster) {
        return cluster.reactive();
    }


    @Bean
    public ReactiveBucket bucket(ReactiveCluster reactiveCluster, @Value("${couchbase.bucket}") String bucketName) {
        return reactiveCluster.bucket(bucketName);
    }

    @Bean
    public ReactiveScope scope(ReactiveBucket reactiveBucket, @Value("${couchbase.scope}") String scopeString) {
        return reactiveBucket.scope(scopeString);
    }

}
