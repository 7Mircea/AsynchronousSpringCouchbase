package com.example.p_restapi_couchbase_webflux.config;

import com.couchbase.client.java.*;
import com.couchbase.client.java.env.ClusterEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class Config {
    @Bean
    public Cluster cluster(@Value("${couchbase.clusterHost}") String hostname, @Value("${couchbase.username}") String username,
                                   @Value("${couchbase.password}") String password) {
        ClusterEnvironment.Builder environmentBuilder = ClusterEnvironment.builder();
        environmentBuilder.timeoutConfig().queryTimeout(Duration.ofMinutes(20)).build();
        ClusterOptions options = ClusterOptions.clusterOptions(username,password).environment(environmentBuilder.build());
        return Cluster.connect(hostname,options);
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
