package com.example.p_restapi_couchbase_webflux.exceptions;

public class RepositoryException extends RuntimeException{

    public RepositoryException(String message) {
        super(message);
    }

    public RepositoryException(Throwable t) {
        super(t);
    }

    public RepositoryException(String message, Throwable t) {
        super(message, t);
    }
}
