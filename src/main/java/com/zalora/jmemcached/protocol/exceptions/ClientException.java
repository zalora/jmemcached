package com.zalora.jmemcached.protocol.exceptions;

/**
 * @author Ryan Daum
 */
public class ClientException extends Exception {

    public ClientException() {}
    public ClientException(String s) { super(s); }
    public ClientException(String s, Throwable throwable) { super(s, throwable); }
    public ClientException(Throwable throwable) { super(throwable); }

}
