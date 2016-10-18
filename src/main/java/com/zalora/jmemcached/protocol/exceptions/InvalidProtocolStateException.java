package com.zalora.jmemcached.protocol.exceptions;

/**
 * @author Ryan Daum
 */
public class InvalidProtocolStateException extends Exception {

    public InvalidProtocolStateException() {}
    public InvalidProtocolStateException(String s) { super(s); }
    public InvalidProtocolStateException(String s, Throwable throwable) { super(s, throwable); }
    public InvalidProtocolStateException(Throwable throwable) { super(throwable); }

}
