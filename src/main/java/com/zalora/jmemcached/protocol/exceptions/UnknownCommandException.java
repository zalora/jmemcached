package com.zalora.jmemcached.protocol.exceptions;

/**
 * @author Ryan Daum
 */
public class UnknownCommandException extends ClientException {

    public UnknownCommandException() {}
    public UnknownCommandException(String s) { super(s); }
    public UnknownCommandException(String s, Throwable throwable) { super(s, throwable); }
    public UnknownCommandException(Throwable throwable) { super(throwable); }

}
