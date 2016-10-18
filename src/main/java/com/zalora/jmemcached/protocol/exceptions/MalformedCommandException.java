package com.zalora.jmemcached.protocol.exceptions;

/**
 * @author Ryan Daum
 */
public class MalformedCommandException extends ClientException {

    public MalformedCommandException() {}
    public MalformedCommandException(String s) { super(s); }
    public MalformedCommandException(String s, Throwable throwable) { super(s, throwable); }
    public MalformedCommandException(Throwable throwable) { super(throwable); }

}
