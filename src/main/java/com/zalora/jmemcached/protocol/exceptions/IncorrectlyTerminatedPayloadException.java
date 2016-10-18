package com.zalora.jmemcached.protocol.exceptions;

/**
 * @author Ryan Daum
 */
public class IncorrectlyTerminatedPayloadException extends ClientException {

    public IncorrectlyTerminatedPayloadException() {}
    public IncorrectlyTerminatedPayloadException(String s) { super(s); }
    public IncorrectlyTerminatedPayloadException(String s, Throwable throwable) { super(s, throwable); }
    public IncorrectlyTerminatedPayloadException(Throwable throwable) { super(throwable); }

}
