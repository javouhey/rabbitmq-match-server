package com.raverun.match.server.api;

public interface RetryPolicy
{
    boolean retry( long numberOfExecutions );

    boolean mustAlert();

    /**
     * Uses properties in {@code daemon.properties}
     * <ul>
     * <li>daemon.backoff.multiplier
     * <li>daemon.backoff.max.millisecond
     * </ul>
     * 
     * @param currentBackoff - duration in milliseconds for backoff
     * @return
     */
    int newBackoff( int currentBackoff );
}
