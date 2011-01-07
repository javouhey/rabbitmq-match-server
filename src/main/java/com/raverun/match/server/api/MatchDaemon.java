package com.raverun.match.server.api;

/**
 * A {@code MatchHandler} subscribes to requests for 
 * matching (by reading from a rabbitMq queue)
 * 
 * @author Gavin Bong
 */
public interface MatchDaemon extends Runnable
{
    void start();
    
    void stop();
    
    void join() throws InterruptedException;
}
