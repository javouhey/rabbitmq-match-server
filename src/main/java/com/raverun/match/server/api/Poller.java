package com.raverun.match.server.api;

/**
 * Its responsibilities are as follows:
 * <ul>
 * <li>retrieves a Connection to rabbitmq. If it fails, terminates
 * <li>Otherwise, starts a polling loop.
 * <li>wake up every few milliseconds to poll the Channel for messages.
 * <li>If there is a message, submit task to the threadpool that will start the matching process
 * <ul>
 * @author Gavin Bong
 */ 
public interface Poller extends Runnable
{
    void start();
    
    void stop();
    
    /**
     * @throws IllegalStateException if {@code start} was not invoked first
     * @throws InterruptedException
     */
    void join() throws InterruptedException;
}
