package com.raverun.match.server.impl;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.raverun.match.server.api.MatchDaemon;
import com.raverun.match.server.api.Poller;
import com.raverun.match.server.api.PollerFactory;
import com.raverun.match.server.api.RetryPolicy;
import com.raverun.shared.services.DbLogger;
import com.raverun.shared.services.IntegrationType;
import com.raverun.shared.services.OperationType;


public class DefaultMatchDaemon implements MatchDaemon
{
    @Inject
    public DefaultMatchDaemon( PollerFactory factory, RetryPolicy retryPolicy, DbLogger dbLog )
    {
        this.retryPolicy = retryPolicy;
        this.factory     = factory;
        this.dbLog       = dbLog;
    }

    public void join() throws InterruptedException
    {
        thread.join();
    }

    public void start()
    {
        thread = new Thread( this );
        thread.setName( "matching daemon" );
        thread.start();
    }

    public void stop()
    {
        running = false;
        thread.interrupt();
    }

    public void run()
    {
        long pollerCount = 1;
        int backoff = 500; // milliseconds

        while( running )
        {
            Poller poller = null;
            
            try
            {
                Thread.sleep( backoff );
                poller = factory.create( pollerCount );
                poller.start();
                poller.join();
            }
            catch( Throwable t )
            {
                logger.error( t );
            }
            finally
            {
                try
                {
                    if( poller != null )
                    {
                        poller.stop();
                    }
                }
                catch( Throwable ignored ) {}
            }

            if( !retryPolicy.retry( ++pollerCount ) )
                break;

            backoff = retryPolicy.newBackoff( backoff );
            logger.info( "new backoff: " + backoff );
        }

        if( retryPolicy.mustAlert() )
            ; // TODO alert via email

        dbLog.log( IntegrationType.RABBITMQ, OperationType.MATCH_DAEMON, "matcher-server", "Superdaemon for the matching process has died." );
    }
    
    private final PollerFactory factory;
    private final RetryPolicy retryPolicy;
    private final DbLogger dbLog;
    
    private volatile boolean running = true;
    private volatile Thread thread;

    private static final Logger logger = Logger.getLogger( DefaultMatchDaemon.class );
}
