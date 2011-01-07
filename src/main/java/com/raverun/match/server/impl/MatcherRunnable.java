package com.raverun.match.server.impl;

import java.sql.Connection;
import java.sql.SQLWarning;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.google.inject.Provider;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.raverun.match.api.MatchHandler;
import com.raverun.match.api.MatchParam;
import com.raverun.match.server.db.ConnectionPool;

public class MatcherRunnable implements Runnable
{
    @AssistedInject
    public MatcherRunnable( Provider<DataSource> dsProvider, MatchHandler matchHandler, @Assisted MatchParam param )
    {
        this.matchHandler = matchHandler;
        this.dsProvider = dsProvider;
        this.param = param;
    }

    @SuppressWarnings("unused")
    public void run()
    {
        DataSource dataSource = dsProvider.get();
        logger.info( toLogPrefix() + "start process" );
        matchHandler.process( param );
        logger.info( toLogPrefix() + "finished" );
    }

    private final String toLogPrefix()
    {
        StringBuilder builder = new StringBuilder();
        builder.append( this.getClass().getSimpleName() ).append( " => " );
        builder.append( param.toString() ).append( "... " );

        return builder.toString();
    }
    
    private final MatchParam param;
    private final Provider<DataSource> dsProvider;
    private final MatchHandler matchHandler;
    
    private static final Logger logger = Logger.getLogger( MatcherRunnable.class );
}
