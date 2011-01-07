package com.raverun.match.server;

import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.Stage;
import com.google.inject.assistedinject.FactoryProvider;
import com.google.inject.name.Names;
import com.raverun.match.api.MatchHandler;
import com.raverun.match.logic.MatchHandlerImpl;
import com.raverun.match.server.api.MatchDaemon;
import com.raverun.match.server.api.MatcherRunnableFactory;
import com.raverun.match.server.api.PollerFactory;
import com.raverun.match.server.api.RetryPolicy;
import com.raverun.match.server.db.DbConnectionProvider;
import com.raverun.match.server.impl.MatcherRunnable;
import com.raverun.match.server.impl.DefaultMatchDaemon;
import com.raverun.match.server.impl.DefaultPoller;
import com.raverun.match.server.impl.DefaultRetryPolicy;
import com.raverun.match.server.impl.ExecutorServiceProvider;
import com.raverun.queue.QueueConnectionProvider;
import com.raverun.shared.Configuration;
import com.raverun.shared.Obfuscator;
import com.raverun.shared.StageMapper;
import com.raverun.shared.impl.DbConfiguration;
import com.raverun.shared.impl.FileConfiguration;
import com.raverun.shared.impl.ObfuscatorJasypt;
import com.raverun.shared.impl.ObfuscatorRot47;
import com.raverun.shared.impl.StageMapperImpl;
import com.raverun.shared.persistence.DataSourceProvider;
import com.raverun.shared.services.DbLogger;
import com.raverun.shared.services.impl.DbLoggerImpl;
import com.rabbitmq.client.Connection;

public class GuiceModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bindConstant().annotatedWith( Names.named( "filename" ) ).to( path );

        bindConstant().annotatedWith( Names.named( "stage" ) ).to( Stage.PRODUCTION.toString() );

        bind( Obfuscator.class ).annotatedWith( Names.named("rot47") ).
            to( ObfuscatorRot47.class ).in( Scopes.SINGLETON );

        bind( Obfuscator.class ).annotatedWith( Names.named("jasypt") ).
            to( ObfuscatorJasypt.class ).in( Scopes.SINGLETON );

        bind( StageMapper.class ).to( StageMapperImpl.class ).in(
            Scopes.SINGLETON );

        switch( ct )
        {
        case DB:
            bind( Configuration.class ).to( DbConfiguration.class ).in(
                Scopes.SINGLETON );
            break;
        case FILE:
            bind( Configuration.class ).to( FileConfiguration.class ).in(
                Scopes.SINGLETON );
            break;
        }

        bind( DataSource.class ).toProvider( DataSourceProvider.class ).in( Scopes.SINGLETON );

        bind( RetryPolicy.class ).to( DefaultRetryPolicy.class ).in( Scopes.SINGLETON );

        bind( Connection.class ).toProvider( QueueConnectionProvider.class );

        bind( ExecutorService.class ).toProvider( ExecutorServiceProvider.class );

        bind( java.sql.Connection.class ).toProvider( DbConnectionProvider.class );

        bind( DbLogger.class ).to( DbLoggerImpl.class ).in( Scopes.SINGLETON );

        bind( MatchHandler.class ).to( MatchHandlerImpl.class ).in( Scopes.SINGLETON );

        bind( PollerFactory.class ).toProvider(
            FactoryProvider.newFactory( PollerFactory.class,
                DefaultPoller.class ) );

        bind( MatcherRunnableFactory.class ).toProvider(
            FactoryProvider.newFactory( MatcherRunnableFactory.class,
                MatcherRunnable.class ) );

        bind( MatchDaemon.class ).to( DefaultMatchDaemon.class ).in( Scopes.SINGLETON );

        logger.info( "Guice configured successfully" );
    }

    public GuiceModule( ConfigurationType ct, String path )
    {
        this.ct = ct;
        this.path = path;
    }

    public static enum ConfigurationType
    {
        DB, FILE
    }

    private static final Logger logger = Logger.getLogger( GuiceModule.class );
    
    private final ConfigurationType ct;
    private final String path;
}
