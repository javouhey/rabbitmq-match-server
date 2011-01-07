package com.raverun.match.server.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.google.inject.Provider;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.raverun.match.api.MatchParam;
import com.raverun.match.server.api.MatcherRunnableFactory;
import com.raverun.match.server.api.Poller;
import com.raverun.queue.NullQueueConnection;
import com.raverun.shared.Common;
import com.raverun.shared.Configuration;
import com.raverun.shared.StageMapper;
import com.raverun.shared.services.DbLogger;
import com.raverun.shared.services.IntegrationType;
import com.raverun.shared.services.OperationType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Gavin Bong
 */
public class DefaultPoller implements Poller
{
    @AssistedInject
    public DefaultPoller( Provider<Connection> queueConnectionProvider,
        Configuration config, StageMapper mapper, DbLogger dbLog, Provider<ExecutorService> executorProvider,
        MatcherRunnableFactory runnableFactory, @Assisted long id )
    {
        this.runnableFactory = runnableFactory;
        this.queueConnectionProvider = queueConnectionProvider;
        this.config = config;
        this.mapper = mapper;
        this.dbLog = dbLog;
        this.executorProvider = executorProvider;
        this.id = id;

        pollFrequency = config.i( DefaultPoller.KEY_POLL_FREQUENCY_MS, 2000 );
        logger.info( "Polling frequency (ms): " + pollFrequency );
    }


    public void join() throws InterruptedException
    {
        if( thread == null )
            throw new IllegalStateException( "start() has not been called" );

        thread.join();
    }

    public void start()
    {
        thread = new Thread( this );
        thread.setName( "Match Poller( " + id + " )" );
        thread.start();
    }

    public void stop()
    {
        running = false;
        if( thread != null )
        {
            logger.info( "interrupting Poller" );
            thread.interrupt();
        }
    }

    public void run()
    {
        Connection connection = null;
        Channel channel = null;

        ExecutorService executor = executorProvider.get();

        try
        {
            final String stagePostfix = mapper.toCanonicalStage();
            
            connection = queueConnectionProvider.get();
            if( connection == null || connection instanceof NullQueueConnection )
            {
                dbLog.log( IntegrationType.MATCH, OperationType.CONNECT_TO_Q, DAEMON + id + ")" , "Cannot connect to queue on host " + config.s( Configuration.KEY_RABBITMQ_SERVER + stagePostfix ) +
                    " at port " + config.i( Configuration.KEY_RABBITMQ_PORT + stagePostfix, DEFAULT_PORT )  );
                logger.error( "Cannot connect to queue" );
                return;
            }

            final String EXCHANGE = config.s( KEY_EXCHANGE );
            final String QUEUE = config.s( KEY_QUEUENAME );

            ChannelWrapper tuple = getChannelFor( connection, EXCHANGE, QUEUE );
            channel = tuple.channel;

            logger.info( "Subscriber connected to " + config.s( Configuration.KEY_RABBITMQ_SERVER + stagePostfix ) +
                " at port " + config.i( Configuration.KEY_RABBITMQ_PORT + stagePostfix, DEFAULT_PORT ) );


            dbLog.log( IntegrationType.MATCH, OperationType.CONNECT_TO_Q, DAEMON + id + ")", "successfully connected to queue '" + QUEUE + "' on host " + config.s( Configuration.KEY_RABBITMQ_SERVER + stagePostfix ) +
                " at port " + config.i( Configuration.KEY_RABBITMQ_PORT + stagePostfix, DEFAULT_PORT )  );


            logger.info( "Please look into table [integrationlog] to track the status of the match-server" );
            while( running )
            {
                channel.txSelect();
                GetResponse response = channel.basicGet( QUEUE, false );
                if( response != null )
                {
                    logger.debug( "msgcount = " + response.getMessageCount() );

                    MatchParam matchParam = null;
                    try
                    {
                        matchParam = MatchParam.deserialize( response.getBody() );
                        logger.info( "received // " + matchParam.toString() + " // with deliveryTag:" + response.getEnvelope().getDeliveryTag() );
                        channel.basicAck( response.getEnvelope().getDeliveryTag(), false );
                        channel.txCommit();

                        timedRun( executor, runnableFactory.create( matchParam ), 20, TimeUnit.SECONDS );
                    }
                    catch( IllegalArgumentException e )
                    {

                        dbLog.log( IntegrationType.MATCH, OperationType.DESERIALIZING, DAEMON + id + ")", "Error deserializing body of queue message" );

                    }
                    catch( InterruptedException ie )
                    {
                        ie.printStackTrace(); // TODO what to do ?
                    }
                    catch( ExecutionException ee )
                    {
                        ee.printStackTrace(); // TODO what to do ?
                    }
                }
                Thread.sleep( pollFrequency );
            }

        }
        catch( Throwable t )
        {
            logger.error( t );
            dbLog.log( IntegrationType.MATCH, OperationType.READ_FROM_Q, DAEMON + id + ")", Common.makeStackTrace( t ) );

        }
        finally
        {
            try
            {
                if( connection != null )
                {
                    logger.info( "[rabbitmq/matchserver] connection closing..." );
                    connection.close( 500 );
                    logger.info( "[rabbitmq/matchserver] connection closed" );
                }
            }
            catch( IOException ioe ) {}
            catch( ShutdownSignalException sse ) {}

            logger.info( "Attempting to shutdown executor" );
            try
            {
                executor.shutdown();
                executor.awaitTermination( 5, TimeUnit.SECONDS );
                logger.info( "executor is fully terminated" );
            }
            catch( InterruptedException ignored ) 
            {
                logger.error( "oops", ignored );
            }

            StringBuilder builder = new StringBuilder();
            builder.append( DAEMON ).append( id ).append( ")" );

            logger.info( builder.toString() );

            dbLog.log( IntegrationType.MATCH, OperationType.CONNECT_TO_Q, builder.toString(), "dead" );

        }
    }

    /**
     * Adapted from code in ch7 of Java Concurrency in practice
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private final void timedRun( ExecutorService executor, Runnable runnable, long timeout, TimeUnit unit )
        throws InterruptedException, ExecutionException
    {
        Future<?> task = executor.submit( runnable );
        try
        {
            task.get( timeout, unit );
        }
        catch( TimeoutException toe )
        {
            // noop
        }
        catch( ExecutionException ee )
        {
            throw ee;
        }
        finally
        {
            task.cancel( true );
        }
    }

    private ChannelWrapper getChannelFor( Connection conn,
        String exchange, String q ) throws IOException
    {
        Channel channel = conn.createChannel();
        if( channel == null )
            throw new IOException( "No more channels" );

        final boolean PASSIVE = false; // false if you want it to create if missing
        final boolean DURABLE = true;
        final boolean EXCLUSIVE = false;
        final boolean AUTODELETE = false;

        channel.exchangeDeclare( exchange, EXCHANGE_TYPE, DURABLE );

        channel.queueDeclare( q, PASSIVE, DURABLE, EXCLUSIVE,
            AUTODELETE, new HashMap<String, Object>() );

        channel.queueBind( q, exchange, q );

        ChannelWrapper result = new ChannelWrapper();
        result.channel = channel;

        return result;
    }

    private final MatcherRunnableFactory runnableFactory;
    private final Configuration config;
    private final Provider<Connection> queueConnectionProvider;
    private final DbLogger dbLog;
    private final StageMapper mapper;
    private final Provider<ExecutorService> executorProvider;

    private final long id;
    private final int pollFrequency;

    private volatile Thread thread;
    private volatile boolean running = true;

    private final static String EXCHANGE_TYPE = "direct";
    private final static String DAEMON = "(matcher-server ";

    private final static String KEY_POLL_FREQUENCY_MS = "daemon.poll.frequency.millisecond";
    private final static int DEFAULT_PORT = 5672;    
    private static final Logger logger = Logger.getLogger( DefaultPoller.class );

    private final static String KEY_QUEUENAME = "rabbitmq.queuename.match";

    private final static String KEY_EXCHANGE = "rabbitmq.exchange.match";
    
    private class ChannelWrapper
    {
        public Channel channel;

        /**
         * @deprecated only for rabbitmq 1.4.0
         */
        public int ticket;
    }
}
