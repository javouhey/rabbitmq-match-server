package com.raverun.match.server.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

/**
 * Copied from core to facilitate easy integration
 */
public class ConnectionPool
{
    private static ConnectionPool instance = new ConnectionPool();

    private BasicDataSource dataSource;
    private int poolSize;
    
    private ConnectionPool()
    {
        poolSize = 30;
    }

    public void setup( String userid, String password, Properties properties )
    {
        try
        {
            Class.forName( properties.getProperty( "db.driver" ) );
            dataSource = new BasicDataSource();
            dataSource.setUsername( userid );
            dataSource.setPassword( password );
            dataSource.setUrl( properties.getProperty( "db.url" ) );
            dataSource.setInitialSize( 5 );
            dataSource.setMaxActive( poolSize );
            dataSource.setMaxIdle( 10 );
            dataSource.setMinIdle( 5 );
            dataSource.setValidationQuery( "select 1 from dual" );
            
            logger.info( ID + "ConnectionPool ready" );
        }
        catch( ClassNotFoundException e )
        {
            logger.fatal( ID + "Failed to setup database connection pool", e );
            return;
        }
    }

    public static ConnectionPool getInstance()
    {
        return instance;
    }

    public void close()
    {
        if( dataSource != null )
        {
            try
            {
                dataSource.close();
            }
            catch( Exception ignore )
            {
            }
            logger.info( ID + "Closed DataSource" );
        }
    }

    public Connection getConnection() throws IllegalStateException
    {
        Connection conn = null;
        try
        {
            conn = dataSource.getConnection();
        }
        catch( SQLException sqlException )
        {
            logger.fatal( sqlException );
            throw new IllegalStateException(
                ID + "Failed to get db connection from db connection pool." );
        }
        return conn;
    }

    public void closeConnection( Connection conn ) 
    {
        if( conn != null )
        {
            try
            {
                conn.close();
            }
            catch( Throwable t )
            {
                logger.warn( ID + "Exception caught when closing database connection.", t );
            }
        }
    }

    private final static String ID = "[match.server] ";

    private static final Logger logger = Logger
        .getLogger( ConnectionPool.class );
}
