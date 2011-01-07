package com.raverun.match.server.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.raverun.shared.Common;
import com.raverun.shared.Configuration;
import com.raverun.shared.StageMapper;

/**
 * Database connection pool
 * 
 * @author Gavin Bong
 */
@Singleton
public class DbConnectionProvider implements Provider<Connection>
{
    @Inject
    public DbConnectionProvider( Configuration config, StageMapper mapper ) throws Exception
    {
        final String stage = mapper.toCanonicalStage();
        
        Class.forName( config.s( KEY_DRIVER ) );
        dataSource = new BasicDataSource();
        dataSource.setUsername( config.s( KEY_DATABASE_USERID + stage ) );
        dataSource.setPassword( config.s( KEY_DATABASE_PASSWD + stage ) );
        dataSource.setUrl( config.s( KEY_URL ) );
        dataSource.setInitialSize( 3 );
        dataSource.setMaxActive( config.i( KEY_MAX_ACTIVE, 13 ) );
        dataSource.setMaxIdle( 5 );
        dataSource.setMinIdle( 3 );
        dataSource.setValidationQuery( "select 1 from dual" );
    }

    public Connection get()
    {
        try
        {
            return dataSource.getConnection();
        }
        catch( SQLException e )
        {
            throw new RuntimeException( Common.makeStackTrace( e ) );
        }
    }

    public final static String KEY_DATABASE_USERID = "db.userid.";
    public final static String KEY_DATABASE_PASSWD = "db.password.";

    public final static String KEY_URL        = "db.url";
    public final static String KEY_DRIVER     = "db.driver";
    public final static String KEY_MAX_ACTIVE = "db.maxactive";

    private final BasicDataSource dataSource;
}

