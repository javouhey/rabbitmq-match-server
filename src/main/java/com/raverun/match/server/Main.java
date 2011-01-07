package com.raverun.match.server;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.raverun.match.server.GuiceModule.ConfigurationType;
import com.raverun.match.server.api.MatchDaemon;
import com.raverun.shared.Constraint;
import com.raverun.shared.services.PropertyFile;
import com.raverun.shared.services.impl.PropertyFileImpl;

public class Main
{
    @Inject
    public Main( MatchDaemon daemon )
    {
        this.daemon = daemon;
    }

    public static void main( String[] args )
    {
    // defaults
        String service    = "file";
        String path       = null;
        String stage      = "production";
        String datasource = "mcdb";

        String os = System.getProperty( "os.name" ).toLowerCase();
        System.out.println( "Detected operating system ... " + os );
        if( os.startsWith( "linux" ) )
            path = PREFIX_PROPERTY_FILE_LINUX + DEFAULT_PROPERTY_FILENAME;
        else
            path = PREFIX_PROPERTY_FILE_WIN + DEFAULT_PROPERTY_FILENAME;

    // process command line
        Options options = newOptions();
        CommandLineParser parser = new PosixParser();
        try
        {
            CommandLine cmd = parser.parse( options, args);

            if( cmd.hasOption( "h" ) )
            {
                printHelp( options );   
                System.exit( 0 );
            }


            if( cmd.hasOption( 'p' ) ) 
            {
                if( Constraint.EmptyString.isFulfilledBy( cmd.getOptionValue( 'p' ) ))
                    throw new ParseException( "path cannot be an empty string" );

                path = cmd.getOptionValue( 'p' ).trim();
            }

            if( cmd.hasOption( 'd' ) ) 
            {
                if( Constraint.EmptyString.isFulfilledBy( cmd.getOptionValue( 'd' ) ))
                    throw new ParseException( "datasource cannot be an empty string" );

                datasource = cmd.getOptionValue( 'd' ).trim();
            }
        }
        catch( ParseException pe )
        {
            printHelp( options );
            System.exit( 0 ); 
        }


        logger.info( "Configuring with properties [service=" + service + " | path=" + path + " | stage=" + stage + " | datasource=" + DATASOURCE_PREFIX + FORWARD_SLASH + datasource + "]" );

        final PropertyFile propertyFile = new PropertyFileImpl();

        if( !propertyFile.validFile( path ) )
        {
            logger.fatal( "Properties file " + path + " not found. Aborting!" );
            throw new RuntimeException( "properties file invalid" );
        }

        Main main = null;
        
        try
        {
            initializeJndiDataSource( propertyFile.load( path ), datasource );

            Injector injector = getInjector( configurationFor( service ), stageFor( stage ), path );
            logger.info( "Guice Injector created" );

            main = injector.getInstance( Main.class );
            
            final Main[] mainArray = new Main[] { main };
            Runtime.getRuntime().addShutdownHook( 
                new Thread() 
                { 
                    public void run() 
                    { 
                        logger.info( "Shutting down" );
                        if( mainArray[ 0 ].daemon != null )
                            mainArray[ 0 ].daemon.stop();
                     }
                 });
            
            main.daemon.start();
            main.daemon.join();

            logger.info( "dying" );
        }
        catch( Throwable t )
        {
            logger.error( "Could not start due to", t );
        }
        finally
        {
            if( main != null && main.daemon != null )
                main.daemon.stop();
        }
    }

    private static final void printHelp( Options options )
    {
        System.out.println( "\nMatch server. ${daemon.version} Copyright 2009 raverun.com\n" );
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "java -jar match-server.jar", options );
    }

    /**
     * @throws IllegalStateException
     */
    private static void initializeJndiDataSource( Properties props, final String datasource )
    {
        try 
        {
            ic = new InitialContext();

            ic.createSubcontext( "java:" );
            ic.createSubcontext( "java:comp" );
            ic.createSubcontext( "java:comp/env" );
            ic.createSubcontext( DATASOURCE_PREFIX ); // "java:comp/env/jdbc"

            BasicDataSource ds = new BasicDataSource();
            ds.setUrl( props.getProperty( "db.url" ) );
            ds.setDriverClassName( props.getProperty( "db.driver" ) );
            ds.setTimeBetweenEvictionRunsMillis( 900 );
            ds.setMinEvictableIdleTimeMillis( 65000 );
            ds.setValidationQuery( "SELECT 1" );
            ds.setTestOnBorrow( true );
            ds.setTestWhileIdle( true );
            ds.setMaxActive( 15 );
            ds.setMaxWait( 10000 );
            ds.setMaxIdle( 6 );
            ds.setUsername( USER );
            ds.setPassword( PASS );

            final String fullDatasource = DATASOURCE_PREFIX + FORWARD_SLASH + datasource;
            ic.bind( fullDatasource, ds ); // "java:comp/env/jdbc/mcdb"

            BasicDataSource bds = (BasicDataSource) ic.lookup( fullDatasource ); // "java:comp/env/jdbc/mcdb"
            logger.info( "Main: " + bds.getDriverClassName() );
        } 
        catch( NamingException ex ) 
        {
            throw new IllegalStateException( ex );
        }
    }
    
    private static Injector getInjector( ConfigurationType ct, Stage stage, String path )
    {
        Injector injector = Guice.createInjector( stage,
            new AbstractModule[] { new GuiceModule( ct, path ) } );
        return injector;
    }

    private static ConfigurationType configurationFor( String readValue )
    {
        // Defaults to FILE type if the {@code readValue} is unrecognizable
        if( !map.containsKey( readValue ) )
            return ConfigurationType.FILE;

        return map.get( readValue );
    }

    private static Stage stageFor( String stage )
    {
        return Stage.PRODUCTION;
    }

    @SuppressWarnings("static-access")
    private static Options newOptions()
    {
        Options options = new Options();

        Option help = new Option( "h", "print this message" );
        options.addOption( help );

        Option dbOption = OptionBuilder.withArgName( "datasource name" )
                                       .hasArg()
                                       .withDescription( "java:comp/env/jdbc/${value}. Where '${value}' placeholder defaults to 'mcdb'" )
                                       .create( "d" );
        options.addOption( dbOption );

        Option pathOption = OptionBuilder.withArgName( "path" )
                                     .hasArg()
                                     .withDescription( "fully qualified path to your config file e.g. defaults linux(/etc/raverun/matchserver.properties) windows(C:\\matchserver.properties)" )
                                     .create( "p" );
        options.addOption( pathOption );

        return options;
    }

    private static Map<String, ConfigurationType> map = new HashMap<String, ConfigurationType>( 2 );

    public final static String CONFIG_DB = "db";
    public final static String CONFIG_FILE = "file";

    // FIXME temporary hack
    private static final String USER = "raverun";
    private static final String PASS = "dba"; 

    static
    {
        map.put( CONFIG_DB, ConfigurationType.DB );
        map.put( CONFIG_FILE, ConfigurationType.FILE );
        map.put( null, ConfigurationType.FILE );
    }
    
    private static final Logger logger = Logger.getLogger( Main.class );

    private final MatchDaemon daemon;

    private static InitialContext ic;

// --------- constants ---------------

    private static final String FORWARD_SLASH = "/";
    private static final String DATASOURCE_PREFIX = "java:comp/env/jdbc";
    private static final String PREFIX_PROPERTY_FILE_WIN = "C:\\";
    private static final String PREFIX_PROPERTY_FILE_LINUX = "/etc/raverun/";
    private static final String DEFAULT_PROPERTY_FILENAME = "matchserver.properties";
}
