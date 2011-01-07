package com.raverun.match.server.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.raverun.shared.Configuration;

@Singleton
public class ExecutorServiceProvider implements Provider<ExecutorService>
{
    @Inject
    public ExecutorServiceProvider( Configuration config )
    {
        this.config = config;
    }
    
    public ExecutorService get()
    {
        return Executors.newFixedThreadPool( config.i( KEY_SIGNUP_POOLSIZE, 10 ) );
    }

    private final Configuration config;

    private final static String KEY_SIGNUP_POOLSIZE = "daemon.threadpool.size";
}
