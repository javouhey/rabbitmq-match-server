package com.raverun.match.server.impl;

import com.google.inject.Inject;
import com.raverun.match.server.api.RetryPolicy;
import com.raverun.shared.Configuration;

/**
 * A reference implementation of {@code RetryPolicy} that
 * <ul>
 * <li>retries forever
 * <li>does not alert (for now)
 * <li>uses {@code daemon.backoff.max.millisecond} and {@code daemon.backoff.multiplier} from {@code daemon.properties}
 * </ul>
 * 
 * @author Gavin Bong
 */
public class DefaultRetryPolicy implements RetryPolicy
{
    @Inject
    public DefaultRetryPolicy( Configuration config )
    {
        BACKOFF_MULTIPLIER = config.i( DefaultRetryPolicy.KEY_BACKOFF_MULTIPLIER, 2 );
        BACKOFF_MAX = config.i( DefaultRetryPolicy.KEY_BACKOFF_MAX, 20000 );
    }

    public boolean mustAlert()
    {
        return true;
    }

    public int newBackoff( int currentBackoff )
    {
        return min( BACKOFF_MAX, currentBackoff * BACKOFF_MULTIPLIER );
    }

    public boolean retry( long numberOfExecutions )
    {
        return true;
    }

    private final int min( int left, int right )
    {
        if( left <= right )
            return left;
        else
            return right;
    }
    
    private final int BACKOFF_MULTIPLIER;
    private final int BACKOFF_MAX;

    private static final String KEY_BACKOFF_MULTIPLIER = "daemon.backoff.multiplier";
    private static final String KEY_BACKOFF_MAX        = "daemon.backoff.max.millisecond";
}

