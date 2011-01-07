package com.raverun.match.server.api;

import com.raverun.match.api.MatchParam;

public interface MatcherRunnableFactory
{
    Runnable create( MatchParam param );
}
