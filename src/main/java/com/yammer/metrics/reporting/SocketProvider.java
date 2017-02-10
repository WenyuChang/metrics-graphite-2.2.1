package com.yammer.metrics.reporting;

import java.net.Socket;

public abstract interface SocketProvider
{
    public abstract Socket get()
            throws Exception;
}