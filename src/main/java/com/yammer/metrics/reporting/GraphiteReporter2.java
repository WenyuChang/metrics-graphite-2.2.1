package com.yammer.metrics.reporting;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.core.VirtualMachineMetrics.GarbageCollectorStats;
import com.yammer.metrics.stats.Snapshot;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphiteReporter2
        extends AbstractPollingReporter
        implements MetricProcessor<Long>
{
    private static final Logger LOG = LoggerFactory.getLogger(GraphiteReporter2.class);
    protected final String prefix;
    protected final MetricPredicate predicate;
    protected final Locale locale = Locale.US;
    protected final Clock clock;
    protected final SocketProvider socketProvider;
    protected final VirtualMachineMetrics vm;
    protected Writer writer;
    public boolean printVMMetrics = true;
    private String cycleUUID;

    public static void enable(long period, TimeUnit unit, String host, int port)
    {
        enable(Metrics.defaultRegistry(), period, unit, host, port);
    }

    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port)
    {
        enable(metricsRegistry, period, unit, host, port, null);
    }

    public static void enable(long period, TimeUnit unit, String host, int port, String prefix)
    {
        enable(Metrics.defaultRegistry(), period, unit, host, port, prefix);
    }

    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix)
    {
        enable(metricsRegistry, period, unit, host, port, prefix, MetricPredicate.ALL);
    }

    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix, MetricPredicate predicate)
    {
        try
        {
            GraphiteReporter2 reporter = new GraphiteReporter2(metricsRegistry, prefix, predicate, new DefaultSocketProvider(host, port), Clock.defaultClock());

            reporter.start(period, unit);
        }
        catch (Exception e)
        {
            LOG.error("Error creating/starting Graphite reporter:", e);
        }
    }

    public GraphiteReporter2(String host, int port, String prefix)
            throws IOException
    {
        this(Metrics.defaultRegistry(), host, port, prefix);
    }

    public GraphiteReporter2(MetricsRegistry metricsRegistry, String host, int port, String prefix)
            throws IOException
    {
        this(metricsRegistry, prefix, MetricPredicate.ALL, new DefaultSocketProvider(host, port), Clock.defaultClock());
    }

    public GraphiteReporter2(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, SocketProvider socketProvider, Clock clock)
            throws IOException
    {
        this(metricsRegistry, prefix, predicate, socketProvider, clock, VirtualMachineMetrics.getInstance());
    }

    public GraphiteReporter2(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, SocketProvider socketProvider, Clock clock, VirtualMachineMetrics vm)
            throws IOException
    {
        this(metricsRegistry, prefix, predicate, socketProvider, clock, vm, "graphite-reporter");
    }

    public GraphiteReporter2(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, SocketProvider socketProvider, Clock clock, VirtualMachineMetrics vm, String name)
            throws IOException
    {
        super(metricsRegistry, name);
        this.socketProvider = socketProvider;
        this.vm = vm;

        this.clock = clock;
        if (prefix != null) {
            this.prefix = (prefix + ".");
        } else {
            this.prefix = "";
        }
        this.predicate = predicate;
    }

    public void run()
    {
        cycleUUID = UUID.randomUUID().toString();
        Socket socket = null;
        try
        {
            LOG.debug("[" + cycleUUID + " ] Start new cycle of pusing metrics. ");
            socket = this.socketProvider.get();
            this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

            long epoch = this.clock.time() / 1000L;
            if (this.printVMMetrics) {
                printVmMetrics(epoch);
            }

            printRegularMetrics(Long.valueOf(epoch));
            this.writer.flush();
        }
        catch (Exception e)
        {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error writing to Graphite", e);
            } else {
                LOG.warn("Error writing to Graphite: {}", e.getMessage());
            }
            if (this.writer != null) {
                try
                {
                    this.writer.flush();
                }
                catch (Exception e1)
                {
                    LOG.error("Error while flushing writer:", e1);
                }
            }
        }
        finally
        {
            if (socket != null) {
                try
                {
                    socket.close();
                }
                catch (Exception e)
                {
                    LOG.error("Error while closing socket:", e);
                }
            }
            this.writer = null;
            LOG.debug("[" + cycleUUID + " ] Finish cycle of pusing metrics");
        }
    }

    protected void printRegularMetrics(Long epoch)
    {
        LOG.debug("[" + cycleUUID + " ] Start to push regular metrics");

        for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(this.predicate).entrySet()) {
            LOG.debug("[" + cycleUUID + " ] Inside for first loop for metrics: " + entry.getKey());
            for (Map.Entry<MetricName, Metric> subEntry : (entry.getValue()).entrySet())
            {
                LOG.debug("[" + cycleUUID + " ] Inside for second loop for metrics: " + subEntry.getKey().toString());
                Metric metric = (Metric)subEntry.getValue();
                if (metric != null) {
                    try
                    {
                        LOG.debug("[" + cycleUUID + " ] Start to process metrics: " + subEntry.getValue().toString());
                        metric.processWith(this, (MetricName)subEntry.getKey(), epoch);
                        LOG.debug("[" + cycleUUID + " ] Finish to processing metrics: " + subEntry.getValue().toString());
                    }
                    catch (Exception ignored)
                    {
                        LOG.error("Error printing regular metrics:", ignored);
                    }
                }
            }
        }

        LOG.debug("[" + cycleUUID + " ] Finish pushing regular metrics");
    }

    protected void sendInt(long timestamp, String name, String valueName, long value)
    {
        sendToGraphite(timestamp, name, valueName + " " + String.format(this.locale, "%d", new Object[] { Long.valueOf(value) }));
    }

    protected void sendFloat(long timestamp, String name, String valueName, double value)
    {
        sendToGraphite(timestamp, name, valueName + " " + String.format(this.locale, "%2.2f", new Object[] { Double.valueOf(value) }));
    }

    protected void sendObjToGraphite(long timestamp, String name, String valueName, Object value)
    {
        sendToGraphite(timestamp, name, valueName + " " + String.format(this.locale, "%s", new Object[] { value }));
    }

    protected void sendToGraphite(long timestamp, String name, String value)
    {
        try
        {
            if (!this.prefix.isEmpty()) {
                this.writer.write(this.prefix);
            }
            this.writer.write(sanitizeString(name));
            this.writer.write(46);
            this.writer.write(value);
            this.writer.write(32);
            this.writer.write(Long.toString(timestamp));
            this.writer.write(10);
            this.writer.flush();
        }
        catch (Exception e)
        {
            LOG.error("Error sending to Graphite:", e);
        }
    }

    protected String sanitizeName(MetricName name)
    {
        StringBuilder sb = new StringBuilder().append(name.getGroup()).append('.').append(name.getType()).append('.');
        if (name.hasScope()) {
            sb.append(name.getScope()).append('.');
        }
        return name.getName();
    }

    protected String sanitizeString(String s)
    {
        return s.replace(' ', '-');
    }

    public void processGauge(MetricName name, Gauge<?> gauge, Long epoch)
            throws IOException
    {
        sendObjToGraphite(epoch.longValue(), sanitizeName(name), "value", gauge.value());
    }

    public void processCounter(MetricName name, Counter counter, Long epoch)
            throws IOException
    {
        sendInt(epoch.longValue(), sanitizeName(name), "count", counter.count());
    }

    public void processMeter(MetricName name, Metered meter, Long epoch)
            throws IOException
    {
        String sanitizedName = sanitizeName(name);
        sendInt(epoch.longValue(), sanitizedName, "count", meter.count());
        sendFloat(epoch.longValue(), sanitizedName, "meanRate", meter.meanRate());
        sendFloat(epoch.longValue(), sanitizedName, "1MinuteRate", meter.oneMinuteRate());
        sendFloat(epoch.longValue(), sanitizedName, "5MinuteRate", meter.fiveMinuteRate());
        sendFloat(epoch.longValue(), sanitizedName, "15MinuteRate", meter.fifteenMinuteRate());
    }

    public void processHistogram(MetricName name, Histogram histogram, Long epoch)
            throws IOException
    {
        String sanitizedName = sanitizeName(name);
        sendSummarizable(epoch.longValue(), sanitizedName, histogram);
        sendSampling(epoch.longValue(), sanitizedName, histogram);
    }

    public void processTimer(MetricName name, Timer timer, Long epoch)
            throws IOException
    {
        processMeter(name, timer, epoch);
        String sanitizedName = sanitizeName(name);
        sendSummarizable(epoch.longValue(), sanitizedName, timer);
        sendSampling(epoch.longValue(), sanitizedName, timer);
    }

    protected void sendSummarizable(long epoch, String sanitizedName, Summarizable metric)
            throws IOException
    {
        sendFloat(epoch, sanitizedName, "min", metric.min());
        sendFloat(epoch, sanitizedName, "max", metric.max());
        sendFloat(epoch, sanitizedName, "mean", metric.mean());
        sendFloat(epoch, sanitizedName, "stddev", metric.stdDev());
    }

    protected void sendSampling(long epoch, String sanitizedName, Sampling metric)
            throws IOException
    {
        Snapshot snapshot = metric.getSnapshot();
        sendFloat(epoch, sanitizedName, "median", snapshot.getMedian());
        sendFloat(epoch, sanitizedName, "75percentile", snapshot.get75thPercentile());
        sendFloat(epoch, sanitizedName, "95percentile", snapshot.get95thPercentile());
        sendFloat(epoch, sanitizedName, "98percentile", snapshot.get98thPercentile());
        sendFloat(epoch, sanitizedName, "99percentile", snapshot.get99thPercentile());
        sendFloat(epoch, sanitizedName, "999percentile", snapshot.get999thPercentile());
    }

    protected void printVmMetrics(long epoch)
    {
        LOG.debug("[" + cycleUUID + " ] Start to push VM metrics");

        sendFloat(epoch, "jvm.memory", "heap_usage", this.vm.heapUsage());
        sendFloat(epoch, "jvm.memory", "non_heap_usage", this.vm.nonHeapUsage());
        for (Map.Entry<String, Double> pool : this.vm.memoryPoolUsage().entrySet()) {
            sendFloat(epoch, "jvm.memory.memory_pool_usages", sanitizeString((String)pool.getKey()), ((Double)pool.getValue()).doubleValue());
        }
        sendInt(epoch, "jvm", "daemon_thread_count", this.vm.daemonThreadCount());
        sendInt(epoch, "jvm", "thread_count", this.vm.threadCount());
        sendInt(epoch, "jvm", "uptime", this.vm.uptime());
        sendFloat(epoch, "jvm", "fd_usage", this.vm.fileDescriptorUsage());
        for (Map.Entry<Thread.State, Double> entry : this.vm.threadStatePercentages().entrySet()) {
            sendFloat(epoch, "jvm.thread-states", ((Thread.State)entry.getKey()).toString().toLowerCase(), ((Double)entry.getValue()).doubleValue());
        }
        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : this.vm.garbageCollectors().entrySet())
        {
            String name = "jvm.gc." + sanitizeString((String)entry.getKey());
            sendInt(epoch, name, "time", ((VirtualMachineMetrics.GarbageCollectorStats)entry.getValue()).getTime(TimeUnit.MILLISECONDS));
            sendInt(epoch, name, "runs", ((VirtualMachineMetrics.GarbageCollectorStats)entry.getValue()).getRuns());
        }

        LOG.debug("[" + cycleUUID + " ] Finish pushing VM metrics");
    }

    public static class DefaultSocketProvider
            implements SocketProvider
    {
        private final String host;
        private final int port;

        public DefaultSocketProvider(String host, int port)
        {
            this.host = host;
            this.port = port;
        }

        public Socket get()
                throws Exception
        {
            return new Socket(this.host, this.port);
        }
    }
}
