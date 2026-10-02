using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace KestrelCache.Server.Observability;

/// <summary>
/// Server-level counters and a latency histogram, exported in Prometheus text format.
/// </summary>
/// <remarks>
/// <para>
/// The histogram is the part that matters. A counter of commands served tells you throughput,
/// which is the number that looks best and explains least; what an operator actually needs when
/// something is wrong is the <i>shape</i> of the latency distribution, because a server
/// averaging 200 µs while 1 request in 500 takes 50 ms is a server with a problem that no mean
/// will reveal.
/// </para>
/// <para>
/// Buckets are cumulative and exponentially spaced, which is the Prometheus convention:
/// <c>le="0.001"</c> counts every request at or under a millisecond. Exponential spacing is
/// what makes one modest set of buckets cover microseconds through seconds, and it matches how
/// latency problems actually present — an order of magnitude at a time.
/// </para>
/// </remarks>
internal sealed class ServerMetrics
{
    /// <summary>Upper bounds in seconds, matching the Prometheus client-library defaults.</summary>
    private static readonly double[] BucketBounds =
    [
        0.000_05, 0.000_1, 0.000_25, 0.000_5,
        0.001, 0.002_5, 0.005, 0.01,
        0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    private readonly long[] _buckets = new long[BucketBounds.Length + 1]; // last is +Inf
    private readonly DateTimeOffset _startedAt = DateTimeOffset.UtcNow;

    private long _connectionsAccepted;
    private long _connectionsActive;
    private long _connectionsRejected;
    private long _commandsTotal;
    private long _commandErrors;
    private long _protocolErrors;
    private long _latencyTicksTotal;
    private long _bytesRead;
    private long _bytesWritten;
    private long _keyspaceHits;
    private long _keyspaceMisses;

    internal void ConnectionAccepted()
    {
        Interlocked.Increment(ref _connectionsAccepted);
        Interlocked.Increment(ref _connectionsActive);
    }

    internal void ConnectionClosed() => Interlocked.Decrement(ref _connectionsActive);

    internal void ConnectionRejected() => Interlocked.Increment(ref _connectionsRejected);

    internal void ProtocolError() => Interlocked.Increment(ref _protocolErrors);

    internal void CommandError() => Interlocked.Increment(ref _commandErrors);

    internal void KeyspaceHit() => Interlocked.Increment(ref _keyspaceHits);

    internal void KeyspaceMiss() => Interlocked.Increment(ref _keyspaceMisses);

    internal void BytesRead(long count) => Interlocked.Add(ref _bytesRead, count);

    internal void BytesWritten(long count) => Interlocked.Add(ref _bytesWritten, count);

    /// <summary>Records one command's latency in <see cref="Stopwatch"/> ticks.</summary>
    internal void CommandCompleted(long stopwatchTicks)
    {
        Interlocked.Increment(ref _commandsTotal);
        Interlocked.Add(ref _latencyTicksTotal, stopwatchTicks);

        double seconds = (double)stopwatchTicks / Stopwatch.Frequency;

        // Linear search over seventeen bounds beats a binary search here: the array is one cache
        // line's worth of doubles and the common case exits in the first few comparisons.
        int index = 0;
        while (index < BucketBounds.Length && seconds > BucketBounds[index])
        {
            index++;
        }

        Interlocked.Increment(ref _buckets[index]);
    }

    /// <summary>Active client connections.</summary>
    internal long ActiveConnections => Interlocked.Read(ref _connectionsActive);

    /// <summary>Commands served since start.</summary>
    internal long CommandsTotal => Interlocked.Read(ref _commandsTotal);

    /// <summary>
    /// Renders every metric in Prometheus exposition format, engine counters included.
    /// </summary>
    internal string Render(
        EngineStats engine,
        ServerOptions options,
        KestrelCache.Raft.RaftStats? raft = null)
    {
        var text = new StringBuilder(4096);

        void Counter(string name, string help, double value, string? labels = null)
        {
            text.Append("# HELP ").Append(name).Append(' ').Append(help).Append('\n');
            text.Append("# TYPE ").Append(name).Append(" counter\n");
            text.Append(name);
            if (labels is not null) text.Append('{').Append(labels).Append('}');
            text.Append(' ')
                .Append(value.ToString("G17", CultureInfo.InvariantCulture))
                .Append('\n');
        }

        void Gauge(string name, string help, double value)
        {
            text.Append("# HELP ").Append(name).Append(' ').Append(help).Append('\n');
            text.Append("# TYPE ").Append(name).Append(" gauge\n");
            text.Append(name).Append(' ')
                .Append(value.ToString("G17", CultureInfo.InvariantCulture))
                .Append('\n');
        }

        // ---- server
        Gauge("kestrelcache_up", "1 when the server is serving.", 1);
        Gauge(
            "kestrelcache_uptime_seconds",
            "Seconds since the server started.",
            (DateTimeOffset.UtcNow - _startedAt).TotalSeconds);
        Gauge("kestrelcache_connections_active", "Client connections currently open.", ActiveConnections);
        Counter("kestrelcache_connections_accepted_total", "Connections accepted.", Interlocked.Read(ref _connectionsAccepted));
        Counter("kestrelcache_connections_rejected_total", "Connections refused at the limit.", Interlocked.Read(ref _connectionsRejected));
        Counter("kestrelcache_commands_total", "Commands served.", CommandsTotal);
        Counter("kestrelcache_command_errors_total", "Commands that returned an error.", Interlocked.Read(ref _commandErrors));
        Counter("kestrelcache_protocol_errors_total", "Connections closed for malformed RESP.", Interlocked.Read(ref _protocolErrors));
        Counter("kestrelcache_net_input_bytes_total", "Bytes read from clients.", Interlocked.Read(ref _bytesRead));
        Counter("kestrelcache_net_output_bytes_total", "Bytes written to clients.", Interlocked.Read(ref _bytesWritten));

        // The ratio an operator actually watches: a cache whose hit rate has collapsed is a
        // cache that has stopped doing its job, whatever its latency looks like.
        Counter("kestrelcache_keyspace_hits_total", "Lookups that found a key.", Interlocked.Read(ref _keyspaceHits));
        Counter("kestrelcache_keyspace_misses_total", "Lookups that found nothing.", Interlocked.Read(ref _keyspaceMisses));

        // ---- latency histogram
        text.Append("# HELP kestrelcache_command_duration_seconds Command latency.\n");
        text.Append("# TYPE kestrelcache_command_duration_seconds histogram\n");

        long cumulative = 0;
        for (int i = 0; i < BucketBounds.Length; i++)
        {
            cumulative += Interlocked.Read(ref _buckets[i]);
            text.Append("kestrelcache_command_duration_seconds_bucket{le=\"")
                .Append(BucketBounds[i].ToString("G17", CultureInfo.InvariantCulture))
                .Append("\"} ")
                .Append(cumulative)
                .Append('\n');
        }

        cumulative += Interlocked.Read(ref _buckets[^1]);
        text.Append("kestrelcache_command_duration_seconds_bucket{le=\"+Inf\"} ")
            .Append(cumulative).Append('\n');
        text.Append("kestrelcache_command_duration_seconds_count ").Append(cumulative).Append('\n');
        text.Append("kestrelcache_command_duration_seconds_sum ")
            .Append(((double)Interlocked.Read(ref _latencyTicksTotal) / Stopwatch.Frequency)
                .ToString("G17", CultureInfo.InvariantCulture))
            .Append('\n');

        // ---- storage engine
        Gauge("kestrelcache_engine_keys", "Keys tracked (an upper bound on the LSM engine).", engine.KeyCount);
        Gauge("kestrelcache_engine_disk_bytes", "Total bytes occupied on disk.", engine.DiskSizeBytes);
        Gauge("kestrelcache_engine_data_file_bytes", "Bytes in data files (log or SSTables).", engine.DataFileBytes);
        Gauge("kestrelcache_engine_wal_bytes", "Bytes in the write-ahead log.", engine.WriteAheadLogBytes);
        Gauge("kestrelcache_engine_live_bytes", "Bytes resident in data files.", engine.LiveDataBytes);
        Gauge(
            "kestrelcache_engine_stale_ratio",
            "Fraction of the data files that is garbage; zero when the engine cannot measure it.",
            engine.StaleRatio);
        Counter("kestrelcache_engine_reads_total", "Engine read operations.", engine.Reads);
        Counter("kestrelcache_engine_writes_total", "Engine write operations.", engine.Writes);
        Counter("kestrelcache_engine_deletes_total", "Engine delete operations.", engine.Deletes);
        Counter("kestrelcache_engine_fsyncs_total", "fsync calls issued.", engine.Syncs);
        Counter("kestrelcache_engine_compactions_total", "Compactions completed.", engine.Compactions);
        Counter("kestrelcache_engine_compaction_bytes_total", "Bytes rewritten by compaction.", engine.CompactionBytesWritten);
        Counter("kestrelcache_engine_bloom_negatives_total", "Reads a Bloom filter answered without I/O.", engine.BloomFilterNegatives);
        Counter("kestrelcache_engine_bloom_false_positives_total", "Bloom filter said maybe and was wrong.", engine.BloomFilterFalsePositives);
        Counter("kestrelcache_engine_block_cache_hits_total", "Block cache hits.", engine.BlockCacheHits);
        Counter("kestrelcache_engine_block_cache_misses_total", "Block cache misses.", engine.BlockCacheMisses);
        Gauge("kestrelcache_engine_block_cache_hit_rate", "Block cache hit rate.", engine.BlockCacheHitRate);

        // Per-level table counts are the clearest single signal of compaction health: a level 0
        // that keeps growing means the compactor is not keeping up with the write rate.
        if (engine.SsTablesPerLevel.Count > 0)
        {
            text.Append("# HELP kestrelcache_engine_sstables SSTables per level.\n");
            text.Append("# TYPE kestrelcache_engine_sstables gauge\n");
            for (int level = 0; level < engine.SsTablesPerLevel.Count; level++)
            {
                text.Append("kestrelcache_engine_sstables{level=\"")
                    .Append(level).Append("\"} ")
                    .Append(engine.SsTablesPerLevel[level]).Append('\n');
            }
        }

        // ---- runtime
        Gauge("kestrelcache_process_working_set_bytes", "Resident memory.", Environment.WorkingSet);
        Gauge("kestrelcache_dotnet_gc_heap_bytes", "Managed heap size.", GC.GetTotalMemory(forceFullCollection: false));
        Gauge("kestrelcache_dotnet_gc_collections_gen0", "Gen-0 collections.", GC.CollectionCount(0));
        Gauge("kestrelcache_dotnet_gc_collections_gen1", "Gen-1 collections.", GC.CollectionCount(1));
        Gauge("kestrelcache_dotnet_gc_collections_gen2", "Gen-2 collections.", GC.CollectionCount(2));
        Gauge("kestrelcache_dotnet_threadpool_threads", "Thread-pool threads.", ThreadPool.ThreadCount);
        Gauge("kestrelcache_dotnet_threadpool_queue_length", "Queued thread-pool work items.", ThreadPool.PendingWorkItemCount);

        // ---- consensus
        if (raft is not null)
        {
            Gauge("kestrelcache_raft_enabled", "1 when this node is part of a Raft cluster.", 1);
            Gauge(
                "kestrelcache_raft_is_leader",
                "1 when this node currently accepts writes.",
                raft.Role == KestrelCache.Raft.RaftRole.Leader ? 1 : 0);
            Gauge("kestrelcache_raft_term", "The term this node believes it is in.", raft.Term);
            Gauge("kestrelcache_raft_commit_index", "Highest committed log index.", raft.CommitIndex);
            Gauge("kestrelcache_raft_last_applied", "Highest index applied to the state machine.", raft.LastApplied);
            Gauge("kestrelcache_raft_log_last_index", "Index of the last log entry.", raft.LastLogIndex);

            // The log's live range is what makes snapshotting visible. A first index above 1
            // means the prefix has been folded into a snapshot, and the gap between first and
            // last is what a restart actually has to replay.
            Gauge("kestrelcache_raft_log_first_index", "Lowest index still held as a log entry.", raft.FirstLogIndex);
            Gauge("kestrelcache_raft_log_entries", "Entries the log holds.", raft.LogEntryCount);
            Gauge("kestrelcache_raft_log_bytes", "Bytes the Raft log occupies.", raft.LogSizeBytes);
            Gauge("kestrelcache_raft_snapshot_index", "Last index covered by the snapshot.", raft.SnapshotIndex);
            Gauge("kestrelcache_raft_snapshot_bytes", "Size of the snapshot file.", raft.SnapshotSizeBytes);
            Counter("kestrelcache_raft_snapshots_taken_total", "Snapshots this node has taken.", raft.SnapshotsTaken);
            Counter("kestrelcache_raft_snapshots_installed_total", "Snapshots received from a leader.", raft.SnapshotsInstalled);
            Counter("kestrelcache_raft_snapshot_chunks_sent_total", "Snapshot chunks sent as leader.", raft.SnapshotChunksSent);
            Counter("kestrelcache_raft_elections_started_total", "Elections this node has started.", raft.ElectionsStarted);
            Counter("kestrelcache_raft_pre_votes_won_total", "Pre-vote rounds won, each becoming a real campaign.", raft.PreVotesWon);

            // The useful one. Each lost pre-vote is an election that did not happen: the term was
            // never incremented and the rest of the cluster never had to react. A climbing count
            // means something keeps trying to campaign and cannot -- a flapping link, or a node
            // that does not know it was removed.
            Counter("kestrelcache_raft_pre_votes_lost_total", "Pre-vote rounds lost, each an election avoided.", raft.PreVotesLost);
            Counter("kestrelcache_raft_elections_won_total", "Elections this node has won.", raft.ElectionsWon);
            Counter("kestrelcache_raft_membership_changes_total", "Membership changes completed.", raft.MembershipChanges);

            if (raft.Configuration is { } configuration)
            {
                Gauge("kestrelcache_raft_voters", "Voters in the current configuration.", configuration.Voters.Count);

                // Learners are replicated to but counted in no quorum, so they are tracked
                // separately: a cluster of three voters and two learners tolerates one failure,
                // not two.
                Gauge("kestrelcache_raft_learners", "Non-voting members.", configuration.Learners.Count);
                Gauge("kestrelcache_raft_quorum_size", "Votes needed for a decision.", configuration.QuorumSize);

                // Worth alerting on: a change that never leaves the joint phase means the cluster
                // permanently needs two majorities, so it tolerates fewer failures than either
                // configuration alone.
                Gauge(
                    "kestrelcache_raft_membership_change_in_progress",
                    "1 while a membership change is in its joint phase.",
                    configuration.IsJoint ? 1 : 0);
            }

            // Replication lag per follower is the clearest sign of a node falling behind far
            // enough to need a state transfer rather than ordinary catch-up.
            if (raft.MatchIndex.Count > 0)
            {
                text.Append("# HELP kestrelcache_raft_peer_match_index Highest index each peer has acknowledged.\n");
                text.Append("# TYPE kestrelcache_raft_peer_match_index gauge\n");
                foreach (var (peer, matchIndex) in raft.MatchIndex.OrderBy(p => p.Key))
                {
                    text.Append("kestrelcache_raft_peer_match_index{peer=\"")
                        .Append(peer).Append("\"} ").Append(matchIndex).Append('\n');
                }
            }
        }
        else
        {
            Gauge("kestrelcache_raft_enabled", "1 when this node is part of a Raft cluster.", 0);
        }

        Counter(
            "kestrelcache_build_info",
            "Static build and configuration labels.",
            1,
            $"engine=\"{engine.Engine}\",sync=\"{options.SyncPolicy}\",runtime=\"{Environment.Version}\"");

        return text.ToString();
    }
}
