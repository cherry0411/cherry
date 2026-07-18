using System.Buffers.Binary;
using System.Threading.Channels;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Cherry.Infrastructure.Heat;

public enum HeatAcceptStatus
{
    Accepted,
    Replay,
    Conflict,
    Backpressure,
    Failed
}

public sealed record HeatAcceptResult(
    HeatAcceptStatus Status,
    int Received,
    int Inserted,
    ulong ExpectedSequence,
    string? Error = null);

public enum HeatCompletionStatus { Accepted, Replay, Conflict, Backpressure, Failed }
public sealed record HeatCompletionResult(HeatCompletionStatus Status, string? Error = null);

internal abstract record HeatAccumulatorCommand;
internal sealed record HeatWriteCommand(
    ChhtBatch Batch,
    TaskCompletionSource<HeatAcceptResult> Completion) : HeatAccumulatorCommand;
internal sealed record HeatCompletionCommand(
    ChhtCompletion Value,
    TaskCompletionSource<HeatCompletionResult> Completion) : HeatAccumulatorCommand;
internal sealed record HeatBarrierCommand(
    DateOnly Day,
    TaskCompletionSource<bool> Completion) : HeatAccumulatorCommand;

public sealed class HeatAccumulatorService : BackgroundService
{
    private readonly HeatOptions _options;
    private readonly HeatRuntimeMetrics _metrics;
    private readonly ILogger<HeatAccumulatorService> _logger;
    private readonly Channel<HeatAccumulatorCommand> _channel;
    private readonly HashSet<DateOnly> _sealingDays = [];
    private readonly object _gate = new();

    public HeatAccumulatorService(
        HeatOptions options,
        HeatRuntimeMetrics metrics,
        ILogger<HeatAccumulatorService> logger)
    {
        _options = options;
        _metrics = metrics;
        _logger = logger;
        _channel = Channel.CreateBounded<HeatAccumulatorCommand>(new BoundedChannelOptions(options.ChannelCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public async Task<HeatAcceptResult> SubmitAsync(ChhtBatch batch, CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<HeatAcceptResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate)
        {
            if (_sealingDays.Contains(batch.Day))
                return new HeatAcceptResult(HeatAcceptStatus.Conflict, batch.RecordCount, 0, batch.Sequence, "UTC day is sealing or sealed");
            // The sealing check and queue insertion are one ordering decision.
            // A barrier can never overtake a write that was admitted as open.
            if (!_channel.Writer.TryWrite(new HeatWriteCommand(batch, completion)))
            {
                _metrics.Rejected();
                return new HeatAcceptResult(HeatAcceptStatus.Backpressure, batch.RecordCount, 0, batch.Sequence, "Heat accumulator queue is full");
            }
        }
        return await completion.Task.WaitAsync(cancellationToken);
    }

    public async Task<HeatCompletionResult> SubmitCompletionAsync(
        ChhtCompletion value,
        CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<HeatCompletionResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate)
        {
            if (_sealingDays.Contains(value.Day))
                return new HeatCompletionResult(HeatCompletionStatus.Conflict, "UTC day is sealing or sealed");
            if (!_channel.Writer.TryWrite(new HeatCompletionCommand(value, completion)))
            {
                _metrics.Rejected();
                return new HeatCompletionResult(HeatCompletionStatus.Backpressure, "Heat accumulator queue is full");
            }
        }
        return await completion.Task.WaitAsync(cancellationToken);
    }

    public async Task<bool> SealBarrierAsync(DateOnly day, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (!_sealingDays.Add(day)) return true;
        }
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_channel.Writer.TryWrite(new HeatBarrierCommand(day, completion)))
        {
            lock (_gate) _sealingDays.Remove(day);
            return false;
        }
        try
        {
            return await completion.Task.WaitAsync(cancellationToken);
        }
        catch
        {
            lock (_gate) _sealingDays.Remove(day);
            throw;
        }
    }

    public void AllowSealRetry(DateOnly day)
    {
        lock (_gate) _sealingDays.Remove(day);
    }

    public string PathForDay(DateOnly day) =>
        Path.Combine(_options.DataDirectory, $"heat-{day:yyyy-MM-dd}.sqlite3");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Directory.CreateDirectory(_options.DataDirectory);
        HeatAccumulatorCommand? deferred = null;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var command = deferred ?? await _channel.Reader.ReadAsync(stoppingToken);
                deferred = null;
                switch (command)
                {
                    case HeatBarrierCommand barrier:
                        barrier.Completion.TrySetResult(true);
                        break;
                    case HeatWriteCommand write:
                        var group = new List<HeatWriteCommand>(_options.CommitBatchRequests) { write };
                        while (group.Count < _options.CommitBatchRequests &&
                               _channel.Reader.TryRead(out var candidate))
                        {
                            if (candidate is HeatWriteCommand next && next.Batch.Day == write.Batch.Day)
                                group.Add(next);
                            else
                            {
                                deferred = candidate;
                                break;
                            }
                        }
                        await ProcessWriteBatchAsync(group, stoppingToken);
                        break;
                    case HeatCompletionCommand completion:
                        await ProcessCompletionAsync(completion, stoppingToken);
                        break;
                }
            }
        }
        finally
        {
            if (deferred is not null) CancelPending(deferred);
            while (_channel.Reader.TryRead(out var pending)) CancelPending(pending);
        }
    }

    private static void CancelPending(HeatAccumulatorCommand command)
    {
        if (command is HeatWriteCommand write)
            write.Completion.TrySetResult(new HeatAcceptResult(
                HeatAcceptStatus.Failed, write.Batch.RecordCount, 0, write.Batch.Sequence, "Heat service stopped"));
        else if (command is HeatBarrierCommand barrier)
            barrier.Completion.TrySetResult(false);
        else if (command is HeatCompletionCommand completion)
            completion.Completion.TrySetResult(new HeatCompletionResult(
                HeatCompletionStatus.Failed, "Heat service stopped"));
    }

    private async Task ProcessCompletionAsync(
        HeatCompletionCommand command,
        CancellationToken stoppingToken)
    {
        try
        {
            await using var connection = await OpenAsync(PathForDay(command.Value.Day), stoppingToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(stoppingToken);
            var result = await PersistCompletionAsync(connection, transaction, command.Value, stoppingToken);
            await transaction.CommitAsync(stoppingToken);
            _metrics.ClearFailure();
            if (result.Status is not (HeatCompletionStatus.Accepted or HeatCompletionStatus.Replay))
                _metrics.Rejected();
            command.Completion.TrySetResult(result);
        }
        catch (Exception exception)
        {
            _metrics.Fail(exception);
            _logger.LogError(exception, "Heat SQLite completion commit failed for {Day}", command.Value.Day);
            command.Completion.TrySetResult(new HeatCompletionResult(
                HeatCompletionStatus.Failed,
                IsStorageFailure(exception) ? "Heat storage unavailable" : "Heat completion commit failed"));
        }
    }

    private async Task ProcessWriteBatchAsync(
        IReadOnlyList<HeatWriteCommand> commands,
        CancellationToken stoppingToken)
    {
        try
        {
            await using var connection = await OpenAsync(PathForDay(commands[0].Batch.Day), stoppingToken);
            await using var transaction =
                (SqliteTransaction)await connection.BeginTransactionAsync(stoppingToken);
            var results = new HeatAcceptResult[commands.Count];
            for (var index = 0; index < commands.Count; index++)
                results[index] = await PersistAsync(connection, transaction, commands[index].Batch, stoppingToken);

            // synchronous=FULL makes this group commit the durable ACK boundary.
            await transaction.CommitAsync(stoppingToken);
            _metrics.ClearFailure();
            for (var index = 0; index < commands.Count; index++)
            {
                var result = results[index];
                if (result.Status is HeatAcceptStatus.Accepted or HeatAcceptStatus.Replay)
                    _metrics.Accepted(result.Received, result.Status == HeatAcceptStatus.Replay);
                else
                    _metrics.Rejected();
                commands[index].Completion.TrySetResult(result);
            }
        }
        catch (Exception exception)
        {
            _metrics.Fail(exception);
            _logger.LogError(exception, "Heat SQLite group commit failed for {Day}", commands[0].Batch.Day);
            foreach (var command in commands)
                command.Completion.TrySetResult(new HeatAcceptResult(
                    HeatAcceptStatus.Failed,
                    command.Batch.RecordCount,
                    0,
                    command.Batch.Sequence,
                    IsStorageFailure(exception) ? "Heat storage unavailable" : "Heat commit failed"));
        }
    }

    private static async Task<HeatAcceptResult> PersistAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ChhtBatch batch,
        CancellationToken cancellationToken)
    {
        var epoch = UInt64Bytes(batch.Epoch);
        var start = UInt64Bytes(batch.Sequence);
        var end = UInt64Bytes(batch.EndSequence);

        await using (var replay = connection.CreateCommand())
        {
            replay.Transaction = transaction;
            replay.CommandText =
                "SELECT end_sequence, payload_sha256, inserted_count FROM receipts " +
                "WHERE crawler_id=$crawler AND epoch=$epoch AND start_sequence=$start";
            replay.Parameters.AddWithValue("$crawler", batch.CrawlerId);
            replay.Parameters.AddWithValue("$epoch", epoch);
            replay.Parameters.AddWithValue("$start", start);
            await using var reader = await replay.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                var same = ((byte[])reader[0]).AsSpan().SequenceEqual(end) &&
                           ((byte[])reader[1]).AsSpan().SequenceEqual(batch.PayloadSha256);
                var inserted = reader.GetInt32(2);
                return same
                    ? new HeatAcceptResult(HeatAcceptStatus.Replay, batch.RecordCount, inserted, batch.EndSequence + 1)
                    : new HeatAcceptResult(HeatAcceptStatus.Conflict, batch.RecordCount, 0, batch.Sequence, "Sequence was already committed with a different payload");
            }
        }

        await using (var completed = connection.CreateCommand())
        {
            completed.Transaction = transaction;
            completed.CommandText = "SELECT 1 FROM completions WHERE crawler_id=$crawler";
            completed.Parameters.AddWithValue("$crawler", batch.CrawlerId);
            if (await completed.ExecuteScalarAsync(cancellationToken) is not null)
                return new HeatAcceptResult(
                    HeatAcceptStatus.Conflict, batch.RecordCount, 0, batch.Sequence,
                    "Crawler UTC day was already completed");
        }

        var expected = batch.Sequence;
        await using (var head = connection.CreateCommand())
        {
            head.Transaction = transaction;
            head.CommandText = "SELECT next_sequence FROM receipt_heads WHERE crawler_id=$crawler AND epoch=$epoch";
            head.Parameters.AddWithValue("$crawler", batch.CrawlerId);
            head.Parameters.AddWithValue("$epoch", epoch);
            var scalar = await head.ExecuteScalarAsync(cancellationToken);
            if (scalar is byte[] bytes) expected = ReadUInt64(bytes);
        }
        if (batch.Sequence != expected)
        {
            return new HeatAcceptResult(HeatAcceptStatus.Conflict, batch.RecordCount, 0, expected, "Non-contiguous heat sequence");
        }

        var insertedCount = 0;
        await using var insertHash = connection.CreateCommand();
        insertHash.Transaction = transaction;
        insertHash.CommandText = "INSERT OR IGNORE INTO hashes(info_hash) VALUES ($hash)";
        var insertHashParameter = insertHash.Parameters.Add("$hash", SqliteType.Blob);
        await using var findHash = connection.CreateCommand();
        findHash.Transaction = transaction;
        findHash.CommandText = "SELECT hash_id FROM hashes WHERE info_hash=$hash";
        var findHashParameter = findHash.Parameters.Add("$hash", SqliteType.Blob);
        await using var insertSeen = connection.CreateCommand();
        insertSeen.Transaction = transaction;
        insertSeen.CommandText = "INSERT OR IGNORE INTO seen(hash_id, actor) VALUES ($hash_id, $actor)";
        var seenHashParameter = insertSeen.Parameters.Add("$hash_id", SqliteType.Integer);
        var seenActorParameter = insertSeen.Parameters.Add("$actor", SqliteType.Integer);

        foreach (var group in batch.Groups)
        {
            insertHashParameter.Value = group.InfoHash;
            await insertHash.ExecuteNonQueryAsync(cancellationToken);
            findHashParameter.Value = group.InfoHash;
            var hashId = (long)(await findHash.ExecuteScalarAsync(cancellationToken)
                ?? throw new InvalidDataException("SQLite hash dictionary lookup failed"));
            seenHashParameter.Value = hashId;
            foreach (var actor in group.ActorFingerprints)
            {
                seenActorParameter.Value = actor;
                insertedCount += await insertSeen.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        var next = checked(batch.EndSequence + 1);
        await using (var receipt = connection.CreateCommand())
        {
            receipt.Transaction = transaction;
            receipt.CommandText =
                "INSERT INTO receipts(crawler_id,epoch,start_sequence,end_sequence,payload_sha256,inserted_count) " +
                "VALUES($crawler,$epoch,$start,$end,$digest,$inserted)";
            receipt.Parameters.AddWithValue("$crawler", batch.CrawlerId);
            receipt.Parameters.AddWithValue("$epoch", epoch);
            receipt.Parameters.AddWithValue("$start", start);
            receipt.Parameters.AddWithValue("$end", end);
            receipt.Parameters.AddWithValue("$digest", batch.PayloadSha256);
            receipt.Parameters.AddWithValue("$inserted", insertedCount);
            await receipt.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var head = connection.CreateCommand())
        {
            head.Transaction = transaction;
            head.CommandText =
                "INSERT INTO receipt_heads(crawler_id,epoch,next_sequence) VALUES($crawler,$epoch,$next) " +
                "ON CONFLICT(crawler_id,epoch) DO UPDATE SET next_sequence=excluded.next_sequence";
            head.Parameters.AddWithValue("$crawler", batch.CrawlerId);
            head.Parameters.AddWithValue("$epoch", epoch);
            head.Parameters.AddWithValue("$next", UInt64Bytes(next));
            await head.ExecuteNonQueryAsync(cancellationToken);
        }

        return new HeatAcceptResult(HeatAcceptStatus.Accepted, batch.RecordCount, insertedCount, next);
    }

    private static async Task<HeatCompletionResult> PersistCompletionAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ChhtCompletion value,
        CancellationToken cancellationToken)
    {
        var epoch = UInt64Bytes(value.Epoch);
        var start = UInt64Bytes(value.StartSequence);
        var next = UInt64Bytes(value.NextSequence);
        await using (var replay = connection.CreateCommand())
        {
            replay.Transaction = transaction;
            replay.CommandText =
                "SELECT epoch,start_sequence,next_sequence,clean FROM completions WHERE crawler_id=$crawler";
            replay.Parameters.AddWithValue("$crawler", value.CrawlerId);
            await using var reader = await replay.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                var same = ((byte[])reader[0]).AsSpan().SequenceEqual(epoch) &&
                           ((byte[])reader[1]).AsSpan().SequenceEqual(start) &&
                           ((byte[])reader[2]).AsSpan().SequenceEqual(next) && reader.GetInt64(3) == 1;
                return same
                    ? new HeatCompletionResult(HeatCompletionStatus.Replay)
                    : new HeatCompletionResult(HeatCompletionStatus.Conflict,
                        "Crawler UTC day completion conflicts with committed evidence");
            }
        }

        var receiptCount = 0;
        var expected = value.StartSequence;
        await using (var receipts = connection.CreateCommand())
        {
            receipts.Transaction = transaction;
            receipts.CommandText =
                "SELECT epoch,start_sequence,end_sequence FROM receipts " +
                "WHERE crawler_id=$crawler ORDER BY epoch,start_sequence";
            receipts.Parameters.AddWithValue("$crawler", value.CrawlerId);
            await using var reader = await receipts.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!((byte[])reader[0]).AsSpan().SequenceEqual(epoch))
                    return new HeatCompletionResult(HeatCompletionStatus.Conflict,
                        "Crawler UTC day has receipts from another spool epoch");
                var receiptStart = ReadUInt64((byte[])reader[1]);
                var receiptEnd = ReadUInt64((byte[])reader[2]);
                if (receiptStart != expected || receiptEnd < receiptStart || receiptEnd == ulong.MaxValue)
                    return new HeatCompletionResult(HeatCompletionStatus.Conflict,
                        "Crawler UTC day receipt chain is not contiguous from completion start");
                expected = receiptEnd + 1;
                receiptCount++;
            }
        }
        if ((receiptCount == 0 && value.StartSequence != value.NextSequence) ||
            (receiptCount > 0 && expected != value.NextSequence))
            return new HeatCompletionResult(HeatCompletionStatus.Conflict,
                "Crawler UTC day receipt chain does not end at completion next sequence");

        await using (var heads = connection.CreateCommand())
        {
            heads.Transaction = transaction;
            heads.CommandText =
                "SELECT epoch,next_sequence FROM receipt_heads WHERE crawler_id=$crawler";
            heads.Parameters.AddWithValue("$crawler", value.CrawlerId);
            await using var reader = await heads.ExecuteReaderAsync(cancellationToken);
            var headCount = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                headCount++;
                if (!((byte[])reader[0]).AsSpan().SequenceEqual(epoch) ||
                    ReadUInt64((byte[])reader[1]) != value.NextSequence)
                    return new HeatCompletionResult(HeatCompletionStatus.Conflict,
                        "Crawler UTC day receipt head does not match completion");
            }
            if (headCount != (receiptCount == 0 ? 0 : 1))
                return new HeatCompletionResult(HeatCompletionStatus.Conflict,
                    "Crawler UTC day receipt head is missing or ambiguous");
        }

        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText =
            "INSERT INTO completions(crawler_id,epoch,start_sequence,next_sequence,clean) " +
            "VALUES($crawler,$epoch,$start,$next,1)";
        insert.Parameters.AddWithValue("$crawler", value.CrawlerId);
        insert.Parameters.AddWithValue("$epoch", epoch);
        insert.Parameters.AddWithValue("$start", start);
        insert.Parameters.AddWithValue("$next", next);
        await insert.ExecuteNonQueryAsync(cancellationToken);
        return new HeatCompletionResult(HeatCompletionStatus.Accepted);
    }

    internal static async Task<SqliteConnection> OpenAsync(string path, CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Private,
            Pooling = false
        }.ToString());
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA foreign_keys=ON;
            PRAGMA busy_timeout=5000;
            CREATE TABLE IF NOT EXISTS hashes (
                hash_id INTEGER PRIMARY KEY,
                info_hash BLOB NOT NULL UNIQUE CHECK(length(info_hash)=20)
            );
            CREATE TABLE IF NOT EXISTS seen (
                hash_id INTEGER NOT NULL,
                actor INTEGER NOT NULL,
                PRIMARY KEY(hash_id,actor),
                FOREIGN KEY(hash_id) REFERENCES hashes(hash_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS receipt_heads (
                crawler_id TEXT NOT NULL,
                epoch BLOB NOT NULL CHECK(length(epoch)=8),
                next_sequence BLOB NOT NULL CHECK(length(next_sequence)=8),
                PRIMARY KEY(crawler_id,epoch)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS receipts (
                crawler_id TEXT NOT NULL,
                epoch BLOB NOT NULL CHECK(length(epoch)=8),
                start_sequence BLOB NOT NULL CHECK(length(start_sequence)=8),
                end_sequence BLOB NOT NULL CHECK(length(end_sequence)=8),
                payload_sha256 BLOB NOT NULL CHECK(length(payload_sha256)=32),
                inserted_count INTEGER NOT NULL CHECK(inserted_count>=0),
                PRIMARY KEY(crawler_id,epoch,start_sequence)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS completions (
                crawler_id TEXT NOT NULL PRIMARY KEY,
                epoch BLOB NOT NULL CHECK(length(epoch)=8),
                start_sequence BLOB NOT NULL CHECK(length(start_sequence)=8),
                next_sequence BLOB NOT NULL CHECK(length(next_sequence)=8),
                clean INTEGER NOT NULL CHECK(clean=1)
            ) WITHOUT ROWID;
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
        return connection;
    }

    private static byte[] UInt64Bytes(ulong value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, value);
        return bytes;
    }

    private static ulong ReadUInt64(byte[] value)
    {
        if (value.Length != 8) throw new InvalidDataException("Invalid SQLite uint64 receipt value");
        return BinaryPrimitives.ReadUInt64BigEndian(value);
    }

    private static bool IsStorageFailure(Exception exception) =>
        exception is IOException or UnauthorizedAccessException ||
        exception is SqliteException sqlite && sqlite.SqliteErrorCode is 10 or 11 or 13 or 26;
}
