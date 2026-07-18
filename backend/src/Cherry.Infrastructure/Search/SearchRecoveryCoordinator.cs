namespace Cherry.Infrastructure.Search;

/// <summary>
/// Lets ordinary metadata/heat projection passes run concurrently while giving
/// the rare destructive index recovery operation an exclusive in-process gate.
/// The storage deployment runs one API replica; PostgreSQL generation fencing
/// remains the durable protection for retries and process crashes.
/// </summary>
public sealed class SearchRecoveryCoordinator
{
    private readonly object _gate = new();
    private readonly SemaphoreSlim _recoverySerial = new(1, 1);
    private TaskCompletionSource<bool> _changed = NewSignal();
    private int _activeProjections;
    private bool _recoveryRequested;

    public async ValueTask<IAsyncDisposable> EnterProjectionAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            Task wait;
            lock (_gate)
            {
                if (!_recoveryRequested)
                {
                    _activeProjections++;
                    return new Lease(ReleaseProjection);
                }
                wait = _changed.Task;
            }
            await wait.WaitAsync(cancellationToken);
        }
    }

    public async ValueTask<IAsyncDisposable> EnterRecoveryAsync(CancellationToken cancellationToken)
    {
        await _recoverySerial.WaitAsync(cancellationToken);
        try
        {
            while (true)
            {
                Task wait;
                lock (_gate)
                {
                    if (!_recoveryRequested)
                    {
                        _recoveryRequested = true;
                        Pulse();
                    }
                    if (_activeProjections == 0)
                        return new Lease(ReleaseRecovery);
                    wait = _changed.Task;
                }
                await wait.WaitAsync(cancellationToken);
            }
        }
        catch
        {
            lock (_gate)
            {
                _recoveryRequested = false;
                Pulse();
            }
            _recoverySerial.Release();
            throw;
        }
    }

    private void ReleaseProjection()
    {
        lock (_gate)
        {
            if (_activeProjections <= 0)
                throw new InvalidOperationException("Search projection lease underflow");
            _activeProjections--;
            Pulse();
        }
    }

    private void ReleaseRecovery()
    {
        lock (_gate)
        {
            _recoveryRequested = false;
            Pulse();
        }
        _recoverySerial.Release();
    }

    private void Pulse()
    {
        var completed = _changed;
        _changed = NewSignal();
        completed.TrySetResult(true);
    }

    private static TaskCompletionSource<bool> NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class Lease(Action release) : IAsyncDisposable
    {
        private Action? _release = release;

        public ValueTask DisposeAsync()
        {
            Interlocked.Exchange(ref _release, null)?.Invoke();
            return ValueTask.CompletedTask;
        }
    }
}
