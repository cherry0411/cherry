namespace Cherry.Infrastructure.Heat;

public sealed class HeatProjectionStatusCache
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private (DateOnly? Day, int CoverageMask) _value;
    private DateTime _expiresAt;

    public async Task<(DateOnly? Day, int CoverageMask)> GetAsync(
        Func<CancellationToken, Task<(DateOnly? Day, int CoverageMask)>> loader,
        CancellationToken cancellationToken)
    {
        if (DateTime.UtcNow < _expiresAt) return _value;
        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (DateTime.UtcNow < _expiresAt) return _value;
            _value = await loader(cancellationToken);
            _expiresAt = DateTime.UtcNow.AddSeconds(30);
            return _value;
        }
        finally { _gate.Release(); }
    }

    public void Set(DateOnly day, int coverageMask)
    {
        _value = (day, coverageMask);
        _expiresAt = DateTime.UtcNow.AddSeconds(30);
    }
}
