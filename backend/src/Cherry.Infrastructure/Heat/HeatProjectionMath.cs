namespace Cherry.Infrastructure.Heat;

public readonly record struct HeatVector(long Heat3d, long Heat7d, long Heat15d)
{
    public static HeatVector operator +(HeatVector left, HeatVector right) => new(
        checked(left.Heat3d + right.Heat3d),
        checked(left.Heat7d + right.Heat7d),
        checked(left.Heat15d + right.Heat15d));
}

public sealed record DailyHeatProjectionDocument(long Id, long Heat3d, long Heat7d, long Heat15d);
public sealed record HourlyHeatProjectionDocument(long Id, long Heat24h);

public static class HeatProjectionMath
{
    public static readonly int[] BoundaryOffsets = [0, 3, 7, 15];

    public static IReadOnlyList<DailyHeatProjectionDocument> BuildIncremental(
        DateOnly targetDay,
        short shard,
        IReadOnlyDictionary<DateOnly, IReadOnlyList<HeatFrameEntry>> frames)
    {
        var affected = new SortedSet<long>();
        foreach (var offset in BoundaryOffsets)
            if (frames.TryGetValue(targetDay.AddDays(-offset), out var entries))
                foreach (var entry in entries)
                    if ((entry.TorrentId & 63) == shard)
                        affected.Add(entry.TorrentId);

        var current = affected.ToDictionary(id => id, _ => new long[3]);
        var previous = affected.ToDictionary(id => id, _ => new long[3]);
        for (var offset = 0; offset <= 15; offset++)
        {
            if (!frames.TryGetValue(targetDay.AddDays(-offset), out var entries)) continue;
            foreach (var entry in entries)
            {
                if ((entry.TorrentId & 63) != shard || !affected.Contains(entry.TorrentId)) continue;
                if (offset <= 2) current[entry.TorrentId][0] = checked(current[entry.TorrentId][0] + entry.Count);
                if (offset <= 6) current[entry.TorrentId][1] = checked(current[entry.TorrentId][1] + entry.Count);
                if (offset <= 14) current[entry.TorrentId][2] = checked(current[entry.TorrentId][2] + entry.Count);
                if (offset is >= 1 and <= 3) previous[entry.TorrentId][0] = checked(previous[entry.TorrentId][0] + entry.Count);
                if (offset is >= 1 and <= 7) previous[entry.TorrentId][1] = checked(previous[entry.TorrentId][1] + entry.Count);
                if (offset is >= 1 and <= 15) previous[entry.TorrentId][2] = checked(previous[entry.TorrentId][2] + entry.Count);
            }
        }

        return affected
            .Where(id => !current[id].SequenceEqual(previous[id]))
            .Select(id => new DailyHeatProjectionDocument(id, current[id][0], current[id][1], current[id][2]))
            .ToArray();
    }

    public static IReadOnlyList<DailyHeatProjectionDocument> BuildFull(
        DateOnly targetDay,
        short shard,
        IReadOnlyDictionary<DateOnly, IReadOnlyList<HeatFrameEntry>> frames)
    {
        var values = new SortedDictionary<long, long[]>();
        for (var offset = 0; offset <= 14; offset++)
        {
            if (!frames.TryGetValue(targetDay.AddDays(-offset), out var entries)) continue;
            foreach (var entry in entries)
            {
                if ((entry.TorrentId & 63) != shard) continue;
                if (!values.TryGetValue(entry.TorrentId, out var heat))
                    values[entry.TorrentId] = heat = new long[3];
                if (offset <= 2) heat[0] = checked(heat[0] + entry.Count);
                if (offset <= 6) heat[1] = checked(heat[1] + entry.Count);
                heat[2] = checked(heat[2] + entry.Count);
            }
        }
        return values.Select(pair => new DailyHeatProjectionDocument(
            pair.Key, pair.Value[0], pair.Value[1], pair.Value[2])).ToArray();
    }
}
