namespace Cherry.Infrastructure.Heat;

public readonly record struct HeatVector(long Heat1d, long Heat7d, long Heat15d, long Heat30d)
{
    public static HeatVector operator +(HeatVector left, HeatVector right) => new(
        checked(left.Heat1d + right.Heat1d),
        checked(left.Heat7d + right.Heat7d),
        checked(left.Heat15d + right.Heat15d),
        checked(left.Heat30d + right.Heat30d));
}

public sealed record HeatProjectionDocument(long Id, long Heat1d, long Heat7d, long Heat15d, long Heat30d);

public static class HeatProjectionMath
{
    public static readonly int[] BoundaryOffsets = [0, 1, 7, 15, 30];

    public static IReadOnlyList<HeatProjectionDocument> BuildIncremental(
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

        var current = affected.ToDictionary(id => id, _ => new long[4]);
        var previous = affected.ToDictionary(id => id, _ => new long[4]);
        for (var offset = 0; offset <= 30; offset++)
        {
            if (!frames.TryGetValue(targetDay.AddDays(-offset), out var entries)) continue;
            foreach (var entry in entries)
            {
                if ((entry.TorrentId & 63) != shard || !affected.Contains(entry.TorrentId)) continue;
                if (offset == 0) current[entry.TorrentId][0] = entry.Count;
                if (offset <= 6) current[entry.TorrentId][1] = checked(current[entry.TorrentId][1] + entry.Count);
                if (offset <= 14) current[entry.TorrentId][2] = checked(current[entry.TorrentId][2] + entry.Count);
                if (offset <= 29) current[entry.TorrentId][3] = checked(current[entry.TorrentId][3] + entry.Count);
                if (offset == 1) previous[entry.TorrentId][0] = entry.Count;
                if (offset is >= 1 and <= 7) previous[entry.TorrentId][1] = checked(previous[entry.TorrentId][1] + entry.Count);
                if (offset is >= 1 and <= 15) previous[entry.TorrentId][2] = checked(previous[entry.TorrentId][2] + entry.Count);
                if (offset is >= 1 and <= 30) previous[entry.TorrentId][3] = checked(previous[entry.TorrentId][3] + entry.Count);
            }
        }

        return affected
            .Where(id => !current[id].SequenceEqual(previous[id]))
            .Select(id => new HeatProjectionDocument(id, current[id][0], current[id][1], current[id][2], current[id][3]))
            .ToArray();
    }

    public static IReadOnlyList<HeatProjectionDocument> BuildFull(
        DateOnly targetDay,
        short shard,
        IReadOnlyDictionary<DateOnly, IReadOnlyList<HeatFrameEntry>> frames)
    {
        var values = new SortedDictionary<long, long[]>();
        for (var offset = 0; offset <= 29; offset++)
        {
            if (!frames.TryGetValue(targetDay.AddDays(-offset), out var entries)) continue;
            foreach (var entry in entries)
            {
                if ((entry.TorrentId & 63) != shard) continue;
                if (!values.TryGetValue(entry.TorrentId, out var heat))
                    values[entry.TorrentId] = heat = new long[4];
                if (offset == 0) heat[0] = entry.Count;
                if (offset <= 6) heat[1] = checked(heat[1] + entry.Count);
                if (offset <= 14) heat[2] = checked(heat[2] + entry.Count);
                heat[3] = checked(heat[3] + entry.Count);
            }
        }
        return values.Select(pair => new HeatProjectionDocument(
            pair.Key, pair.Value[0], pair.Value[1], pair.Value[2], pair.Value[3])).ToArray();
    }
}
