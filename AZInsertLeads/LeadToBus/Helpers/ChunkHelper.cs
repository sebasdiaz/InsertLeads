
// Helpers/ChunkHelper.cs
namespace LeadToBus.Helpers
{
    public static class ChunkHelper
    {
        public static IEnumerable<List<T>> ChunkBy<T>(List<T> source, int chunkSize)
        {
            for (int i = 0; i < source.Count; i += chunkSize)
                yield return source.GetRange(i, Math.Min(chunkSize, source.Count - i));
        }
    }
}