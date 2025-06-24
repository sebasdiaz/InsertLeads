// Services/ILeadFetcher.cs
using LeadToBus.Models;

namespace LeadToBus.Services
{
    public interface ILeadFetcher
    {
        List<LeadDto> GetLeadsToExport();
    }
}
