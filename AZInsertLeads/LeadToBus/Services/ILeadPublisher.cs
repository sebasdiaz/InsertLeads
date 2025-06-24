// Services/ILeadPublisher.cs
using LeadToBus.Models;

namespace LeadToBus.Services
{
    public interface ILeadPublisher
    {
        Task<bool> TrySendAsync(List<LeadDto> leads, int batchIndex);
    }
}
