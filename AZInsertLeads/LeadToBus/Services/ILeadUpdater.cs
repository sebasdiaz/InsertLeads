
// Services/ILeadUpdater.cs
namespace LeadToBus.Services
{
    public interface ILeadUpdater
    {
        void MarkAsExported(List<Guid> leadIds);
    }
}