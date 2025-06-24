
// Services/LeadUpdater.cs
using LeadToBus.Services;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;

namespace LeadToBus.Services
{
    public class LeadUpdater : ILeadUpdater
    {
        private readonly ServiceClient _crmSvc;
        private readonly ILogger<LeadUpdater> _logger;

        public LeadUpdater(ServiceClient crmSvc, ILogger<LeadUpdater> logger)
        {
            _crmSvc = crmSvc;
            _logger = logger;
        }

        public void MarkAsExported(List<Guid> leadIds)
        {
            var request = new ExecuteMultipleRequest
            {
                Settings = new ExecuteMultipleSettings
                {
                    ContinueOnError = true,
                    ReturnResponses = true
                },
                Requests = new OrganizationRequestCollection()
            };

            foreach (var id in leadIds)
            {
                var update = new Entity("lead", id) { ["fax"] = Settings.ExportedMark };
                request.Requests.Add(new UpdateRequest { Target = update });
            }

            try
            {
                var response = (ExecuteMultipleResponse)_crmSvc.Execute(request);
                int success = response.Responses.Count(r => r.Fault == null);
                int failed = response.Responses.Count(r => r.Fault != null);
                _logger.LogInformation($"Marked exported: {success}, Failed: {failed}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error marking leads as exported");
            }
        }
    }
}

