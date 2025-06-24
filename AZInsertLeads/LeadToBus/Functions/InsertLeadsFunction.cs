
// Functions/InsertLeadsFunction.cs
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using LeadToBus.Models;
using LeadToBus.Services;
using LeadToBus.Helpers;

namespace LeadToBus.Functions
{
    public class InsertLeadsFunction
    {
        private readonly ILogger _logger;
        private readonly ILeadFetcher _fetcher;
        private readonly ILeadPublisher _publisher;
        private readonly ILeadUpdater _updater;

        public InsertLeadsFunction(ILoggerFactory loggerFactory, ILeadFetcher fetcher, ILeadPublisher publisher, ILeadUpdater updater)
        {
            _logger = loggerFactory.CreateLogger<InsertLeadsFunction>();
            _fetcher = fetcher;
            _publisher = publisher;
            _updater = updater;
        }

        [Function("InsertLeads")]
        public async Task Run([TimerTrigger("%TimerSchedule%", RunOnStartup = true)] TimerInfo myTimer)
        {
            _logger.LogInformation($"InsertLeads triggered at: {DateTime.Now}");

            var leads = _fetcher.GetLeadsToExport();

            if (!leads.Any())
            {
                _logger.LogInformation("No leads to export.");
                return;
            }

            var batchCount = 0;
            foreach (var batch in ChunkHelper.ChunkBy(leads, Settings.BatchSize))
            {
                if (await _publisher.TrySendAsync(batch, batchCount))
                {
                    _updater.MarkAsExported(batch.Select(l => l.LeadId).ToList());
                }
                batchCount++;
                await Task.Delay(5000);
            }

            _logger.LogInformation($"Next run at: {myTimer.ScheduleStatus?.Next}");
        }
    }
}
