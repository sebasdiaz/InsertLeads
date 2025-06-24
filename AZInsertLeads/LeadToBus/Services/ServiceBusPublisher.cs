
// Services/ServiceBusPublisher.cs
using Azure.Messaging.ServiceBus;
using LeadToBus.Models;
using LeadToBus.Services;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace LeadToBus.Services
{
    public class ServiceBusPublisher : ILeadPublisher
    {
        private readonly ServiceBusSender _sender;
        private readonly ILogger<ServiceBusPublisher> _logger;

        public ServiceBusPublisher(ServiceBusSender sender, ILogger<ServiceBusPublisher> logger)
        {
            _sender = sender;
            _logger = logger;
        }

        public async Task<bool> TrySendAsync(List<LeadDto> leads, int batchIndex)
        {
            try
            {
                var json = JsonSerializer.Serialize(leads);
                var size = System.Text.Encoding.UTF8.GetByteCount(json);

                if (size > 256000)
                {
                    _logger.LogWarning($"Batch {batchIndex} exceeds size limit.");
                    return false;
                }

                await _sender.SendMessageAsync(new ServiceBusMessage(json)
                {
                    ContentType = "application/json",
                    MessageId = Guid.NewGuid().ToString()
                });

                _logger.LogInformation($"Batch {batchIndex} sent successfully.");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error sending batch {batchIndex}");
                return false;
            }
        }
    }
}