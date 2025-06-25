using Microsoft.AspNetCore.Http;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using System.Text.Json;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace WhatsAppToBus;

public class WhatsAppToBus
{
    private readonly ILogger _logger;

    public WhatsAppToBus(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<WhatsAppToBus>();
    }

    [Function("WhatsAppToBus")]
    public async Task Run([TimerTrigger("%TimerSchedule%", RunOnStartup = true)] TimerInfo myTimer)
    {
        _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

        string conn = Environment.GetEnvironmentVariable("sourceCRMConn");

        string connPreProd2 = Environment.GetEnvironmentVariable("destinationCRMConn");

        ServiceClient crmSvc = new ServiceClient(conn);

        string serviceBusConnectionString = Environment.GetEnvironmentVariable("ServiceBusConnectionString");
        string queueName = Environment.GetEnvironmentVariable("ServiceBusQueueName");

        ServiceBusClient client = new ServiceBusClient(serviceBusConnectionString);
        ServiceBusSender sender = client.CreateSender(queueName);

        var jsonWhatsppList = new List<object>();

        if (crmSvc.IsReady)
        {
            int pageNumber = 1;
            string pagingCookie = null;
            bool moreRecords = true;
            int recordsCount = 0;
            int globalBatchCount = 0;

            // Instantiate QueryExpression query
            var query = new QueryExpression("axx_whatsapp");
            query.TopCount = 50;

            // Add columns to query.ColumnSet
            query.ColumnSet.AddColumns(
                "axx_atomresponse",
                "axx_telephone",
                "axx_templateid",
                "regardingobjectid",
                "subject",
                "activitytypecode",
                "description",
                "exchangeweblink",
                "activityid",
                "createdon",
                "modifiedon",
                "ownerid");

            // Add conditions to query.Criteria
            query.Criteria.AddCondition("exchangeweblink", ConditionOperator.Null);
            query.AddOrder("modifiedon", OrderType.Descending);
            query.PageInfo = new PagingInfo
            {
                Count = 60,
                PageNumber = pageNumber,
                PagingCookie = pagingCookie
            };

            EntityCollection whatsapps = crmSvc.RetrieveMultiple(query);

            foreach (var whatsapp in whatsapps.Entities)
            {
                var activityId = whatsapp.GetAttributeValue<Guid>("activityid");
                var regardingObjectId = whatsapp.GetAttributeValue<EntityReference>("regardingobjectid")?.Id ?? Guid.Empty;
                var axx_atomResponse = whatsapp.GetAttributeValue<string>("axx_atomresponse") ?? string.Empty;
                var axx_telephone = whatsapp.GetAttributeValue<string>("axx_telephone") ?? string.Empty;
                var axx_templateId = whatsapp.GetAttributeValue<EntityReference>("axx_templateid")?.Id ?? Guid.Empty;
                var subject = whatsapp.GetAttributeValue<string>("subject") ?? string.Empty;
                var description = whatsapp.GetAttributeValue<string>("description") ?? string.Empty;
                var exchangeWebLink = whatsapp.GetAttributeValue<string>("exchangeweblink") ?? string.Empty;
                var createdOn = whatsapp.GetAttributeValue<DateTime>("createdon");
                var modifiedOn = whatsapp.GetAttributeValue<DateTime>("modifiedon");
                var ownerId = whatsapp.GetAttributeValue<EntityReference>("ownerid")?.Id ?? Guid.Empty;

                var whatsappData = new
                {
                    ActivityId = activityId,
                    RegardingObjectId = regardingObjectId,
                    AxxAtomResponse = axx_atomResponse,
                    AxxTelephone = axx_telephone,
                    AxxTemplateId = axx_templateId,
                    Subject = subject,
                    Description = description,
                    ExchangeWebLink = exchangeWebLink,
                    CreatedOn = createdOn,
                    ModifiedOn = modifiedOn,
                    OwnerId = ownerId
                };
                
                jsonWhatsppList.Add(whatsappData);


            }
            int batchSize = 60; // configurable
            var batches = ChunkBy(jsonWhatsppList, batchSize);
            foreach (var batch in batches)
            {
                try
                {
                    string jsonBatch = JsonSerializer.Serialize(batch);

                    // Validar tamaño (opcional)
                    if (System.Text.Encoding.UTF8.GetByteCount(jsonBatch) > 256000)
                    {
                        _logger.LogWarning($"Batch {globalBatchCount} supera límite de tamaño. Considerar reducir batchSize o dividir más.");
                        continue;
                    }

                    var message = new ServiceBusMessage(jsonBatch)
                    {
                        ContentType = "application/json",
                        MessageId = Guid.NewGuid().ToString()
                    };

                    await sender.SendMessageAsync(message);
                    _logger.LogInformation($"Batch {globalBatchCount} enviado con {batch.Count} leads.");

                    // Obtener los IDs de este batch
                    var sentIds = batch
                        .Select(lead => Guid.Parse(lead.GetType().GetProperty("leadId")?.GetValue(lead)?.ToString() ?? Guid.Empty.ToString()))
                        .Where(id => id != Guid.Empty)
                        .ToList();

                    // Ejecutar actualización en origen
                    setExportedLead(sentIds, crmSvc, _logger);

                    await Task.Delay(5000); // cinco segundos entre lotes

                    globalBatchCount++;
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error al enviar batch {globalBatchCount}: {ex.Message}");
                }
            }

        }

    }
    public static IEnumerable<List<T>> ChunkBy<T>(List<T> source, int chunkSize)
    {
        for (int i = 0; i < source.Count; i += chunkSize)
        {
            yield return source.GetRange(i, Math.Min(chunkSize, source.Count - i));
        }
    }

    public static void setExportedWhatsapp(List<Guid> successfullySentIds, ServiceClient crmSvc, ILogger logger)
    {
        if (successfullySentIds.Any())
        {
            var updateRequests = new ExecuteMultipleRequest()
            {
                Settings = new ExecuteMultipleSettings
                {
                    ContinueOnError = true,
                    ReturnResponses = true
                },
                Requests = new OrganizationRequestCollection()
            };

            foreach (var whatsappId in successfullySentIds)
            {
                var update = new Entity("axx_whatsapp", whatsappId);
                update["exchangeweblink"] = "S";

                var updateRequest = new UpdateRequest { Target = update };
                updateRequests.Requests.Add(updateRequest);
            }

            try
            {
                var response = (ExecuteMultipleResponse)crmSvc.Execute(updateRequests);
                int failed = response.Responses.Count(r => r.Fault != null);
                int success = response.Responses.Count(r => r.Fault == null);

                logger.LogInformation($"ExecuteMultiple completed. Success: {success}, Failed: {failed}");

                foreach (var fault in response.Responses.Where(r => r.Fault != null))
                {
                    logger.LogError($"Update failed: {fault.Fault.Message}");
                }
            }
            catch (Exception ex)
            {
                logger.LogError($"ExecuteMultiple request failed: {ex.Message}");
            }
        }
    }
}