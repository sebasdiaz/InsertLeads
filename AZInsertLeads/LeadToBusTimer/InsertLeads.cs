using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using static Grpc.Core.Metadata;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace AZTimerInsertLeads
{
    public class InsertLeads
    {
        private readonly ILogger _logger;

        public InsertLeads(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<InsertLeads>();
        }

        [Function("InsertLeads")]
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

            var messages = new List<ServiceBusMessage>();

            var jsonLeadsList = new List<object>();

            var successfullySentLeadIds = new List<Guid>();

            if (crmSvc.IsReady)
            {
                _logger.LogInformation("Connected to Dataverse.");
                int pageNumber = 1;
                string pagingCookie = null;
                bool moreRecords = true;
                int recordsCount = 0;
                int globalBatchCount = 0;

                var query_fax = "S";
                var query = new QueryExpression("lead");
                query.ColumnSet.AddColumns(
                    "a365_businessfacilityid",
                    "a365_businessid",
                    "a365_devicebrandid",
                    "a365_deviceclassid",
                    "a365_leadscore_date",
                    "a365_leadscore_state",
                    "address1_addressid",
                    "address1_addresstypecode",
                    "address2_addresstypecode",
                    "axx_asesorprospecto",
                    "axx_canaldeingreso",
                    "axx_cita",
                    "axx_clienteasiste",
                    "axx_direccionsucursaloculto",
                    "axx_empresaorigen",
                    "axx_fechaasignaciongestor",
                    "axx_gestorasignado",
                    "axx_istestdrive",
                    "axx_logasignacion",
                    "axx_marcaoculto",
                    "axx_mediodecontactopreferido",
                    "axx_metodoasignacionlead",
                    "axx_motherlastname",
                    "axx_nit",
                    "axx_nrodocumentotributario",
                    "axx_observacioneslvehiculo",
                    "axx_origenprospecto",
                    "axx_ownerpuestooculto",
                    "axx_paisorigen",
                    "axx_porquemedioseentero",
                    "axx_residente",
                    "axx_sucursal",
                    "axx_telefonoowneroculto",
                    "axx_tipodocumentoidentificador",
                    "axx_tipodocumentotributario",
                    "axx_url_referral",
                    "axx_utm_campaign",
                    "axx_utm_content",
                    "axx_utm_medium",
                    "axx_utm_source",
                    "axx_utm_term",
                    "cdi_enviadofabrica",
                    "cdi_idfabrica",
                    "description",
                    "emailaddress1",
                    "firstname",
                    "fullname",
                    "lastname",
                    "leadid",
                    "leadqualitycode",
                    "msauto_interest",
                    "msdyn_ordertype",
                    "msdyn_segmentid",
                    "telephone1",
                    "ownerid");
                /*
                var query_axx_asesorprospectoid = "06a58f30-c2cb-ee11-9079-000d3a1d5696";
                query.Criteria.AddCondition("axx_asesorprospecto", ConditionOperator.Equal, query_axx_asesorprospectoid);
                */
                query.Criteria.AddCondition("fax", ConditionOperator.NotEqual, query_fax);
                query.AddOrder("modifiedon", OrderType.Descending);
                query.PageInfo = new PagingInfo
                {
                    Count = 60,
                    PageNumber = pageNumber,
                    PagingCookie = pagingCookie
                };

                EntityCollection leads = crmSvc.RetrieveMultiple(query);

                if (leads.Entities.Any())
                {
                    _logger.LogInformation($"Leads found: {leads.Entities.Count}");
                    foreach (var lead in leads.Entities)
                    {
                        var leadId = lead.GetAttributeValue<Guid>("leadid");

                        var leadName = lead.GetAttributeValue<string>("fullname");
                        var leadEmail = lead.GetAttributeValue<string>("emailaddress1");
                        var leadPhone = lead.GetAttributeValue<string>("telephone1");
                        var leadBusinessFacilityId = lead.GetAttributeValue<EntityReference>("a365_businessfacilityid");
                        var leadBusinessFacilityName = string.Empty;
                        if (leadBusinessFacilityId != null)
                        {
                            leadBusinessFacilityName = leadBusinessFacilityId.Name;
                        }
                        var leadBusinessId = lead.GetAttributeValue<EntityReference>("a365_businessid");
                        var leadBusinessName = string.Empty;
                        if (leadBusinessId != null)
                        {
                            leadBusinessName = leadBusinessId.Name;
                        }
                        var leadDeviceBrandId = lead.GetAttributeValue<EntityReference>("a365_devicebrandid");
                        var leadDeviceBrandName = string.Empty;
                        if (leadDeviceBrandId != null)
                        {
                            leadDeviceBrandName = leadDeviceBrandId.Name;
                        }
                        var leadDeviceClassId = lead.GetAttributeValue<EntityReference>("a365_deviceclassid");
                        var leadDeviceClassName = string.Empty;
                        if (leadDeviceClassId != null)
                        {
                            leadDeviceClassName = leadDeviceClassId.Name;
                        }
                        var leadLeadScoreDate = lead.GetAttributeValue<DateTime>("a365_leadscore_date");
                        var leadLeadScoreState = lead.GetAttributeValue<int>("a365_leadscore_state");
                        var leadAsesorProspecto = lead.GetAttributeValue<EntityReference>("axx_asesorprospecto");
                        var leadAsesorProspectoName = string.Empty;
                        if (leadAsesorProspecto != null)
                        {
                            leadAsesorProspectoName = leadAsesorProspecto.Name;
                        }
                        var leadCanalDeIngreso = lead.GetAttributeValue<EntityReference>("axx_canaldeingreso");
                        var leadCanalDeIngresoName = string.Empty;
                        if (leadCanalDeIngreso != null)
                        {
                            leadCanalDeIngresoName = leadCanalDeIngreso.Name;
                        }
                        var leadCita = lead.GetAttributeValue<EntityReference>("axx_cita");
                        var leadCitaName = string.Empty;
                        if (leadCita != null)
                        {
                            leadCitaName = leadCita.Name;
                        }
                        var leadClienteAsiste = lead.GetAttributeValue<bool>("axx_clienteasiste");
                        var leadDireccionSucursalOculto = lead.GetAttributeValue<string>("axx_direccionsucursaloculto");
                        var leadEmpresaOrigen = lead.GetAttributeValue<OptionSetValue>("axx_empresaorigen");
                        var leadFechaAsignacionGestor = lead.GetAttributeValue<DateTime>("axx_fechaasignaciongestor");
                        var leadGestorAsignado = lead.GetAttributeValue<bool>("axx_gestorasignado");
                        var leadIsTestDrive = lead.GetAttributeValue<bool>("axx_istestdrive");
                        var leadLogAsignacion = lead.GetAttributeValue<string>("axx_logasignacion");
                        var leadMarcaOculto = lead.GetAttributeValue<string>("axx_marcaoculto");
                        var leadMedioDeContactoPreferido = lead.GetAttributeValue<OptionSetValue>("axx_mediodecontactopreferido");
                        var leadMetodoAsignacionLead = lead.GetAttributeValue<OptionSetValue>("axx_metodoasignacionlead");
                        var leadMotherLastName = lead.GetAttributeValue<string>("axx_motherlastname");
                        var leadNit = lead.GetAttributeValue<string>("axx_nit");
                        var leadNroDocumentoTributario = lead.GetAttributeValue<string>("axx_nrodocumentotributario");
                        var leadObservacionesVehiculo = lead.GetAttributeValue<string>("axx_observacioneslvehiculo");
                        var leadOrigenProspecto = lead.GetAttributeValue<OptionSetValue>("axx_origenprospecto");
                        var leadOwnerPuestoOculto = lead.GetAttributeValue<string>("axx_ownerpuestooculto");
                        var leadPaisOrigen = lead.GetAttributeValue<EntityReference>("axx_paisorigen");
                        var leadPaisOrigenName = string.Empty;
                        if (leadPaisOrigen != null)
                        {
                            leadPaisOrigenName = leadPaisOrigen.Name;
                        }
                        var leadPorQueMedioSeEntero = lead.GetAttributeValue<OptionSetValue>("axx_porquemedioseentero");
                        var leadResidente = lead.GetAttributeValue<bool>("axx_residente");
                        var leadSucursal = lead.GetAttributeValue<EntityReference>("axx_sucursal");
                        var leadSucursalName = string.Empty;
                        if (leadSucursal != null)
                        {
                            leadSucursalName = leadSucursal.Name;
                        }
                        var leadTelefonoOwnerOculto = lead.GetAttributeValue<string>("axx_telefonoowneroculto");
                        var leadTipoDocumentoIdentificador = lead.GetAttributeValue<OptionSetValue>("axx_tipodocumentoidentificador");
                        var leadTipoDocumentoTributario = lead.GetAttributeValue<OptionSetValue>("axx_tipodocumentotributario");
                        var leadUrlReferral = lead.GetAttributeValue<string>("axx_url_referral");
                        var leadUtmCampaign = lead.GetAttributeValue<string>("axx_utm_campaign");
                        var leadUtmContent = lead.GetAttributeValue<string>("axx_utm_content");
                        var leadUtmMedium = lead.GetAttributeValue<string>("axx_utm_medium");
                        var leadUtmSource = lead.GetAttributeValue<string>("axx_utm_source");
                        var leadUtmTerm = lead.GetAttributeValue<string>("axx_utm_term");
                        var leadEnviadoFabrica = lead.GetAttributeValue<bool>("cdi_enviadofabrica");
                        var leadIdFabrica = lead.GetAttributeValue<string>("cdi_idfabrica");
                        var leadDescription = lead.GetAttributeValue<string>("description");
                        var leadFirstName = lead.GetAttributeValue<string>("firstname");
                        var leadLastName = lead.GetAttributeValue<string>("lastname");
                        var leadLeadQualityCode = lead.GetAttributeValue<OptionSetValue>("leadqualitycode");
                        var leadMsaAutoInterest = lead.GetAttributeValue<OptionSetValue>("msauto_interest");
                        var leadMsdynOrderType = lead.GetAttributeValue<OptionSetValue>("msdyn_ordertype");
                        var leadMsdynSegmentId = lead.GetAttributeValue<EntityReference>("msdyn_segmentid");
                        var leadMsdynSegmentIdName = string.Empty;
                        if (leadMsdynSegmentId != null)
                        {
                            leadMsdynSegmentIdName = leadMsdynSegmentId.Name;
                        }
                        var leadOwnerId = lead.GetAttributeValue<EntityReference>("ownerid");
                        var leadOwnerIdName = string.Empty;
                        if (leadOwnerId != null)
                        {
                            leadOwnerIdName = leadOwnerId.Name;
                        }

                        var jsonLead = new
                        {
                            leadId = leadId,
                            leadName = leadName,
                            leadEmail = leadEmail,
                            leadPhone = leadPhone,
                            leadBusinessFacilityId = leadBusinessFacilityId,
                            leadBusinessFacilityName = leadBusinessFacilityName,
                            leadBusinessId = leadBusinessId,
                            leadBusinessName = leadBusinessName,
                            leadDeviceBrandId = leadDeviceBrandId,
                            leadDeviceBrandName = leadDeviceBrandName,
                            leadDeviceClassId = leadDeviceClassId,
                            leadDeviceClassName = leadDeviceClassName,
                            leadLeadScoreDate = leadLeadScoreDate,
                            leadLeadScoreState = leadLeadScoreState,
                            leadAsesorProspecto = leadAsesorProspecto,
                            leadAsesorProspectoName = leadAsesorProspectoName,
                            leadCanalDeIngreso = leadCanalDeIngreso,
                            leadCanalDeIngresoName = leadCanalDeIngresoName,
                            leadCita = leadCita,
                            leadCitaName = leadCitaName,
                            leadClienteAsiste = leadClienteAsiste,
                            leadDireccionSucursalOculto = leadDireccionSucursalOculto,
                            leadEmpresaOrigen = leadEmpresaOrigen,
                            leadFechaAsignacionGestor = leadFechaAsignacionGestor,
                            leadGestorAsignado = leadGestorAsignado,
                            leadIsTestDrive = leadIsTestDrive,
                            leadLogAsignacion = leadLogAsignacion,
                            leadMarcaOculto = leadMarcaOculto,
                            leadMedioDeContactoPreferido = leadMedioDeContactoPreferido,
                            leadMetodoAsignacionLead = leadMetodoAsignacionLead,
                            leadMotherLastName = leadMotherLastName,
                            leadNit = leadNit,
                            leadNroDocumentoTributario = leadNroDocumentoTributario,
                            leadObservacionesVehiculo = leadObservacionesVehiculo,
                            leadOrigenProspecto = leadOrigenProspecto,
                            leadOwnerPuestoOculto = leadOwnerPuestoOculto,
                            leadPaisOrigen = leadPaisOrigen,
                            leadPaisOrigenName = leadPaisOrigenName,
                            leadPorQueMedioSeEntero = leadPorQueMedioSeEntero,
                            leadResidente = leadResidente,
                            leadSucursal = leadSucursal,
                            leadSucursalName = leadSucursalName,
                            leadTelefonoOwnerOculto = leadTelefonoOwnerOculto,
                            leadTipoDocumentoIdentificador = leadTipoDocumentoIdentificador,
                            leadTipoDocumentoTributario = leadTipoDocumentoTributario,
                            leadUrlReferral = leadUrlReferral,
                            leadUtmCampaign = leadUtmCampaign,
                            leadUtmContent = leadUtmContent,
                            leadUtmMedium = leadUtmMedium,
                            leadUtmSource = leadUtmSource,
                            leadUtmTerm = leadUtmTerm,
                            leadEnviadoFabrica = leadEnviadoFabrica,
                            leadIdFabrica = leadIdFabrica,
                            leadDescription = leadDescription,
                            leadFirstName = leadFirstName,
                            leadLastName = leadLastName,
                            leadLeadQualityCode = leadLeadQualityCode,
                            leadMsaAutoInterest = leadMsaAutoInterest,
                            leadMsdynOrderType = leadMsdynOrderType,
                            leadMsdynSegmentId = leadMsdynSegmentId,
                            leadMsdynSegmentIdName = leadMsdynSegmentIdName,
                            leadOwnerId = leadOwnerId,
                            leadOwnerIdName = leadOwnerIdName
                        };

                        _logger.LogInformation($"{recordsCount++.ToString()}, Lead ID: {leadId}, Name: {leadName}, Email: {leadEmail}, Phone: {leadPhone}");

                        jsonLeadsList.Add(jsonLead);
                    }

                    int batchSize = 60; // configurable
                    var batches = ChunkBy(jsonLeadsList, batchSize);

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
            else
            {
                _logger.LogError("Failed to connect to Dataverse.");
            }

            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }
        }
        public static IEnumerable<List<T>> ChunkBy<T>(List<T> source, int chunkSize)
        {
            for (int i = 0; i < source.Count; i += chunkSize)
            {
                yield return source.GetRange(i, Math.Min(chunkSize, source.Count - i));
            }
        }

        public static void setExportedLead(List<Guid> successfullySentLeadIds, ServiceClient crmSvc, ILogger logger)
        {
            if (successfullySentLeadIds.Any())
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

                foreach (var leadId in successfullySentLeadIds)
                {
                    var update = new Entity("lead", leadId);
                    update["fax"] = "S";

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
}
