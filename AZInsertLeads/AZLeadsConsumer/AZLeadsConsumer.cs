using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace AZLeadsConsumer
{
    public class AZLeadsConsumer
    {
        private readonly ILogger _logger;

        public AZLeadsConsumer(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<AZLeadsConsumer>();
        }

        [FunctionName("ProcessLeads")]
        public async Task Run([TimerTrigger("%TimerSchedule%", RunOnStartup = true)] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            string connProd = Environment.GetEnvironmentVariable("destinationCRMConn");
            string serviceBusConnectionString = Environment.GetEnvironmentVariable("ServiceBusConnectionString");
            string queueName = Environment.GetEnvironmentVariable("ServiceBusQueueName");
            string destinationQueue = Environment.GetEnvironmentVariable("ServiceBusQueueDestination");

            var client = new ServiceBusClient(serviceBusConnectionString);
            var receiver = client.CreateReceiver(queueName);
            var sender = client.CreateSender(destinationQueue);

            ServiceClient crmSvc = new ServiceClient(connProd);
            if (!crmSvc.IsReady)
            {
                log.LogError("No se pudo conectar al entorno de destino.");
                return;
            }

            try
            {
                IReadOnlyList<ServiceBusReceivedMessage> messages = await receiver.ReceiveMessagesAsync(
                    maxMessages: 1,
                    maxWaitTime: TimeSpan.FromSeconds(300));

                log.LogInformation($"Recibidos {messages.Count} mensajes.");

                foreach (var message in messages)
                {
                    try
                    {
                        var leadEntities = await ProcesarLeadsDesdeQueue(message, crmSvc, log);
                        // No se completa el mensaje para que permanezca en la cola
                        log.LogInformation($"Mensaje procesado pero dejado en la cola: {message.MessageId}");
                        // Reenviar el mensaje
                        var forwardedMessage = new ServiceBusMessage(message.Body);
                        // Copiar propiedades
                        foreach (var prop in message.ApplicationProperties)
                        {
                            forwardedMessage.ApplicationProperties[prop.Key] = prop.Value;
                        }
                        // copiar también el MessageId
                        forwardedMessage.MessageId = message.MessageId;
                        // Enviar a la nueva cola
                        await sender.SendMessageAsync(forwardedMessage);
                        await receiver.CompleteMessageAsync(message);

                        var createRequests = new ExecuteMultipleRequest
                        {
                            Settings = new ExecuteMultipleSettings
                            {
                                ContinueOnError = true,
                                ReturnResponses = true
                            },
                            Requests = new OrganizationRequestCollection()
                        };

                        foreach (var entity in leadEntities)
                        {
                            var create = new CreateRequest { Target = entity };
                            createRequests.Requests.Add(create);
                        }

                        try
                        {
                            var response = (ExecuteMultipleResponse)crmSvc.Execute(createRequests);

                            for (int i = 0; i < response.Responses.Count; i++)
                            {
                                var item = response.Responses[i];
                                if (item.Fault != null)
                                {
                                    log.LogError($"Error insertando lead #{i}: {item.Fault.Message}");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            log.LogError($"Error en ExecuteMultiple: {ex.Message}");
                        }
                        // completar el mensaje original
                    }
                    catch (Exception ex)
                    {
                        log.LogError($"Error procesando mensaje: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                log.LogError($"Error general: {ex.Message}");
            }
            finally
            {
                await receiver.CloseAsync();
                await client.DisposeAsync();
            }

            log.LogInformation($"Función finalizada a las: {DateTime.Now}");
        }

        public async Task<List<Entity>> ProcesarLeadsDesdeQueue(ServiceBusReceivedMessage msg, ServiceClient crmSvc, ILogger logger)
        {
            var leadEntities = new List<Entity>();

            var asesorCache = new Dictionary<string, Guid>();
            var facilityCache = new Dictionary<string, Guid>();
            var businessCache = new Dictionary<string, Guid>();
            var canalCache = new Dictionary<string, Guid>();
            var citaCache = new Dictionary<string, Guid>();
            var paisCache = new Dictionary<string, Guid>();
            var brandCache = new Dictionary<string, Guid>();
            var classCache = new Dictionary<string, Guid>();
            var segmentCache = new Dictionary<string, Guid>();
            var sucursalCache = new Dictionary<string, Guid>();

            try
            {
                var body = msg.Body.ToString();
                var leads = JsonSerializer.Deserialize<List<LeadDto>>(body);

                foreach (var dto in leads)
                {
                    Entity lead = ConvertToEntity(dto, crmSvc,
                        asesorCache, facilityCache, businessCache, canalCache, citaCache,
                        paisCache, brandCache, classCache, segmentCache, sucursalCache);

                    leadEntities.Add(lead);
                }
            }
            catch (Exception ex)
            {
                logger.LogError($"Error deserializando mensaje: {ex.Message}");
            }
            return leadEntities;
        }

        /*
        public void InsertLead(Entity lead, Guid leadId)
        {
            var businessMsdynSegmentCache = new Dictionary<string, Guid>();
            var businessFacilityCache = new Dictionary<string, Guid>();
            var businessCache = new Dictionary<string, Guid>();
            var deviceBrandCache = new Dictionary<string, Guid>();
            var deviceClassCache = new Dictionary<string, Guid>();
            var asesorProspectoCache = new Dictionary<string, Guid>();
            var canalIngresoCache = new Dictionary<string, Guid>();
            var citaCache = new Dictionary<string, Guid>();
            var paisCache = new Dictionary<string, Guid>();
            var sucursalCache = new Dictionary<string, Guid>();
            var segmentCache = new Dictionary<string, Guid>();

            ServiceClient crmSvcPreProd2 = new ServiceClient(connPreProd2);

            if (crmSvcPreProd2.IsReady)
            {
                var isInsert = true;
                try
                {
                    Entity leadPreProd = crmSvcPreProd2.Retrieve("lead", leadId, new ColumnSet("leadid"));
                    if (leadPreProd != null)
                    {
                        isInsert = false;
                    }
                }
                catch { }
            }

            Entity newLead = new Entity("lead", leadId);

            newLead["emailaddress1"] = leadEmail;
            newLead["telephone1"] = leadPhone;
            newLead["firstname"] = leadFirstName;
            newLead["lastname"] = leadLastName;
            newLead["fullname"] = leadName;
            newLead["description"] = leadDescription;
            newLead["leadqualitycode"] = leadLeadQualityCode;
            newLead["msauto_interest"] = leadMsaAutoInterest;
            newLead["msdyn_ordertype"] = leadMsdynOrderType;

            if (!string.IsNullOrEmpty(leadMsdynSegmentIdName))
            {
                if (!businessMsdynSegmentCache.TryGetValue(leadMsdynSegmentIdName, out Guid id))
                {
                    EntityCollection segment = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("msdyn_segment")
                    {
                        ColumnSet = new ColumnSet("msdyn_name"),
                        Criteria = new FilterExpression
                        {
                            Conditions = { new ConditionExpression("msdyn_name", ConditionOperator.Equal, leadMsdynSegmentIdName) }
                        }
                    });
                    if (segment.Entities.Count > 0)
                    {
                        id = segment.Entities.First().GetAttributeValue<Guid>("msdyn_segmentid");
                        businessMsdynSegmentCache.Add(leadMsdynSegmentIdName, id);
                    }
                }
                newLead["msdyn_segmentid"] = new EntityReference("msdyn_segment", id);
            }

            if (!string.IsNullOrEmpty(leadBusinessFacilityName))
            {
                if (!businessFacilityCache.TryGetValue(leadBusinessFacilityName, out Guid id))
                {
                    EntityCollection businessFacility = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("msauto_businessfacility")
                    {
                        ColumnSet = new ColumnSet("msauto_name"),
                        Criteria = new FilterExpression
                        {
                            Conditions = { new ConditionExpression("msauto_name", ConditionOperator.Equal, leadBusinessFacilityName) }
                        }
                    });
                    if (businessFacility.Entities.Count > 0)
                    {
                        id = businessFacility.Entities.First().GetAttributeValue<Guid>("msauto_businessfacilityid");
                        businessFacilityCache.Add(leadBusinessFacilityName, id);
                    }
                }
                newLead["a365_businessfacilityid"] = new EntityReference("msauto_businessfacility", id);
            }

            if (!string.IsNullOrEmpty(leadBusinessName))
            {
                if (!businessCache.TryGetValue(leadBusinessName, out Guid id))
                {
                    EntityCollection business = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("msauto_business")
                    {
                        ColumnSet = new ColumnSet("msauto_name"),
                        Criteria = new FilterExpression
                        {
                            Conditions = { new ConditionExpression("msauto_name", ConditionOperator.Equal, leadBusinessName) }
                        }
                    });
                    if (business.Entities.Count > 0)
                    {
                        id = business.Entities.First().GetAttributeValue<Guid>("msauto_businessid");
                        businessCache.Add(leadBusinessName, id);
                    }
                }
                newLead["a365_businessid"] = new EntityReference("msauto_business", id);
            }

            if (!string.IsNullOrEmpty(leadDeviceBrandName))
            {
                if (!deviceBrandCache.TryGetValue(leadDeviceBrandName, out Guid id))
                {
                    EntityCollection deviceBrand = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("msauto_devicebrand")
                    {
                        ColumnSet = new ColumnSet("msauto_name"),
                        Criteria = new FilterExpression
                        {
                            Conditions = { new ConditionExpression("msauto_name", ConditionOperator.Equal, leadDeviceBrandName) }
                        }
                    });
                    if (deviceBrand.Entities.Count > 0)
                    {
                        id = deviceBrand.Entities.First().GetAttributeValue<Guid>("msauto_devicebrandid");
                        deviceBrandCache.Add(leadDeviceBrandName, id);
                    }
                }
                newLead["a365_devicebrandid"] = new EntityReference("msauto_devicebrand", id);
            }

            if (!string.IsNullOrEmpty(leadDeviceClassName))
            {
                if (!deviceClassCache.TryGetValue(leadDeviceClassName, out Guid id))
                {
                    EntityCollection deviceClass = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("msauto_deviceclass")
                    {
                        ColumnSet = new ColumnSet("msauto_name"),
                        Criteria = new FilterExpression
                        {
                            Conditions = { new ConditionExpression("msauto_name", ConditionOperator.Equal, leadDeviceClassName) }
                        }
                    });
                    if (deviceClass.Entities.Count > 0)
                    {
                        id = deviceClass.Entities.First().GetAttributeValue<Guid>("msauto_deviceclassid");
                        deviceClassCache.Add(leadDeviceClassName, id);
                    }
                }
                newLead["a365_deviceclassid"] = new EntityReference("msauto_deviceclass", id);
            }

            //newLead["a365_leadscore_date"] = leadLeadScoreDate;
            newLead["a365_leadscore_state"] = leadLeadScoreState;

            if (!string.IsNullOrEmpty(leadAsesorProspectoName))
            {
                if (!asesorProspectoCache.TryGetValue(leadAsesorProspectoName, out Guid id))
                {
                    EntityCollection asesorProspecto = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("systemuser")
                    {
                        ColumnSet = new ColumnSet("fullname"),
                        Criteria = new FilterExpression
                        {
                            Conditions = { new ConditionExpression("fullname", ConditionOperator.Equal, leadAsesorProspectoName) }
                        }
                    });
                    if (asesorProspecto.Entities.Count > 0)
                    {
                        id = asesorProspecto.Entities.First().GetAttributeValue<Guid>("systemuserid");
                        asesorProspectoCache.Add(leadAsesorProspectoName, id);
                    }
                }
                newLead["axx_asesorprospecto"] = new EntityReference("systemuser", id);
            }

            if (!string.IsNullOrEmpty(leadCanalDeIngresoName))
            {
                if (!canalIngresoCache.TryGetValue(leadCanalDeIngresoName, out Guid id))
                {
                    EntityCollection canalIngreso = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("axx_canalingreso")
                    {
                        ColumnSet = new ColumnSet("axx_name"),
                        Criteria = new FilterExpression
                        {
                            Conditions =
                                        {
                                    new ConditionExpression("axx_name", ConditionOperator.Equal, leadCanalDeIngresoName)
                                }
                        }
                    });
                    if (canalIngreso.Entities.Count > 0)
                    {
                        id = canalIngreso.Entities.First().GetAttributeValue<Guid>("axx_canalingresoid");
                        canalIngresoCache.Add(leadCanalDeIngresoName, id);
                    }
                }
                newLead["axx_canaldeingreso"] = new EntityReference("axx_canalingreso", id);
            }

            if (!string.IsNullOrEmpty(leadCitaName))
            {
                if (!citaCache.TryGetValue(leadCitaName, out Guid id))
                {
                    EntityCollection cita = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("appointment")
                    {
                        ColumnSet = new ColumnSet("subject"),
                        Criteria = new FilterExpression
                        {
                            Conditions =
                                        {
                                    new ConditionExpression("subject", ConditionOperator.Equal, leadCitaName)
                                }
                        }
                    });
                    if (cita.Entities.Count > 0)
                    {
                        id = cita.Entities.First().GetAttributeValue<Guid>("activityid");
                        citaCache.Add(leadCitaName, id);
                    }
                }
                newLead["axx_cita"] = new EntityReference("appointment", id);
            }

            newLead["axx_clienteasiste"] = leadClienteAsiste;
            newLead["axx_direccionsucursaloculto"] = leadDireccionSucursalOculto;
            newLead["axx_empresaorigen"] = leadEmpresaOrigen;
            newLead["axx_fechaasignaciongestor"] = leadFechaAsignacionGestor;
            newLead["axx_gestorasignado"] = leadGestorAsignado;
            newLead["axx_istestdrive"] = leadIsTestDrive;
            newLead["axx_logasignacion"] = leadLogAsignacion;
            newLead["axx_marcaoculto"] = leadMarcaOculto;
            newLead["axx_mediodecontactopreferido"] = leadMedioDeContactoPreferido;
            newLead["axx_metodoasignacionlead"] = leadMetodoAsignacionLead;
            newLead["axx_motherlastname"] = leadMotherLastName;
            newLead["axx_nit"] = leadNit;
            newLead["axx_nrodocumentotributario"] = leadNroDocumentoTributario;
            newLead["axx_observacioneslvehiculo"] = leadObservacionesVehiculo;
            newLead["axx_origenprospecto"] = leadOrigenProspecto;
            newLead["axx_ownerpuestooculto"] = leadOwnerPuestoOculto;
            if (!string.IsNullOrEmpty(leadPaisOrigenName))
            {
                if (!paisCache.TryGetValue(leadPaisOrigenName, out Guid id))
                {
                    EntityCollection pais = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("axx_pais")
                    {
                        ColumnSet = new ColumnSet("axx_name"),
                        Criteria = new FilterExpression
                        {
                            Conditions =
                                        {
                                    new ConditionExpression("axx_name", ConditionOperator.Equal, leadPaisOrigenName)
                                }
                        }
                    });
                    if (pais.Entities.Count > 0)
                    {
                        id = pais.Entities.First().GetAttributeValue<Guid>("axx_paisid");
                        paisCache.Add(leadPaisOrigenName, id);
                    }
                }
                newLead["axx_paisorigen"] = new EntityReference("axx_pais", id);

            }
            newLead["axx_porquemedioseentero"] = leadPorQueMedioSeEntero;
            newLead["axx_residente"] = leadResidente;
            if (!string.IsNullOrEmpty(leadSucursalName))
            {
                if (!sucursalCache.TryGetValue(leadSucursalName, out Guid id))
                {
                    EntityCollection sucursal = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("axx_sucursal")
                    {
                        ColumnSet = new ColumnSet("axx_name"),
                        Criteria = new FilterExpression
                        {
                            Conditions = { new ConditionExpression("axx_name", ConditionOperator.Equal, leadSucursalName) }
                        }
                    });
                    if (sucursal.Entities.Count > 0)
                    {
                        id = sucursal.Entities.First().GetAttributeValue<Guid>("axx_sucursalid");
                        sucursalCache.Add(leadSucursalName, id);
                    }
                }
                newLead["axx_sucursal"] = new EntityReference("axx_sucursal", id);
            }
            newLead["axx_telefonoowneroculto"] = leadTelefonoOwnerOculto;
            newLead["axx_tipodocumentoidentificador"] = leadTipoDocumentoIdentificador;
            newLead["axx_tipodocumentotributario"] = leadTipoDocumentoTributario;
            newLead["axx_url_referral"] = leadUrlReferral;
            newLead["axx_utm_campaign"] = leadUtmCampaign;
            newLead["axx_utm_content"] = leadUtmContent;
            newLead["axx_utm_medium"] = leadUtmMedium;
            newLead["axx_utm_source"] = leadUtmSource;
            newLead["axx_utm_term"] = leadUtmTerm;
            newLead["cdi_enviadofabrica"] = leadEnviadoFabrica;
            newLead["cdi_idfabrica"] = leadIdFabrica;
            if (!string.IsNullOrEmpty(leadOwnerIdName))
            {
                if (!segmentCache.TryGetValue(leadOwnerIdName, out Guid id))
                {
                    EntityCollection segment = crmSvcPreProd2.RetrieveMultiple(new QueryExpression("systemuser")
                    {
                        ColumnSet = new ColumnSet("fullname"),
                        Criteria = new FilterExpression
                        {
                            Conditions =
                                        {
                                    new ConditionExpression("fullname", ConditionOperator.Equal, leadOwnerIdName)
                                }
                        }
                    });
                    if (segment.Entities.Count > 0)
                    {
                        id = segment.Entities.First().GetAttributeValue<Guid>("systemuserid");
                        segmentCache.Add(leadOwnerIdName, id);
                    }
                }
                newLead["ownerid"] = new EntityReference("systemuser", id);
            }
            try
            {
                if (isInsert)
                {
                    crmSvcPreProd2.Create(newLead);
                    _logger.LogInformation($"Lead {leadId} created successfully.");
                }
                else
                {
                    crmSvcPreProd2.Update(newLead);
                    _logger.LogInformation($"Lead {leadId} updated successfully.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error updating lead {leadId}: {ex.Message}");
            }
        }
        */

        // Esta función convierte un objeto LeadDto en una entidad Dataverse tipo "lead" con resolución de lookups
        public Entity ConvertToEntity(LeadDto dto, ServiceClient crmSvc,
            Dictionary<string, Guid> asesorCache,
            Dictionary<string, Guid> facilityCache,
            Dictionary<string, Guid> businessCache,
            Dictionary<string, Guid> canalCache,
            Dictionary<string, Guid> citaCache,
            Dictionary<string, Guid> paisCache,
            Dictionary<string, Guid> brandCache,
            Dictionary<string, Guid> classCache,
            Dictionary<string, Guid> segmentCache,
            Dictionary<string, Guid> sucursalCache)
        {
            var entity = new Entity("lead", dto.LeadId);

            // Básicos
            entity["firstname"] = dto.LeadFirstName;
            entity["lastname"] = dto.LeadLastName;
            entity["fullname"] = dto.LeadName;
            entity["emailaddress1"] = dto.LeadEmail;
            entity["telephone1"] = dto.LeadPhone;
            entity["description"] = dto.LeadDescription;
            entity["axx_direccionsucursaloculto"] = dto.LeadDireccionSucursalOculto;
            entity["axx_ownerpuestooculto"] = dto.LeadOwnerPuestoOculto;
            entity["axx_marcaoculto"] = dto.LeadMarcaOculto;
            entity["axx_clienteasiste"] = dto.LeadClienteAsiste;
            entity["axx_gestorasignado"] = dto.LeadGestorAsignado;
            entity["axx_istestdrive"] = dto.LeadIsTestDrive;
            entity["axx_residente"] = dto.LeadResidente;
            entity["cdi_enviadofabrica"] = dto.LeadEnviadoFabrica;
            entity["axx_fechaasignaciongestor"] = dto.LeadFechaAsignacionGestor;
            entity["a365_leadscore_date"] = dto.LeadLeadScoreDate;
            entity["a365_leadscore_state"] = dto.LeadLeadScoreState;
            entity["axx_logasignacion"] = dto.LeadLogAsignacion;
            entity["axx_empresaorigen"] = dto.LeadEmpresaOrigen;
            entity["axx_nrodocumentotributario"] = dto.LeadNroDocumentoTributario;
            entity["axx_nit"] = dto.LeadNit;
            entity["axx_observacioneslvehiculo"] = dto.LeadObservacionesVehiculo;
            entity["axx_url_referral"] = dto.LeadUrlReferral;
            entity["axx_utm_campaign"] = dto.LeadUtmCampaign;
            entity["axx_utm_content"] = dto.LeadUtmContent;
            entity["axx_utm_medium"] = dto.LeadUtmMedium;
            entity["axx_utm_source"] = dto.LeadUtmSource;
            entity["axx_utm_term"] = dto.LeadUtmTerm;

            // OptionSets
            if (dto.LeadLeadQualityCode?.Value != null)
                entity["leadqualitycode"] = new OptionSetValue(dto.LeadLeadQualityCode.Value);
            if (dto.LeadMedioDeContactoPreferido?.Value != null)
                entity["axx_mediodecontactopreferido"] = new OptionSetValue(dto.LeadMedioDeContactoPreferido.Value);
            if (dto.LeadMetodoAsignacionLead?.Value != null)
                entity["axx_metodoasignacionlead"] = new OptionSetValue(dto.LeadMetodoAsignacionLead.Value);
            if (dto.LeadMsaAutoInterest?.Value != null)
                entity["msauto_interest"] = new OptionSetValue(dto.LeadMsaAutoInterest.Value);
            if (dto.LeadMsdynOrderType?.Value != null)
                entity["msdyn_ordertype"] = new OptionSetValue(dto.LeadMsdynOrderType.Value);
            if (dto.LeadOrigenProspecto?.Value != null)
                entity["axx_origenprospecto"] = new OptionSetValue(dto.LeadOrigenProspecto.Value);
            if (dto.LeadPorQueMedioSeEntero?.Value != null)
                entity["axx_porquemedioseentero"] = new OptionSetValue(dto.LeadPorQueMedioSeEntero.Value);
            if (dto.LeadTipoDocumentoIdentificador?.Value != null)
                entity["axx_tipodocumentoidentificador"] = new OptionSetValue(dto.LeadTipoDocumentoIdentificador.Value);
            if (dto.LeadTipoDocumentoTributario?.Value != null)
                entity["axx_tipodocumentotributario"] = new OptionSetValue(dto.LeadTipoDocumentoTributario.Value);

            // Lookups resueltos
            SetReference("axx_asesorprospecto", "systemuser", "fullname", "systemuserid", dto.LeadAsesorProspecto?.Name, asesorCache);
            SetReference("a365_businessfacilityid", "msauto_businessfacility", "msauto_name", "msauto_businessfacilityid", dto.LeadBusinessFacilityId?.Name, facilityCache);
            SetReference("a365_businessid", "msauto_business", "msauto_name", "msauto_businessid", dto.LeadBusinessId?.Name, businessCache);
            SetReference("axx_canaldeingreso", "axx_canalingreso", "axx_name", "axx_canalingresoid", dto.LeadCanalDeIngreso?.Name, canalCache);
            SetReference("axx_cita", "appointment", "subject", "activityid", dto.LeadCita?.Name, citaCache);
            SetReference("axx_paisorigen", "axx_pais", "axx_name", "axx_paisid", dto.LeadPaisOrigen?.Name, paisCache);
            SetReference("a365_devicebrandid", "msauto_devicebrand", "msauto_name", "msauto_devicebrandid", dto.LeadDeviceBrandId?.Name, brandCache);
            SetReference("a365_deviceclassid", "msauto_deviceclass", "msauto_name", "msauto_deviceclassid", dto.LeadDeviceClassId?.Name, classCache);
            SetReference("msdyn_segmentid", "msdyn_segment", "msdyn_name", "msdyn_segmentid", dto.LeadMsdynSegmentId?.Name, segmentCache);
            SetReference("axx_sucursal", "axx_sucursal", "axx_name", "axx_sucursalid", dto.LeadSucursal?.Name, sucursalCache);
            SetReference("ownerid", "systemuser", "fullname", "systemuserid", dto.LeadOwnerId?.Name, asesorCache);

            return entity;

            void SetReference(string field, string entityName, string nameField, string idField, string nameValue, Dictionary<string, Guid> cache)
            {
                if (string.IsNullOrWhiteSpace(nameValue)) return;

                if (!cache.TryGetValue(nameValue, out Guid id))
                {
                    var qe = new QueryExpression(entityName)
                    {
                        ColumnSet = new ColumnSet(idField),
                        Criteria = new FilterExpression
                        {
                            Conditions = { new ConditionExpression(nameField, ConditionOperator.Equal, nameValue) }
                        }
                    };
                    var result = crmSvc.RetrieveMultiple(qe);
                    if (result.Entities.Count > 0)
                    {
                        id = result.Entities.First().GetAttributeValue<Guid>(idField);
                        cache[nameValue] = id;
                    }
                }

                if (id != Guid.Empty)
                    entity[field] = new EntityReference(entityName, id);
            }
        }

    }

    public class LeadDto
    {
        public class Lookup
        {
            public Guid Id { get; set; }
            public string LogicalName { get; set; }
            public string Name { get; set; }
        }

        public class OptionSetValueWrapper
        {
            public int Value { get; set; }
        }

        [JsonPropertyName("leadId")] public Guid LeadId { get; set; }
        [JsonPropertyName("leadName")] public string LeadName { get; set; }

        [JsonPropertyName("leadFirstName")] public string LeadFirstName { get; set; }
        [JsonPropertyName("leadLastName")] public string LeadLastName { get; set; }
        [JsonPropertyName("leadEmail")] public string LeadEmail { get; set; }
        [JsonPropertyName("leadPhone")] public string LeadPhone { get; set; }

        [JsonPropertyName("leadDescription")] public string LeadDescription { get; set; }

        [JsonPropertyName("leadDireccionSucursalOculto")] public string LeadDireccionSucursalOculto { get; set; }
        [JsonPropertyName("leadOwnerPuestoOculto")] public string LeadOwnerPuestoOculto { get; set; }
        [JsonPropertyName("leadMarcaOculto")] public string LeadMarcaOculto { get; set; }

        [JsonPropertyName("leadAsesorProspecto")] public Lookup LeadAsesorProspecto { get; set; }
        [JsonPropertyName("leadBusinessFacilityId")] public Lookup LeadBusinessFacilityId { get; set; }
        [JsonPropertyName("leadBusinessId")] public Lookup LeadBusinessId { get; set; }
        [JsonPropertyName("leadCanalDeIngreso")] public Lookup LeadCanalDeIngreso { get; set; }
        [JsonPropertyName("leadCita")] public Lookup LeadCita { get; set; }
        [JsonPropertyName("leadDeviceBrandId")] public Lookup LeadDeviceBrandId { get; set; }
        [JsonPropertyName("leadDeviceClassId")] public Lookup LeadDeviceClassId { get; set; }
        [JsonPropertyName("leadMsdynSegmentId")] public Lookup LeadMsdynSegmentId { get; set; }
        [JsonPropertyName("leadOwnerId")] public Lookup LeadOwnerId { get; set; }
        [JsonPropertyName("leadPaisOrigen")] public Lookup LeadPaisOrigen { get; set; }
        [JsonPropertyName("leadSucursal")] public Lookup LeadSucursal { get; set; }

        [JsonPropertyName("leadLeadQualityCode")] public OptionSetValueWrapper LeadLeadQualityCode { get; set; }
        [JsonPropertyName("leadMedioDeContactoPreferido")] public OptionSetValueWrapper LeadMedioDeContactoPreferido { get; set; }
        [JsonPropertyName("leadMetodoAsignacionLead")] public OptionSetValueWrapper LeadMetodoAsignacionLead { get; set; }
        [JsonPropertyName("leadMsaAutoInterest")] public OptionSetValueWrapper LeadMsaAutoInterest { get; set; }
        [JsonPropertyName("leadMsdynOrderType")] public OptionSetValueWrapper LeadMsdynOrderType { get; set; }
        [JsonPropertyName("leadOrigenProspecto")] public OptionSetValueWrapper LeadOrigenProspecto { get; set; }
        [JsonPropertyName("leadPorQueMedioSeEntero")] public OptionSetValueWrapper LeadPorQueMedioSeEntero { get; set; }
        [JsonPropertyName("leadTipoDocumentoIdentificador")] public OptionSetValueWrapper LeadTipoDocumentoIdentificador { get; set; }
        [JsonPropertyName("leadTipoDocumentoTributario")] public OptionSetValueWrapper LeadTipoDocumentoTributario { get; set; }

        [JsonPropertyName("leadFechaAsignacionGestor")] public DateTime? LeadFechaAsignacionGestor { get; set; }
        [JsonPropertyName("leadLeadScoreDate")] public DateTime? LeadLeadScoreDate { get; set; }

        [JsonPropertyName("leadLeadScoreState")] public int? LeadLeadScoreState { get; set; }
        [JsonPropertyName("leadLogAsignacion")] public string LeadLogAsignacion { get; set; }
        [JsonPropertyName("leadEmpresaOrigen")] public string LeadEmpresaOrigen { get; set; }
        [JsonPropertyName("leadNroDocumentoTributario")] public string LeadNroDocumentoTributario { get; set; }
        [JsonPropertyName("leadNit")] public string LeadNit { get; set; }
        [JsonPropertyName("leadObservacionesVehiculo")] public string LeadObservacionesVehiculo { get; set; }
        [JsonPropertyName("leadUrlReferral")] public string LeadUrlReferral { get; set; }

        [JsonPropertyName("leadUtmCampaign")] public string LeadUtmCampaign { get; set; }
        [JsonPropertyName("leadUtmContent")] public string LeadUtmContent { get; set; }
        [JsonPropertyName("leadUtmMedium")] public string LeadUtmMedium { get; set; }
        [JsonPropertyName("leadUtmSource")] public string LeadUtmSource { get; set; }
        [JsonPropertyName("leadUtmTerm")] public string LeadUtmTerm { get; set; }

        [JsonPropertyName("leadClienteAsiste")] public bool? LeadClienteAsiste { get; set; }
        [JsonPropertyName("leadGestorAsignado")] public bool? LeadGestorAsignado { get; set; }
        [JsonPropertyName("leadIsTestDrive")] public bool? LeadIsTestDrive { get; set; }
        [JsonPropertyName("leadResidente")] public bool? LeadResidente { get; set; }
        [JsonPropertyName("leadEnviadoFabrica")] public bool? LeadEnviadoFabrica { get; set; }

        [JsonPropertyName("leadIdFabrica")] public string LeadIdFabrica { get; set; }
    }
}
