// Services/LeadFetcher.cs
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using LeadToBus.Models;
using LeadToBus.Services;

namespace LeadToBus.Services
{
    public class LeadFetcher : ILeadFetcher
    {
        private readonly ServiceClient _crmSvc;

        public LeadFetcher(ServiceClient crmSvc)
        {
            _crmSvc = crmSvc;
        }

        public List<LeadDto> GetLeadsToExport()
        {
            var leads = new List<LeadDto>();

            var query = new QueryExpression("lead")
            {
                ColumnSet = new ColumnSet("leadid", "fullname", "emailaddress1", "telephone1", "a365_businessfacilityid", "a365_businessid", "a365_devicebrandid", "a365_deviceclassid", "axx_asesorprospecto", "axx_sucursal", "ownerid"),
                Criteria = new FilterExpression()
            };
            query.Criteria.AddCondition("fax", ConditionOperator.NotEqual, Settings.ExportedMark);
            query.TopCount = 500;

            var result = _crmSvc.RetrieveMultiple(query);

            foreach (var entity in result.Entities)
            {
                leads.Add(new LeadDto
                {
                    LeadId = entity.Id,
                    LeadName = entity.GetAttributeValue<string>("fullname"),
                    LeadEmail = entity.GetAttributeValue<string>("emailaddress1"),
                    LeadPhone = entity.GetAttributeValue<string>("telephone1"),
                    LeadBusinessFacilityName = entity.GetAttributeValue<EntityReference>("a365_businessfacilityid")?.Name,
                    LeadBusinessName = entity.GetAttributeValue<EntityReference>("a365_businessid")?.Name,
                    LeadDeviceBrandName = entity.GetAttributeValue<EntityReference>("a365_devicebrandid")?.Name,
                    LeadDeviceClassName = entity.GetAttributeValue<EntityReference>("a365_deviceclassid")?.Name,
                    LeadAsesorProspectoName = entity.GetAttributeValue<EntityReference>("axx_asesorprospecto")?.Name,
                    LeadSucursalName = entity.GetAttributeValue<EntityReference>("axx_sucursal")?.Name,
                    LeadOwnerIdName = entity.GetAttributeValue<EntityReference>("ownerid")?.Name
                });
            }

            return leads;
        }
    }
}
