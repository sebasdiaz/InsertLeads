
// Models/LeadDto.cs
namespace LeadToBus.Models
{
    public class LeadDto
    {
        public Guid LeadId { get; set; }
        public string? LeadName { get; set; }
        public string? LeadEmail { get; set; }
        public string? LeadPhone { get; set; }
        public string? LeadBusinessFacilityName { get; set; }
        public string? LeadBusinessName { get; set; }
        public string? LeadDeviceBrandName { get; set; }
        public string? LeadDeviceClassName { get; set; }
        public string? LeadAsesorProspectoName { get; set; }
        public string? LeadSucursalName { get; set; }
        public string? LeadOwnerIdName { get; set; }
    }
}