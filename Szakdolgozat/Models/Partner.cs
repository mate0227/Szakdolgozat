namespace Szakdolgozat.Models
{
    public class Partner
    {
        public int Id { get; set; }

        public string Code { get; set; } = null!;
        public string Name { get; set; } = null!;

        public bool IsCustomer { get; set; }
        public bool IsSupplier { get; set; }

        public string Country { get; set; } = null!;
        public string County { get; set; } = null!;
        public string City { get; set; } = null!;
        public string Address { get; set; } = null!;

        public string? Email { get; set; }
        public string? Phone { get; set; }
        public string? TaxNumber { get; set; }
    }
}
