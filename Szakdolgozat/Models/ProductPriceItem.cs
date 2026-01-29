namespace Szakdolgozat.Models;

public class ProductPriceItem
{
    public int Id { get; set; }
    public int ProductId { get; set; }

    public int VatId { get; set; }
    public string VatCode { get; set; } = "";
    public string VatName { get; set; } = "";

    public decimal NetPrice { get; set; }
    public decimal GrossPrice { get; set; }
    public string Currency { get; set; } = "";

    public DateTime ValidFrom { get; set; }
    public DateTime? ValidTo { get; set; }
    public DateTime CreatedAt { get; set; }
}
