namespace Szakdolgozat.Models;

public sealed class PriceReportRow
{
    public int ProductId { get; set; }
    public string ProductCode { get; set; } = "";
    public string ProductName { get; set; } = "";
    public string GroupName { get; set; } = "";
    public string Unit { get; set; } = "";

    public string VatCode { get; set; } = "";
    public string VatName { get; set; } = "";

    public decimal? NetPrice { get; set; }
    public decimal? GrossPrice { get; set; }
    public string Currency { get; set; } = "";

    public DateTime? ValidFrom { get; set; }
    public DateTime? ValidTo { get; set; }
}