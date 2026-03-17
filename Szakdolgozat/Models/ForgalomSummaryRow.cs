namespace Szakdolgozat.Models;

public sealed class ForgalomSummaryRow
{
    public string WarehouseCode { get; set; } = "";
    public string WarehouseName { get; set; } = "";

    public int ProductId { get; set; }
    public string ProductCode { get; set; } = "";
    public string ProductName { get; set; } = "";
    public string Unit { get; set; } = "";

    public decimal NyitoQty { get; set; }
    public decimal BevetelQty { get; set; }
    public decimal KiadasQty { get; set; }
    public decimal ZaroQty { get; set; }
}