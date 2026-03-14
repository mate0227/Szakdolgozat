namespace Szakdolgozat.Models;

public sealed class ForgalomSummaryRow
{
    public int ProductId { get; set; }
    public string ProductCode { get; set; } = "";
    public string ProductName { get; set; } = "";
    public string Unit { get; set; } = "";

    public decimal ForgalomQty { get; set; }
    public decimal ForgalomNet { get; set; }
    public decimal ForgalomGross { get; set; }
}