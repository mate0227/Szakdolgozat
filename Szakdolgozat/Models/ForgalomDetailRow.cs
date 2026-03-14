namespace Szakdolgozat.Models;

public sealed class ForgalomDetailRow
{
    public string Tipus { get; set; } = "";

    public int FejId { get; set; }
    public int TetelId { get; set; }

    public string Bizonylat { get; set; } = "";
    public DateTime Datum { get; set; }

    public int ProductId { get; set; }
    public string ProductCode { get; set; } = "";
    public string ProductName { get; set; } = "";
    public string Unit { get; set; } = "";

    public string AfaKod { get; set; } = "";

    public decimal Quantity { get; set; }
    public decimal NetUnitPrice { get; set; }
    public decimal GrossUnitPrice { get; set; }
    public decimal NetAmount { get; set; }
    public decimal GrossAmount { get; set; }

    public int? WarehouseId { get; set; }
    public string WarehouseName { get; set; } = "";

    public int? WarehouseFromId { get; set; }
    public string WarehouseFromName { get; set; } = "";

    public int? WarehouseToId { get; set; }
    public string WarehouseToName { get; set; } = "";
}