namespace Szakdolgozat.Models;

public class StockItem
{
    public int Id { get; set; }
    public string WarehouseCode { get; set; } = "";
    public string ProductCode { get; set; } = "";
    public decimal Qty { get; set; }
    public DateTime UpdatedAt { get; set; }
}
