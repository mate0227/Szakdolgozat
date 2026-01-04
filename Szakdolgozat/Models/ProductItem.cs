namespace Szakdolgozat.Models;

public class ProductItem
{
    public int Id { get; set; }
    public string Code { get; set; } = "";
    public string Name { get; set; } = "";
    public int GroupId { get; set; }
    public string GroupName { get; set; } = "";
    public string Unit { get; set; } = "";
}
