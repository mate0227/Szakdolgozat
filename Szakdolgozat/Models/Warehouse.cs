namespace Szakdolgozat.Models;

public class Warehouse
{
    public int Id { get; set; }
    public string Code { get; set; } = "";
    public string Name { get; set; } = "";
    public bool IsAutomaton { get; set; }

    public string County { get; set; } = "";
    public string City { get; set; } = "";
    public string Address { get; set; } = "";
}
