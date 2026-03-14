namespace Szakdolgozat.Models;

public class KiadasTetel
{
    public int Id { get; set; }
    public string Bizonylat { get; set; } = null!;

    public int TermekId { get; set; }
    public int WarehouseId { get; set; }

    public string Nev { get; set; } = null!;
    public string Me { get; set; } = null!;
    public string AfaKod { get; set; } = null!;

    public decimal Mennyiseg { get; set; }

    public decimal NettoEgysegAr { get; set; }
    public decimal BruttoEgysegAr { get; set; }

    public decimal NettoTetelErtek { get; set; }
    public decimal BruttoTetelErtek { get; set; }
}
