namespace Szakdolgozat.Models
{
    public class BevetelFej
    {
        public int Id { get; set; }
        public string Bizonylat { get; set; } = null!;

        public DateTime Datum { get; set; }
        public bool Lezart { get; set; }

        public string PartnerKod { get; set; } = null!;
        public string PartnerNev { get; set; } = null!;
        public string PartnerIrsz { get; set; } = null!;
        public string PartnerVaros { get; set; } = null!;
        public string PartnerCim { get; set; } = null!;

        public string? Megjegyzes { get; set; }

        public string Valuta { get; set; } = "HUF";
        public decimal Arfolyam { get; set; }

        public decimal NettoErtek { get; set; }
        public decimal BruttoErtek { get; set; }
    }
}
