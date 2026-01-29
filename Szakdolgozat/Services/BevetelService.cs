using FirebirdSql.Data.FirebirdClient;
using System.Globalization;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class BevetelService
{
    private readonly FbConnection _connection;

    public BevetelService(FbConnection connection)
    {
        _connection = connection;
    }

    //fej

    public async Task<List<BevetelFej>> GetAllFejekAsync(string? search = null, int take = 500)
    {
        search = string.IsNullOrWhiteSpace(search) ? null : search.Trim();

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT
                    f.ID,
                    f.BIZONYLAT,
                    f.DATUM,
                    f.LEZART,
                    f.PARTNER_KOD,
                    f.PARTNER_NEV,
                    f.PARTNER_IRSZ,
                    f.PARTNER_VAROS,
                    f.PARTNER_CIM,
                    f.MEGJEGYZES,
                    f.VALUTA,
                    f.ARFOLYAM,
                    f.NETTO_ERTEK,
                    f.BRUTTO_ERTEK
                FROM BEVETEL_FEJ f
            ";

            if (search is not null)
            {
                sql += @"
                WHERE
                    UPPER(COALESCE(f.BIZONYLAT, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(f.PARTNER_KOD, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(f.PARTNER_NEV, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(f.PARTNER_VAROS, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(f.PARTNER_CIM, '')) CONTAINING UPPER(@s)
                    OR CAST(f.ID AS VARCHAR(20)) CONTAINING @s
                ";
            }

            sql += @"
                ORDER BY f.DATUM DESC, f.ID DESC
                FETCH FIRST @take ROWS ONLY
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@take", take);
            if (search is not null)
                cmd.Parameters.AddWithValue("@s", search);

            var result = new List<BevetelFej>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new BevetelFej
                {
                    Id = r.GetInt32(0),
                    Bizonylat = r.GetString(1),
                    Datum = r.GetDateTime(2),
                    Lezart = r.GetBoolean(3),

                    PartnerKod = r.GetString(4),
                    PartnerNev = r.GetString(5),
                    PartnerIrsz = r.GetString(6),
                    PartnerVaros = r.GetString(7),
                    PartnerCim = r.GetString(8),

                    Megjegyzes = r.IsDBNull(9) ? null : r.GetString(9),

                    Valuta = r.IsDBNull(10) ? "HUF" : r.GetString(10),
                    Arfolyam = r.IsDBNull(11) ? 1m : r.GetDecimal(11),

                    NettoErtek = r.IsDBNull(12) ? 0m : r.GetDecimal(12),
                    BruttoErtek = r.IsDBNull(13) ? 0m : r.GetDecimal(13),
                });
            }

            return result;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<BevetelFej?> GetFejByIdAsync(int id)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT
                    f.ID,
                    f.BIZONYLAT,
                    f.DATUM,
                    f.LEZART,
                    f.PARTNER_KOD,
                    f.PARTNER_NEV,
                    f.PARTNER_IRSZ,
                    f.PARTNER_VAROS,
                    f.PARTNER_CIM,
                    f.MEGJEGYZES,
                    f.VALUTA,
                    f.ARFOLYAM,
                    f.NETTO_ERTEK,
                    f.BRUTTO_ERTEK
                FROM BEVETEL_FEJ f
                WHERE f.ID = @id
                ROWS 1
            ", _connection);

            cmd.Parameters.AddWithValue("@id", id);

            using var r = await cmd.ExecuteReaderAsync();
            if (!await r.ReadAsync()) return null;

            return new BevetelFej
            {
                Id = r.GetInt32(0),
                Bizonylat = r.GetString(1),
                Datum = r.GetDateTime(2),
                Lezart = r.GetBoolean(3),

                PartnerKod = r.GetString(4),
                PartnerNev = r.GetString(5),
                PartnerIrsz = r.GetString(6),
                PartnerVaros = r.GetString(7),
                PartnerCim = r.GetString(8),

                Megjegyzes = r.IsDBNull(9) ? null : r.GetString(9),

                Valuta = r.IsDBNull(10) ? "HUF" : r.GetString(10),
                Arfolyam = r.IsDBNull(11) ? 1m : r.GetDecimal(11),

                NettoErtek = r.IsDBNull(12) ? 0m : r.GetDecimal(12),
                BruttoErtek = r.IsDBNull(13) ? 0m : r.GetDecimal(13),
            };
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<BevetelFej?> GetFejByBizonylatAsync(string bizonylat)
    {
        bizonylat = (bizonylat ?? "").Trim();
        if (bizonylat.Length == 0) return null;

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT
                    f.ID,
                    f.BIZONYLAT,
                    f.DATUM,
                    f.LEZART,
                    f.PARTNER_KOD,
                    f.PARTNER_NEV,
                    f.PARTNER_IRSZ,
                    f.PARTNER_VAROS,
                    f.PARTNER_CIM,
                    f.MEGJEGYZES,
                    f.VALUTA,
                    f.ARFOLYAM,
                    f.NETTO_ERTEK,
                    f.BRUTTO_ERTEK
                FROM BEVETEL_FEJ f
                WHERE f.BIZONYLAT = @b
                ROWS 1
            ", _connection);

            cmd.Parameters.AddWithValue("@b", bizonylat);

            using var r = await cmd.ExecuteReaderAsync();
            if (!await r.ReadAsync()) return null;

            return new BevetelFej
            {
                Id = r.GetInt32(0),
                Bizonylat = r.GetString(1),
                Datum = r.GetDateTime(2),
                Lezart = r.GetBoolean(3),

                PartnerKod = r.GetString(4),
                PartnerNev = r.GetString(5),
                PartnerIrsz = r.GetString(6),
                PartnerVaros = r.GetString(7),
                PartnerCim = r.GetString(8),

                Megjegyzes = r.IsDBNull(9) ? null : r.GetString(9),

                Valuta = r.IsDBNull(10) ? "HUF" : r.GetString(10),
                Arfolyam = r.IsDBNull(11) ? 1m : r.GetDecimal(11),

                NettoErtek = r.IsDBNull(12) ? 0m : r.GetDecimal(12),
                BruttoErtek = r.IsDBNull(13) ? 0m : r.GetDecimal(13),
            };
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<int> CreateFejAsync(BevetelFej f)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                INSERT INTO BEVETEL_FEJ
                    (BIZONYLAT, DATUM, LEZART,
                     PARTNER_KOD, PARTNER_NEV, PARTNER_IRSZ, PARTNER_VAROS, PARTNER_CIM,
                     MEGJEGYZES,
                     VALUTA, ARFOLYAM,
                     NETTO_ERTEK, BRUTTO_ERTEK)
                VALUES
                    (@b, @datum, @lezart,
                     @pk, @pn, @pi, @pv, @pc,
                     @megj,
                     @val, @arf,
                     @net, @br)
                RETURNING ID
            ", _connection);

            cmd.Parameters.AddWithValue("@b", (f.Bizonylat ?? "").Trim());
            cmd.Parameters.AddWithValue("@datum", f.Datum);
            cmd.Parameters.AddWithValue("@lezart", f.Lezart);

            cmd.Parameters.AddWithValue("@pk", (f.PartnerKod ?? "").Trim());
            cmd.Parameters.AddWithValue("@pn", (f.PartnerNev ?? "").Trim());
            cmd.Parameters.AddWithValue("@pi", (f.PartnerIrsz ?? "").Trim());
            cmd.Parameters.AddWithValue("@pv", (f.PartnerVaros ?? "").Trim());
            cmd.Parameters.AddWithValue("@pc", (f.PartnerCim ?? "").Trim());

            var megj = string.IsNullOrWhiteSpace(f.Megjegyzes) ? null : f.Megjegyzes.Trim();
            cmd.Parameters.AddWithValue("@megj", (object?)megj ?? DBNull.Value);

            cmd.Parameters.AddWithValue("@val", string.IsNullOrWhiteSpace(f.Valuta) ? "HUF" : f.Valuta.Trim());
            cmd.Parameters.AddWithValue("@arf", f.Arfolyam <= 0 ? 1m : f.Arfolyam);

            cmd.Parameters.AddWithValue("@net", f.NettoErtek);
            cmd.Parameters.AddWithValue("@br", f.BruttoErtek);

            var idObj = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(idObj);
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> UpdateFejAsync(BevetelFej f)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                UPDATE BEVETEL_FEJ
                SET
                    BIZONYLAT = @b,
                    DATUM = @datum,
                    LEZART = @lezart,
                    PARTNER_KOD = @pk,
                    PARTNER_NEV = @pn,
                    PARTNER_IRSZ = @pi,
                    PARTNER_VAROS = @pv,
                    PARTNER_CIM = @pc,
                    MEGJEGYZES = @megj,
                    VALUTA = @val,
                    ARFOLYAM = @arf,
                    NETTO_ERTEK = @net,
                    BRUTTO_ERTEK = @br
                WHERE ID = @id
            ", _connection);

            cmd.Parameters.AddWithValue("@id", f.Id);
            cmd.Parameters.AddWithValue("@b", (f.Bizonylat ?? "").Trim());
            cmd.Parameters.AddWithValue("@datum", f.Datum);
            cmd.Parameters.AddWithValue("@lezart", f.Lezart);

            cmd.Parameters.AddWithValue("@pk", (f.PartnerKod ?? "").Trim());
            cmd.Parameters.AddWithValue("@pn", (f.PartnerNev ?? "").Trim());
            cmd.Parameters.AddWithValue("@pi", (f.PartnerIrsz ?? "").Trim());
            cmd.Parameters.AddWithValue("@pv", (f.PartnerVaros ?? "").Trim());
            cmd.Parameters.AddWithValue("@pc", (f.PartnerCim ?? "").Trim());

            var megj = string.IsNullOrWhiteSpace(f.Megjegyzes) ? null : f.Megjegyzes.Trim();
            cmd.Parameters.AddWithValue("@megj", (object?)megj ?? DBNull.Value);

            cmd.Parameters.AddWithValue("@val", string.IsNullOrWhiteSpace(f.Valuta) ? "HUF" : f.Valuta.Trim());
            cmd.Parameters.AddWithValue("@arf", f.Arfolyam <= 0 ? 1m : f.Arfolyam);

            cmd.Parameters.AddWithValue("@net", f.NettoErtek);
            cmd.Parameters.AddWithValue("@br", f.BruttoErtek);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> BizonylatExistsAsync(string bizonylat)
    {
        bizonylat = (bizonylat ?? "").Trim();
        if (bizonylat.Length == 0) return false;

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT 1
                FROM BEVETEL_FEJ
                WHERE BIZONYLAT = @b
                ROWS 1
            ", _connection);

            cmd.Parameters.AddWithValue("@b", bizonylat);

            var obj = await cmd.ExecuteScalarAsync();
            return obj != null;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    //tetel

    public async Task<List<BevetelTetel>> GetTetelekAsync(string bizonylat)
    {
        bizonylat = (bizonylat ?? "").Trim();
        if (bizonylat.Length == 0) return new List<BevetelTetel>();

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT
                    t.ID,
                    t.BIZONYLAT,
                    t.TERMEK_ID,
                    t.NEV,
                    t.ME,
                    t.AFAKOD,
                    t.MENNYISEG,
                    t.NETTO_EGYSEGAR,
                    t.BRUTTO_EGYSEGAR,
                    t.NETTO_TETELERTEK,
                    t.BRUTTO_TETELERTEK
                FROM BEVETEL_TETEL t
                WHERE t.BIZONYLAT = @b
                ORDER BY t.ID
            ", _connection);

            cmd.Parameters.AddWithValue("@b", bizonylat);

            var result = new List<BevetelTetel>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new BevetelTetel
                {
                    Id = r.GetInt32(0),
                    Bizonylat = r.GetString(1),
                    TermekId = r.GetInt32(2),
                    Nev = r.GetString(3),
                    Me = r.GetString(4),
                    AfaKod = r.GetString(5),
                    Mennyiseg = r.GetDecimal(6),
                    NettoEgysegAr = r.GetDecimal(7),
                    BruttoEgysegAr = r.GetDecimal(8),
                    NettoTetelErtek = r.GetDecimal(9),
                    BruttoTetelErtek = r.GetDecimal(10),
                });
            }

            return result;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<int> CreateTetelAsync(BevetelTetel t)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                INSERT INTO BEVETEL_TETEL
                    (BIZONYLAT, TERMEK_ID, NEV, ME, AFAKOD,
                     MENNYISEG,
                     NETTO_EGYSEGAR, BRUTTO_EGYSEGAR,
                     NETTO_TETELERTEK, BRUTTO_TETELERTEK)
                VALUES
                    (@b, @tid, @nev, @me, @afa,
                     @menny,
                     @ne, @be,
                     @nte, @bte)
                RETURNING ID
            ", _connection);

            cmd.Parameters.AddWithValue("@b", (t.Bizonylat ?? "").Trim());
            cmd.Parameters.AddWithValue("@tid", t.TermekId);

            cmd.Parameters.AddWithValue("@nev", (t.Nev ?? "").Trim());
            cmd.Parameters.AddWithValue("@me", (t.Me ?? "").Trim());
            cmd.Parameters.AddWithValue("@afa", (t.AfaKod ?? "").Trim());

            cmd.Parameters.AddWithValue("@menny", t.Mennyiseg);

            cmd.Parameters.AddWithValue("@ne", t.NettoEgysegAr);
            cmd.Parameters.AddWithValue("@be", t.BruttoEgysegAr);

            cmd.Parameters.AddWithValue("@nte", t.NettoTetelErtek);
            cmd.Parameters.AddWithValue("@bte", t.BruttoTetelErtek);

            var idObj = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(idObj);
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> UpdateTetelAsync(BevetelTetel t)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                UPDATE BEVETEL_TETEL
                SET
                    BIZONYLAT = @b,
                    TERMEK_ID = @tid,
                    NEV = @nev,
                    ME = @me,
                    AFAKOD = @afa,
                    MENNYISEG = @menny,
                    NETTO_EGYSEGAR = @ne,
                    BRUTTO_EGYSEGAR = @be,
                    NETTO_TETELERTEK = @nte,
                    BRUTTO_TETELERTEK = @bte
                WHERE ID = @id
            ", _connection);

            cmd.Parameters.AddWithValue("@id", t.Id);
            cmd.Parameters.AddWithValue("@b", (t.Bizonylat ?? "").Trim());
            cmd.Parameters.AddWithValue("@tid", t.TermekId);

            cmd.Parameters.AddWithValue("@nev", (t.Nev ?? "").Trim());
            cmd.Parameters.AddWithValue("@me", (t.Me ?? "").Trim());
            cmd.Parameters.AddWithValue("@afa", (t.AfaKod ?? "").Trim());

            cmd.Parameters.AddWithValue("@menny", t.Mennyiseg);

            cmd.Parameters.AddWithValue("@ne", t.NettoEgysegAr);
            cmd.Parameters.AddWithValue("@be", t.BruttoEgysegAr);

            cmd.Parameters.AddWithValue("@nte", t.NettoTetelErtek);
            cmd.Parameters.AddWithValue("@bte", t.BruttoTetelErtek);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> DeleteTetelAsync(int id)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"DELETE FROM BEVETEL_TETEL WHERE ID = @id", _connection);
            cmd.Parameters.AddWithValue("@id", id);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    // ---------------------------
    // Totals recalculation (optional, but useful)
    // ---------------------------

    public async Task<bool> RecalcFejTotalsAsync(string bizonylat)
    {
        bizonylat = (bizonylat ?? "").Trim();
        if (bizonylat.Length == 0) return false;

        await _connection.OpenAsync();
        try
        {
            decimal netto = 0m;
            decimal brutto = 0m;

            // Sum from tetel
            using (var sumCmd = new FbCommand(@"
                SELECT
                    COALESCE(SUM(t.NETTO_TETELERTEK), 0),
                    COALESCE(SUM(t.BRUTTO_TETELERTEK), 0)
                FROM BEVETEL_TETEL t
                WHERE t.BIZONYLAT = @b
            ", _connection))
            {
                sumCmd.Parameters.AddWithValue("@b", bizonylat);

                using var r = await sumCmd.ExecuteReaderAsync();
                if (await r.ReadAsync())
                {
                    netto = r.IsDBNull(0) ? 0m : r.GetDecimal(0);
                    brutto = r.IsDBNull(1) ? 0m : r.GetDecimal(1);
                }
            }

            // Update fej totals
            using (var updCmd = new FbCommand(@"
                UPDATE BEVETEL_FEJ
                SET NETTO_ERTEK = @netto,
                    BRUTTO_ERTEK = @brutto
                WHERE BIZONYLAT = @b
            ", _connection))
            {
                updCmd.Parameters.AddWithValue("@netto", netto);
                updCmd.Parameters.AddWithValue("@brutto", brutto);
                updCmd.Parameters.AddWithValue("@b", bizonylat);

                var rows = await updCmd.ExecuteNonQueryAsync();
                return rows > 0;
            }
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> BizonylatExistsForOtherAsync(int id, string bizonylat)
    {
        bizonylat = (bizonylat ?? "").Trim();
        if (bizonylat.Length == 0) return false;

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
            SELECT 1
            FROM BEVETEL_FEJ
            WHERE BIZONYLAT = @b
              AND ID <> @id
            ROWS 1
        ", _connection);

            cmd.Parameters.AddWithValue("@b", bizonylat);
            cmd.Parameters.AddWithValue("@id", id);

            var obj = await cmd.ExecuteScalarAsync();
            return obj != null;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<string> GetNextBizonylatAsync()
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(
                "SELECT NEXT VALUE FOR BEVETEL_BIZONYLAT_SEQ FROM RDB$DATABASE",
                _connection);

            var obj = await cmd.ExecuteScalarAsync();
            var n = Convert.ToInt64(obj, CultureInfo.InvariantCulture);

            // BE-0000001 (7 digits)
            return $"BE-{n:0000000}";
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

}
