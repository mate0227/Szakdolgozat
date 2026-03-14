using FirebirdSql.Data.FirebirdClient;
using System.Globalization;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class AtadasService
{
    private readonly FbConnection _connection;

    public AtadasService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<AtadasFej>> GetAllFejekAsync(string? search = null, int take = 500)
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
                FROM ATADAS_FEJ f
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
                    OR UPPER(COALESCE(f.MEGJEGYZES, '')) CONTAINING UPPER(@s)
                    OR CAST(f.ID AS VARCHAR(20)) CONTAINING @s
                ";
            }

            sql += @"
                ORDER BY f.DATUM DESC, f.ID DESC
                FETCH FIRST @take ROWS ONLY
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@take", take);
            if (search is not null) cmd.Parameters.AddWithValue("@s", search);

            var list = new List<AtadasFej>();
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                list.Add(new AtadasFej
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

            return list;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<AtadasFej?> GetFejByIdAsync(int id)
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
                FROM ATADAS_FEJ f
                WHERE f.ID = @id
                ROWS 1
            ", _connection);

            cmd.Parameters.AddWithValue("@id", id);

            using var r = await cmd.ExecuteReaderAsync();
            if (!await r.ReadAsync()) return null;

            return new AtadasFej
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

    public async Task<AtadasFej?> GetFejByBizonylatAsync(string bizonylat)
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
                FROM ATADAS_FEJ f
                WHERE f.BIZONYLAT = @b
                ROWS 1
            ", _connection);

            cmd.Parameters.AddWithValue("@b", bizonylat);

            using var r = await cmd.ExecuteReaderAsync();
            if (!await r.ReadAsync()) return null;

            return new AtadasFej
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

    public async Task<int> CreateFejAsync(AtadasFej f)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                INSERT INTO ATADAS_FEJ
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

    public async Task<bool> UpdateFejAsync(AtadasFej f)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                UPDATE ATADAS_FEJ
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
                FROM ATADAS_FEJ
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

    public async Task<bool> BizonylatExistsForOtherAsync(int id, string bizonylat)
    {
        bizonylat = (bizonylat ?? "").Trim();
        if (bizonylat.Length == 0) return false;

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT 1
                FROM ATADAS_FEJ
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
                "SELECT NEXT VALUE FOR ATADAS_BIZONYLAT_SEQ FROM RDB$DATABASE",
                _connection);

            var obj = await cmd.ExecuteScalarAsync();
            var n = Convert.ToInt64(obj, CultureInfo.InvariantCulture);
            return $"AT-{n:0000000}";
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<List<AtadasTetel>> GetTetelekAsync(string bizonylat)
    {
        bizonylat = (bizonylat ?? "").Trim();
        if (bizonylat.Length == 0) return new List<AtadasTetel>();

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT
                    t.ID,
                    t.BIZONYLAT,
                    t.TERMEK_ID,
                    t.WAREHOUSE_FROM_ID,
                    t.WAREHOUSE_TO_ID,
                    t.NEV,
                    t.ME,
                    t.AFAKOD,
                    t.MENNYISEG,
                    t.NETTO_EGYSEGAR,
                    t.BRUTTO_EGYSEGAR,
                    t.NETTO_TETELERTEK,
                    t.BRUTTO_TETELERTEK
                FROM ATADAS_TETEL t
                WHERE t.BIZONYLAT = @b
                ORDER BY t.ID
            ", _connection);

            cmd.Parameters.AddWithValue("@b", bizonylat);

            var list = new List<AtadasTetel>();
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                list.Add(new AtadasTetel
                {
                    Id = r.GetInt32(0),
                    Bizonylat = r.GetString(1),
                    TermekId = r.GetInt32(2),
                    WarehouseFromId = r.GetInt32(3),
                    WarehouseToId = r.GetInt32(4),
                    Nev = r.GetString(5),
                    Me = r.GetString(6),
                    AfaKod = r.GetString(7),
                    Mennyiseg = r.GetDecimal(8),
                    NettoEgysegAr = r.GetDecimal(9),
                    BruttoEgysegAr = r.GetDecimal(10),
                    NettoTetelErtek = r.GetDecimal(11),
                    BruttoTetelErtek = r.GetDecimal(12),
                });
            }

            return list;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<int> CreateTetelAsync(AtadasTetel t)
    {
        await _connection.OpenAsync();
        FbTransaction? tx = null;

        try
        {
            tx = await _connection.BeginTransactionAsync();

            var fromCode = await GetWarehouseCodeByIdAsync(t.WarehouseFromId, tx);
            var toCode = await GetWarehouseCodeByIdAsync(t.WarehouseToId, tx);
            var productCode = await GetProductCodeByIdAsync(t.TermekId, tx);

            using var cmd = new FbCommand(@"
                INSERT INTO ATADAS_TETEL
                    (BIZONYLAT, TERMEK_ID, WAREHOUSE_FROM_ID, WAREHOUSE_TO_ID, NEV, ME, AFAKOD,
                     MENNYISEG,
                     NETTO_EGYSEGAR, BRUTTO_EGYSEGAR,
                     NETTO_TETELERTEK, BRUTTO_TETELERTEK)
                VALUES
                    (@b, @tid, @wf, @wt, @nev, @me, @afa,
                     @menny,
                     @ne, @be,
                     @nte, @bte)
                RETURNING ID
            ", _connection, tx);

            cmd.Parameters.AddWithValue("@b", (t.Bizonylat ?? "").Trim());
            cmd.Parameters.AddWithValue("@tid", t.TermekId);
            cmd.Parameters.AddWithValue("@wf", t.WarehouseFromId);
            cmd.Parameters.AddWithValue("@wt", t.WarehouseToId);
            cmd.Parameters.AddWithValue("@nev", (t.Nev ?? "").Trim());
            cmd.Parameters.AddWithValue("@me", (t.Me ?? "").Trim());
            cmd.Parameters.AddWithValue("@afa", (t.AfaKod ?? "").Trim());
            cmd.Parameters.AddWithValue("@menny", t.Mennyiseg);
            cmd.Parameters.AddWithValue("@ne", t.NettoEgysegAr);
            cmd.Parameters.AddWithValue("@be", t.BruttoEgysegAr);
            cmd.Parameters.AddWithValue("@nte", t.NettoTetelErtek);
            cmd.Parameters.AddWithValue("@bte", t.BruttoTetelErtek);

            var idObj = await cmd.ExecuteScalarAsync();
            var newId = Convert.ToInt32(idObj);

            await AddToStockAsync(fromCode, productCode, -t.Mennyiseg, tx);
            await AddToStockAsync(toCode, productCode, +t.Mennyiseg, tx);

            await tx.CommitAsync();
            return newId;
        }
        catch
        {
            if (tx is not null) await tx.RollbackAsync();
            throw;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> UpdateTetelAsync(AtadasTetel t)
    {
        await _connection.OpenAsync();
        FbTransaction? tx = null;

        try
        {
            tx = await _connection.BeginTransactionAsync();

            int oldFromId;
            int oldToId;
            int oldTermekId;
            decimal oldQty;

            using (var q = new FbCommand(@"
                SELECT WAREHOUSE_FROM_ID, WAREHOUSE_TO_ID, TERMEK_ID, MENNYISEG
                FROM ATADAS_TETEL
                WHERE ID = @id
                ROWS 1
            ", _connection, tx))
            {
                q.Parameters.AddWithValue("@id", t.Id);
                using var r = await q.ExecuteReaderAsync();
                if (!await r.ReadAsync()) return false;

                oldFromId = r.GetInt32(0);
                oldToId = r.GetInt32(1);
                oldTermekId = r.GetInt32(2);
                oldQty = r.GetDecimal(3);
            }

            using (var cmd = new FbCommand(@"
                UPDATE ATADAS_TETEL
                SET
                    BIZONYLAT = @b,
                    TERMEK_ID = @tid,
                    WAREHOUSE_FROM_ID = @wf,
                    WAREHOUSE_TO_ID = @wt,
                    NEV = @nev,
                    ME = @me,
                    AFAKOD = @afa,
                    MENNYISEG = @menny,
                    NETTO_EGYSEGAR = @ne,
                    BRUTTO_EGYSEGAR = @be,
                    NETTO_TETELERTEK = @nte,
                    BRUTTO_TETELERTEK = @bte
                WHERE ID = @id
            ", _connection, tx))
            {
                cmd.Parameters.AddWithValue("@id", t.Id);
                cmd.Parameters.AddWithValue("@b", (t.Bizonylat ?? "").Trim());
                cmd.Parameters.AddWithValue("@tid", t.TermekId);
                cmd.Parameters.AddWithValue("@wf", t.WarehouseFromId);
                cmd.Parameters.AddWithValue("@wt", t.WarehouseToId);
                cmd.Parameters.AddWithValue("@nev", (t.Nev ?? "").Trim());
                cmd.Parameters.AddWithValue("@me", (t.Me ?? "").Trim());
                cmd.Parameters.AddWithValue("@afa", (t.AfaKod ?? "").Trim());
                cmd.Parameters.AddWithValue("@menny", t.Mennyiseg);
                cmd.Parameters.AddWithValue("@ne", t.NettoEgysegAr);
                cmd.Parameters.AddWithValue("@be", t.BruttoEgysegAr);
                cmd.Parameters.AddWithValue("@nte", t.NettoTetelErtek);
                cmd.Parameters.AddWithValue("@bte", t.BruttoTetelErtek);

                var rows = await cmd.ExecuteNonQueryAsync();
                if (rows == 0) return false;
            }

            var oldFromCode = await GetWarehouseCodeByIdAsync(oldFromId, tx);
            var oldToCode = await GetWarehouseCodeByIdAsync(oldToId, tx);
            var oldProdCode = await GetProductCodeByIdAsync(oldTermekId, tx);

            var newFromCode = await GetWarehouseCodeByIdAsync(t.WarehouseFromId, tx);
            var newToCode = await GetWarehouseCodeByIdAsync(t.WarehouseToId, tx);
            var newProdCode = await GetProductCodeByIdAsync(t.TermekId, tx);

            if (oldFromCode == newFromCode && oldToCode == newToCode && oldProdCode == newProdCode)
            {
                var diff = t.Mennyiseg - oldQty;
                if (diff != 0m)
                {
                    await AddToStockAsync(newFromCode, newProdCode, -diff, tx);
                    await AddToStockAsync(newToCode, newProdCode, +diff, tx);
                }
            }
            else
            {
                if (oldQty != 0m)
                {
                    await AddToStockAsync(oldFromCode, oldProdCode, +oldQty, tx);
                    await AddToStockAsync(oldToCode, oldProdCode, -oldQty, tx);
                }

                if (t.Mennyiseg != 0m)
                {
                    await AddToStockAsync(newFromCode, newProdCode, -t.Mennyiseg, tx);
                    await AddToStockAsync(newToCode, newProdCode, +t.Mennyiseg, tx);
                }
            }

            await tx.CommitAsync();
            return true;
        }
        catch
        {
            if (tx is not null) await tx.RollbackAsync();
            throw;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> DeleteTetelAsync(int id)
    {
        await _connection.OpenAsync();
        FbTransaction? tx = null;

        try
        {
            tx = await _connection.BeginTransactionAsync();

            int fromId;
            int toId;
            int termekId;
            decimal qty;

            using (var q = new FbCommand(@"
                SELECT WAREHOUSE_FROM_ID, WAREHOUSE_TO_ID, TERMEK_ID, MENNYISEG
                FROM ATADAS_TETEL
                WHERE ID = @id
                ROWS 1
            ", _connection, tx))
            {
                q.Parameters.AddWithValue("@id", id);
                using var r = await q.ExecuteReaderAsync();
                if (!await r.ReadAsync()) return false;

                fromId = r.GetInt32(0);
                toId = r.GetInt32(1);
                termekId = r.GetInt32(2);
                qty = r.GetDecimal(3);
            }

            using (var cmd = new FbCommand(@"DELETE FROM ATADAS_TETEL WHERE ID = @id", _connection, tx))
            {
                cmd.Parameters.AddWithValue("@id", id);
                var rows = await cmd.ExecuteNonQueryAsync();
                if (rows == 0) return false;
            }

            var fromCode = await GetWarehouseCodeByIdAsync(fromId, tx);
            var toCode = await GetWarehouseCodeByIdAsync(toId, tx);
            var productCode = await GetProductCodeByIdAsync(termekId, tx);

            if (qty != 0m)
            {
                await AddToStockAsync(fromCode, productCode, +qty, tx);
                await AddToStockAsync(toCode, productCode, -qty, tx);
            }

            await tx.CommitAsync();
            return true;
        }
        catch
        {
            if (tx is not null) await tx.RollbackAsync();
            throw;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> RecalcFejTotalsAsync(string bizonylat)
    {
        bizonylat = (bizonylat ?? "").Trim();
        if (bizonylat.Length == 0) return false;

        await _connection.OpenAsync();
        try
        {
            decimal netto = 0m;
            decimal brutto = 0m;

            using (var sumCmd = new FbCommand(@"
                SELECT
                    COALESCE(SUM(t.NETTO_TETELERTEK), 0),
                    COALESCE(SUM(t.BRUTTO_TETELERTEK), 0)
                FROM ATADAS_TETEL t
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

            using (var updCmd = new FbCommand(@"
                UPDATE ATADAS_FEJ
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

    private async Task<string> GetWarehouseCodeByIdAsync(int warehouseId, FbTransaction tx)
    {
        using var cmd = new FbCommand(@"
            SELECT w.CODE
            FROM WAREHOUSES w
            WHERE w.ID = @id
            ROWS 1
        ", _connection, tx);

        cmd.Parameters.AddWithValue("@id", warehouseId);

        var obj = await cmd.ExecuteScalarAsync();
        var code = obj?.ToString()?.Trim();

        if (string.IsNullOrWhiteSpace(code))
            throw new InvalidOperationException($"Warehouse code not found for ID={warehouseId}");

        return code!;
    }

    private async Task<string> GetProductCodeByIdAsync(int productId, FbTransaction tx)
    {
        using var cmd = new FbCommand(@"
            SELECT p.CODE
            FROM PRODUCTS p
            WHERE p.ID = @id
            ROWS 1
        ", _connection, tx);

        cmd.Parameters.AddWithValue("@id", productId);

        var obj = await cmd.ExecuteScalarAsync();
        var code = obj?.ToString()?.Trim();

        if (string.IsNullOrWhiteSpace(code))
            throw new InvalidOperationException($"Product code not found for ID={productId}");

        return code!;
    }

    private async Task AddToStockAsync(string warehouseCode, string productCode, decimal qty, FbTransaction tx)
    {
        using (var upd = new FbCommand(@"
            UPDATE STOCK_ITEMS
            SET
                QTY = QTY + @qty,
                UPDATED_AT = CURRENT_TIMESTAMP
            WHERE WAREHOUSE_CODE = @wh
              AND PRODUCT_CODE = @pc
        ", _connection, tx))
        {
            upd.Parameters.AddWithValue("@qty", qty);
            upd.Parameters.AddWithValue("@wh", warehouseCode);
            upd.Parameters.AddWithValue("@pc", productCode);

            var rows = await upd.ExecuteNonQueryAsync();
            if (rows > 0) return;
        }

        using (var ins = new FbCommand(@"
            INSERT INTO STOCK_ITEMS
                (WAREHOUSE_CODE, PRODUCT_CODE, QTY, UPDATED_AT)
            VALUES
                (@wh, @pc, @qty, CURRENT_TIMESTAMP)
        ", _connection, tx))
        {
            ins.Parameters.AddWithValue("@wh", warehouseCode);
            ins.Parameters.AddWithValue("@pc", productCode);
            ins.Parameters.AddWithValue("@qty", qty);

            await ins.ExecuteNonQueryAsync();
        }
    }
}