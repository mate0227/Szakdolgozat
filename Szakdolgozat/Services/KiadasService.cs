using FirebirdSql.Data.FirebirdClient;
using System.Globalization;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class KiadasService
{
    private readonly FbConnection _connection;

    public KiadasService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<KiadasFej>> GetAllFejekAsync(string? search = null, int take = 500)
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
                FROM KIADAS_FEJ f
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

            var result = new List<KiadasFej>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new KiadasFej
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

    public async Task<KiadasFej?> GetFejByIdAsync(int id)
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
                FROM KIADAS_FEJ f
                WHERE f.ID = @id
                ROWS 1
            ", _connection);

            cmd.Parameters.AddWithValue("@id", id);

            using var r = await cmd.ExecuteReaderAsync();
            if (!await r.ReadAsync()) return null;

            return new KiadasFej
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

    public async Task<bool> BizonylatExistsAsync(string bizonylat)
    {
        bizonylat = (bizonylat ?? "").Trim();
        if (bizonylat.Length == 0) return false;

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT 1
                FROM KIADAS_FEJ
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

    public async Task<int> CreateFejAsync(KiadasFej f)
    {
        await _connection.OpenAsync();
        try
        {
            var biz = (f.Bizonylat ?? "").Trim();
            if (string.IsNullOrWhiteSpace(biz))
                biz = await GetNextBizonylatAsync_INTERNAL_UsesOpenConnection();

            using var cmd = new FbCommand(@"
                INSERT INTO KIADAS_FEJ
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

            cmd.Parameters.AddWithValue("@b", biz);
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

            cmd.Parameters.AddWithValue("@net", 0m);
            cmd.Parameters.AddWithValue("@br", 0m);

            var idObj = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(idObj);
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> UpdateFejAsync(KiadasFej f)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                UPDATE KIADAS_FEJ
                SET
                    DATUM = @datum,
                    LEZART = @lezart,
                    PARTNER_KOD = @pk,
                    PARTNER_NEV = @pn,
                    PARTNER_IRSZ = @pi,
                    PARTNER_VAROS = @pv,
                    PARTNER_CIM = @pc,
                    MEGJEGYZES = @megj,
                    VALUTA = @val,
                    ARFOLYAM = @arf
                WHERE ID = @id
            ", _connection);

            cmd.Parameters.AddWithValue("@id", f.Id);
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

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> DeleteFejAsync(int id)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"DELETE FROM KIADAS_FEJ WHERE ID = @id", _connection);
            cmd.Parameters.AddWithValue("@id", id);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
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
                FROM KIADAS_TETEL t
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
                UPDATE KIADAS_FEJ
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

    public async Task<List<KiadasTetel>> GetTetelekAsync(string bizonylat)
    {
        bizonylat = (bizonylat ?? "").Trim();
        if (bizonylat.Length == 0) return new List<KiadasTetel>();

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT
                    t.ID,
                    t.BIZONYLAT,
                    t.TERMEK_ID,
                    t.WAREHOUSE_ID,
                    t.NEV,
                    t.ME,
                    t.AFAKOD,
                    t.MENNYISEG,
                    t.NETTO_EGYSEGAR,
                    t.BRUTTO_EGYSEGAR,
                    t.NETTO_TETELERTEK,
                    t.BRUTTO_TETELERTEK
                FROM KIADAS_TETEL t
                WHERE t.BIZONYLAT = @b
                ORDER BY t.ID
            ", _connection);

            cmd.Parameters.AddWithValue("@b", bizonylat);

            var result = new List<KiadasTetel>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new KiadasTetel
                {
                    Id = r.GetInt32(0),
                    Bizonylat = r.GetString(1),
                    TermekId = r.GetInt32(2),
                    WarehouseId = r.GetInt32(3),
                    Nev = r.GetString(4),
                    Me = r.GetString(5),
                    AfaKod = r.GetString(6),
                    Mennyiseg = r.GetDecimal(7),
                    NettoEgysegAr = r.GetDecimal(8),
                    BruttoEgysegAr = r.GetDecimal(9),
                    NettoTetelErtek = r.GetDecimal(10),
                    BruttoTetelErtek = r.GetDecimal(11),
                });
            }

            return result;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<int> CreateTetelAsync(KiadasTetel t)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                INSERT INTO KIADAS_TETEL
                    (BIZONYLAT, TERMEK_ID, WAREHOUSE_ID, NEV, ME, AFAKOD,
                     MENNYISEG,
                     NETTO_EGYSEGAR, BRUTTO_EGYSEGAR,
                     NETTO_TETELERTEK, BRUTTO_TETELERTEK)
                VALUES
                    (@b, @tid, @wid, @nev, @me, @afa,
                     @menny,
                     @ne, @be,
                     @nte, @bte)
                RETURNING ID
            ", _connection);

            cmd.Parameters.AddWithValue("@b", (t.Bizonylat ?? "").Trim());
            cmd.Parameters.AddWithValue("@tid", t.TermekId);
            cmd.Parameters.AddWithValue("@wid", t.WarehouseId);
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

            await AdjustStockByIdsAsync(t.WarehouseId, t.TermekId, deltaQty: -t.Mennyiseg);

            return newId;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> UpdateTetelAsync(KiadasTetel t)
    {
        await _connection.OpenAsync();
        try
        {
            int oldTermekId;
            int oldWarehouseId;
            decimal oldQty;

            using (var q = new FbCommand(@"
                SELECT TERMEK_ID, WAREHOUSE_ID, MENNYISEG
                FROM KIADAS_TETEL
                WHERE ID = @id
                ROWS 1
            ", _connection))
            {
                q.Parameters.AddWithValue("@id", t.Id);
                using var r = await q.ExecuteReaderAsync();
                if (!await r.ReadAsync()) return false;

                oldTermekId = r.GetInt32(0);
                oldWarehouseId = r.GetInt32(1);
                oldQty = r.GetDecimal(2);
            }

            using var cmd = new FbCommand(@"
                UPDATE KIADAS_TETEL
                SET
                    TERMEK_ID = @tid,
                    WAREHOUSE_ID = @wid,
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
            cmd.Parameters.AddWithValue("@tid", t.TermekId);
            cmd.Parameters.AddWithValue("@wid", t.WarehouseId);
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

            await AdjustStockByIdsAsync(oldWarehouseId, oldTermekId, deltaQty: +oldQty);
            await AdjustStockByIdsAsync(t.WarehouseId, t.TermekId, deltaQty: -t.Mennyiseg);

            return true;
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
            int termekId;
            int warehouseId;
            decimal qty;

            using (var q = new FbCommand(@"
                SELECT TERMEK_ID, WAREHOUSE_ID, MENNYISEG
                FROM KIADAS_TETEL
                WHERE ID = @id
                ROWS 1
            ", _connection))
            {
                q.Parameters.AddWithValue("@id", id);
                using var r = await q.ExecuteReaderAsync();
                if (!await r.ReadAsync()) return false;

                termekId = r.GetInt32(0);
                warehouseId = r.GetInt32(1);
                qty = r.GetDecimal(2);
            }

            using (var cmd = new FbCommand(@"DELETE FROM KIADAS_TETEL WHERE ID = @id", _connection))
            {
                cmd.Parameters.AddWithValue("@id", id);
                var rows = await cmd.ExecuteNonQueryAsync();
                if (rows == 0) return false;
            }

            await AdjustStockByIdsAsync(warehouseId, termekId, deltaQty: +qty);

            return true;
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
                "SELECT NEXT VALUE FOR KIADAS_BIZONYLAT_SEQ FROM RDB$DATABASE",
                _connection);

            var obj = await cmd.ExecuteScalarAsync();
            var n = Convert.ToInt64(obj, CultureInfo.InvariantCulture);

            return $"KI-{n:0000000}";
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    private async Task<string> GetNextBizonylatAsync_INTERNAL_UsesOpenConnection()
    {
        using var cmd = new FbCommand(
            "SELECT NEXT VALUE FOR KIADAS_BIZONYLAT_SEQ FROM RDB$DATABASE",
            _connection);

        var obj = await cmd.ExecuteScalarAsync();
        var n = Convert.ToInt64(obj, CultureInfo.InvariantCulture);

        return $"KI-{n:0000000}";
    }

    private async Task AdjustStockByIdsAsync(int warehouseId, int productId, decimal deltaQty)
    {
        if (deltaQty == 0) return;

        var whCode = await GetWarehouseCodeByIdAsync(warehouseId);
        var prodCode = await GetProductCodeByIdAsync(productId);

        if (string.IsNullOrWhiteSpace(whCode) || string.IsNullOrWhiteSpace(prodCode))
            throw new Exception("Stock update failed: missing warehouse code or product code.");

        await AddToStockAsync(whCode, prodCode, deltaQty);
    }

    private async Task<string> GetWarehouseCodeByIdAsync(int warehouseId)
    {
        using var cmd = new FbCommand(@"
            SELECT CODE
            FROM WAREHOUSES
            WHERE ID = @id
            ROWS 1
        ", _connection);

        cmd.Parameters.AddWithValue("@id", warehouseId);
        var obj = await cmd.ExecuteScalarAsync();
        return obj?.ToString()?.Trim() ?? "";
    }

    private async Task<string> GetProductCodeByIdAsync(int productId)
    {
        using var cmd = new FbCommand(@"
            SELECT CODE
            FROM PRODUCTS
            WHERE ID = @id
            ROWS 1
        ", _connection);

        cmd.Parameters.AddWithValue("@id", productId);
        var obj = await cmd.ExecuteScalarAsync();
        return obj?.ToString()?.Trim() ?? "";
    }

    private async Task AddToStockAsync(string warehouseCode, string productCode, decimal qtyDelta)
    {
        using (var upd = new FbCommand(@"
            UPDATE STOCK_ITEMS
            SET
                QTY = QTY + @qty,
                UPDATED_AT = CURRENT_TIMESTAMP
            WHERE WAREHOUSE_CODE = @wh
              AND PRODUCT_CODE = @pc
        ", _connection))
        {
            upd.Parameters.AddWithValue("@qty", qtyDelta);
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
        ", _connection))
        {
            ins.Parameters.AddWithValue("@wh", warehouseCode);
            ins.Parameters.AddWithValue("@pc", productCode);
            ins.Parameters.AddWithValue("@qty", qtyDelta);

            await ins.ExecuteNonQueryAsync();
        }
    }
}
