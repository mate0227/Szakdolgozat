using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class StockService
{
    private readonly FbConnection _connection;

    public StockService(FbConnection connection)
    {
        _connection = connection;
    }

    private const decimal DisplayZeroEpsilon = 0.00005m;

    public async Task<List<StockItem>> GetAllAsync(string? search = null, int take = 5000)
    {
        search = string.IsNullOrWhiteSpace(search) ? null : search.Trim();

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT
                    s.ID,
                    s.WAREHOUSE_CODE,
                    s.PRODUCT_CODE,
                    s.QTY,
                    s.UPDATED_AT
                FROM STOCK_ITEMS s
                WHERE ABS(s.QTY) >= @eps
            ";

            if (search is not null)
            {
                sql += @"
                AND (
                    UPPER(COALESCE(s.WAREHOUSE_CODE, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(s.PRODUCT_CODE, '')) CONTAINING UPPER(@s)
                    OR CAST(s.ID AS VARCHAR(20)) CONTAINING @s
                )
                ";
            }

            sql += @"
                ORDER BY s.WAREHOUSE_CODE, s.PRODUCT_CODE
                FETCH FIRST @take ROWS ONLY
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@eps", DisplayZeroEpsilon);
            cmd.Parameters.AddWithValue("@take", take);
            if (search is not null) cmd.Parameters.AddWithValue("@s", search);

            var list = new List<StockItem>();
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                list.Add(new StockItem
                {
                    Id = r.GetInt32(0),
                    WarehouseCode = r.GetString(1),
                    ProductCode = r.GetString(2),
                    Qty = r.GetDecimal(3),
                    UpdatedAt = r.GetDateTime(4)
                });
            }

            return list;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<List<StockItem>> GetAsOfAsync(DateTime asOfInclusive, string? search = null, int take = 5000)
    {
        search = string.IsNullOrWhiteSpace(search) ? null : search.Trim();

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT
                    CAST(0 AS INTEGER) AS ID,
                    x.WAREHOUSE_CODE,
                    x.PRODUCT_CODE,
                    SUM(x.QTY_SIGNED) AS QTY,
                    CAST(@asof AS TIMESTAMP) AS UPDATED_AT
                FROM (
                    SELECT
                        w.CODE AS WAREHOUSE_CODE,
                        p.CODE AS PRODUCT_CODE,
                        t.MENNYISEG AS QTY_SIGNED
                    FROM BEVETEL_FEJ f
                    JOIN BEVETEL_TETEL t ON t.BIZONYLAT = f.BIZONYLAT
                    JOIN WAREHOUSES w ON w.ID = t.WAREHOUSE_ID
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    WHERE f.DATUM <= @asof

                    UNION ALL

                    SELECT
                        w.CODE AS WAREHOUSE_CODE,
                        p.CODE AS PRODUCT_CODE,
                        -t.MENNYISEG AS QTY_SIGNED
                    FROM KIADAS_FEJ f
                    JOIN KIADAS_TETEL t ON t.BIZONYLAT = f.BIZONYLAT
                    JOIN WAREHOUSES w ON w.ID = t.WAREHOUSE_ID
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    WHERE f.DATUM <= @asof

                    UNION ALL

                    SELECT
                        wf.CODE AS WAREHOUSE_CODE,
                        p.CODE AS PRODUCT_CODE,
                        -t.MENNYISEG AS QTY_SIGNED
                    FROM ATADAS_FEJ f
                    JOIN ATADAS_TETEL t ON t.BIZONYLAT = f.BIZONYLAT
                    JOIN WAREHOUSES wf ON wf.ID = t.WAREHOUSE_FROM_ID
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    WHERE f.DATUM <= @asof

                    UNION ALL

                    SELECT
                        wt.CODE AS WAREHOUSE_CODE,
                        p.CODE AS PRODUCT_CODE,
                        t.MENNYISEG AS QTY_SIGNED
                    FROM ATADAS_FEJ f
                    JOIN ATADAS_TETEL t ON t.BIZONYLAT = f.BIZONYLAT
                    JOIN WAREHOUSES wt ON wt.ID = t.WAREHOUSE_TO_ID
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    WHERE f.DATUM <= @asof
                ) x
            ";

            if (search is not null)
            {
                sql += @"
                WHERE
                    UPPER(COALESCE(x.WAREHOUSE_CODE, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(x.PRODUCT_CODE, '')) CONTAINING UPPER(@s)
                ";
            }

            sql += @"
                GROUP BY x.WAREHOUSE_CODE, x.PRODUCT_CODE
                HAVING ABS(SUM(x.QTY_SIGNED)) >= @eps
                ORDER BY x.WAREHOUSE_CODE, x.PRODUCT_CODE
                FETCH FIRST @take ROWS ONLY
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@asof", asOfInclusive);
            cmd.Parameters.AddWithValue("@eps", DisplayZeroEpsilon);
            cmd.Parameters.AddWithValue("@take", take);
            if (search is not null) cmd.Parameters.AddWithValue("@s", search);

            var list = new List<StockItem>();
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                list.Add(new StockItem
                {
                    Id = 0,
                    WarehouseCode = r.IsDBNull(1) ? "" : r.GetString(1),
                    ProductCode = r.IsDBNull(2) ? "" : r.GetString(2),
                    Qty = r.IsDBNull(3) ? 0m : r.GetDecimal(3),
                    UpdatedAt = asOfInclusive
                });
            }

            return list;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }
}