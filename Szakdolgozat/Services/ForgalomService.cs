using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public sealed class ForgalomService
{
    private readonly FbConnection _connection;

    public ForgalomService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<ForgalomSummaryRow>> GetSummaryAsync(
        DateTime? dateFrom = null,
        DateTime? dateTo = null,
        string? search = null,
        int take = 5000)
    {
        search = string.IsNullOrWhiteSpace(search) ? null : search.Trim();
        dateFrom ??= DateTime.Today.AddMonths(-1);
        dateTo ??= DateTime.Today;

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT
                    x.PRODUCT_ID,
                    x.PRODUCT_CODE,
                    x.PRODUCT_NAME,
                    x.UNIT,
                    COALESCE(SUM(x.QTY), 0) AS FORGALOM_QTY,
                    COALESCE(SUM(x.NET_AMOUNT), 0) AS FORGALOM_NET,
                    COALESCE(SUM(x.GROSS_AMOUNT), 0) AS FORGALOM_GROSS
                FROM
                (
                    SELECT
                        t.TERMEK_ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        t.NEV AS PRODUCT_NAME,
                        t.ME AS UNIT,
                        t.MENNYISEG AS QTY,
                        t.NETTO_TETELERTEK AS NET_AMOUNT,
                        t.BRUTTO_TETELERTEK AS GROSS_AMOUNT
                    FROM BEVETEL_TETEL t
                    JOIN BEVETEL_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    LEFT JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    WHERE f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo

                    UNION ALL

                    SELECT
                        t.TERMEK_ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        t.NEV AS PRODUCT_NAME,
                        t.ME AS UNIT,
                        -t.MENNYISEG AS QTY,
                        -t.NETTO_TETELERTEK AS NET_AMOUNT,
                        -t.BRUTTO_TETELERTEK AS GROSS_AMOUNT
                    FROM KIADAS_TETEL t
                    JOIN KIADAS_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    LEFT JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    WHERE f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo
                ) x
            ";

            if (search is not null)
            {
                sql += @"
                WHERE
                    UPPER(COALESCE(x.PRODUCT_CODE, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(x.PRODUCT_NAME, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(x.UNIT, '')) CONTAINING UPPER(@s)
                    OR CAST(x.PRODUCT_ID AS VARCHAR(20)) CONTAINING @s
                ";
            }

            sql += @"
                GROUP BY
                    x.PRODUCT_ID,
                    x.PRODUCT_CODE,
                    x.PRODUCT_NAME,
                    x.UNIT
                ORDER BY
                    x.PRODUCT_NAME,
                    x.PRODUCT_CODE
                FETCH FIRST @take ROWS ONLY
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@dateFrom", dateFrom.Value.Date);
            cmd.Parameters.AddWithValue("@dateTo", dateTo.Value.Date);
            cmd.Parameters.AddWithValue("@take", take);

            if (search is not null)
                cmd.Parameters.AddWithValue("@s", search);

            var result = new List<ForgalomSummaryRow>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new ForgalomSummaryRow
                {
                    ProductId = r.GetInt32(0),
                    ProductCode = r.IsDBNull(1) ? "" : r.GetString(1),
                    ProductName = r.IsDBNull(2) ? "" : r.GetString(2),
                    Unit = r.IsDBNull(3) ? "" : r.GetString(3),
                    ForgalomQty = r.IsDBNull(4) ? 0m : r.GetDecimal(4),
                    ForgalomNet = r.IsDBNull(5) ? 0m : r.GetDecimal(5),
                    ForgalomGross = r.IsDBNull(6) ? 0m : r.GetDecimal(6)
                });
            }

            return result;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<List<ForgalomDetailRow>> GetDetailsByProductAsync(
        int productId,
        DateTime? dateFrom = null,
        DateTime? dateTo = null)
    {
        dateFrom ??= DateTime.Today.AddMonths(-1);
        dateTo ??= DateTime.Today;

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT
                    x.TIPUS,
                    x.FEJ_ID,
                    x.TETEL_ID,
                    x.BIZONYLAT,
                    x.DATUM,
                    x.PRODUCT_ID,
                    x.PRODUCT_CODE,
                    x.PRODUCT_NAME,
                    x.UNIT,
                    x.AFAKOD,
                    x.QTY,
                    x.NET_UNIT,
                    x.GROSS_UNIT,
                    x.NET_AMOUNT,
                    x.GROSS_AMOUNT,
                    x.WAREHOUSE_ID,
                    x.WAREHOUSE_NAME,
                    x.WAREHOUSE_FROM_ID,
                    x.WAREHOUSE_FROM_NAME,
                    x.WAREHOUSE_TO_ID,
                    x.WAREHOUSE_TO_NAME
                FROM
                (
                    SELECT
                        'BEVETEL' AS TIPUS,
                        f.ID AS FEJ_ID,
                        t.ID AS TETEL_ID,
                        t.BIZONYLAT,
                        f.DATUM,
                        t.TERMEK_ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        t.NEV AS PRODUCT_NAME,
                        t.ME AS UNIT,
                        t.AFAKOD,
                        t.MENNYISEG AS QTY,
                        t.NETTO_EGYSEGAR AS NET_UNIT,
                        t.BRUTTO_EGYSEGAR AS GROSS_UNIT,
                        t.NETTO_TETELERTEK AS NET_AMOUNT,
                        t.BRUTTO_TETELERTEK AS GROSS_AMOUNT,
                        t.WAREHOUSE_ID,
                        COALESCE(w.NAME || ' (' || w.CODE || ')', '') AS WAREHOUSE_NAME,
                        CAST(NULL AS INTEGER) AS WAREHOUSE_FROM_ID,
                        CAST('' AS VARCHAR(200)) AS WAREHOUSE_FROM_NAME,
                        CAST(NULL AS INTEGER) AS WAREHOUSE_TO_ID,
                        CAST('' AS VARCHAR(200)) AS WAREHOUSE_TO_NAME
                    FROM BEVETEL_TETEL t
                    JOIN BEVETEL_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    LEFT JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    LEFT JOIN WAREHOUSES w ON w.ID = t.WAREHOUSE_ID
                    WHERE t.TERMEK_ID = @pid
                      AND f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo

                    UNION ALL

                    SELECT
                        'KIADAS' AS TIPUS,
                        f.ID AS FEJ_ID,
                        t.ID AS TETEL_ID,
                        t.BIZONYLAT,
                        f.DATUM,
                        t.TERMEK_ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        t.NEV AS PRODUCT_NAME,
                        t.ME AS UNIT,
                        t.AFAKOD,
                        t.MENNYISEG AS QTY,
                        t.NETTO_EGYSEGAR AS NET_UNIT,
                        t.BRUTTO_EGYSEGAR AS GROSS_UNIT,
                        t.NETTO_TETELERTEK AS NET_AMOUNT,
                        t.BRUTTO_TETELERTEK AS GROSS_AMOUNT,
                        t.WAREHOUSE_ID,
                        COALESCE(w.NAME || ' (' || w.CODE || ')', '') AS WAREHOUSE_NAME,
                        CAST(NULL AS INTEGER) AS WAREHOUSE_FROM_ID,
                        CAST('' AS VARCHAR(200)) AS WAREHOUSE_FROM_NAME,
                        CAST(NULL AS INTEGER) AS WAREHOUSE_TO_ID,
                        CAST('' AS VARCHAR(200)) AS WAREHOUSE_TO_NAME
                    FROM KIADAS_TETEL t
                    JOIN KIADAS_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    LEFT JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    LEFT JOIN WAREHOUSES w ON w.ID = t.WAREHOUSE_ID
                    WHERE t.TERMEK_ID = @pid
                      AND f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo

                    UNION ALL

                    SELECT
                        'ATADAS' AS TIPUS,
                        f.ID AS FEJ_ID,
                        t.ID AS TETEL_ID,
                        t.BIZONYLAT,
                        f.DATUM,
                        t.TERMEK_ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        t.NEV AS PRODUCT_NAME,
                        t.ME AS UNIT,
                        t.AFAKOD,
                        t.MENNYISEG AS QTY,
                        t.NETTO_EGYSEGAR AS NET_UNIT,
                        t.BRUTTO_EGYSEGAR AS GROSS_UNIT,
                        t.NETTO_TETELERTEK AS NET_AMOUNT,
                        t.BRUTTO_TETELERTEK AS GROSS_AMOUNT,
                        CAST(NULL AS INTEGER) AS WAREHOUSE_ID,
                        CAST('' AS VARCHAR(200)) AS WAREHOUSE_NAME,
                        t.WAREHOUSE_FROM_ID,
                        COALESCE(wf.NAME || ' (' || wf.CODE || ')', '') AS WAREHOUSE_FROM_NAME,
                        t.WAREHOUSE_TO_ID,
                        COALESCE(wt.NAME || ' (' || wt.CODE || ')', '') AS WAREHOUSE_TO_NAME
                    FROM ATADAS_TETEL t
                    JOIN ATADAS_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    LEFT JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    LEFT JOIN WAREHOUSES wf ON wf.ID = t.WAREHOUSE_FROM_ID
                    LEFT JOIN WAREHOUSES wt ON wt.ID = t.WAREHOUSE_TO_ID
                    WHERE t.TERMEK_ID = @pid
                      AND f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo
                ) x
                ORDER BY x.DATUM DESC, x.TIPUS, x.BIZONYLAT, x.TETEL_ID
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@pid", productId);
            cmd.Parameters.AddWithValue("@dateFrom", dateFrom.Value.Date);
            cmd.Parameters.AddWithValue("@dateTo", dateTo.Value.Date);

            var result = new List<ForgalomDetailRow>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new ForgalomDetailRow
                {
                    Tipus = r.IsDBNull(0) ? "" : r.GetString(0),
                    FejId = r.GetInt32(1),
                    TetelId = r.GetInt32(2),
                    Bizonylat = r.IsDBNull(3) ? "" : r.GetString(3),
                    Datum = r.GetDateTime(4),
                    ProductId = r.GetInt32(5),
                    ProductCode = r.IsDBNull(6) ? "" : r.GetString(6),
                    ProductName = r.IsDBNull(7) ? "" : r.GetString(7),
                    Unit = r.IsDBNull(8) ? "" : r.GetString(8),
                    AfaKod = r.IsDBNull(9) ? "" : r.GetString(9),
                    Quantity = r.IsDBNull(10) ? 0m : r.GetDecimal(10),
                    NetUnitPrice = r.IsDBNull(11) ? 0m : r.GetDecimal(11),
                    GrossUnitPrice = r.IsDBNull(12) ? 0m : r.GetDecimal(12),
                    NetAmount = r.IsDBNull(13) ? 0m : r.GetDecimal(13),
                    GrossAmount = r.IsDBNull(14) ? 0m : r.GetDecimal(14),
                    WarehouseId = r.IsDBNull(15) ? (int?)null : r.GetInt32(15),
                    WarehouseName = r.IsDBNull(16) ? "" : r.GetString(16),
                    WarehouseFromId = r.IsDBNull(17) ? (int?)null : r.GetInt32(17),
                    WarehouseFromName = r.IsDBNull(18) ? "" : r.GetString(18),
                    WarehouseToId = r.IsDBNull(19) ? (int?)null : r.GetInt32(19),
                    WarehouseToName = r.IsDBNull(20) ? "" : r.GetString(20)
                });
            }

            return result;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }
}