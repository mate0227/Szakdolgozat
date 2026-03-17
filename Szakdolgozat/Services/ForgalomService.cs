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

        var fromDate = dateFrom.Value.Date;
        var toDate = dateTo.Value.Date;
        var nyitoDate = fromDate.AddDays(-1);

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT
                    x.WAREHOUSE_CODE,
                    x.WAREHOUSE_NAME,
                    x.PRODUCT_ID,
                    x.PRODUCT_CODE,
                    x.PRODUCT_NAME,
                    x.UNIT,

                    COALESCE(SUM(x.NYITO_QTY), 0)   AS NYITO_QTY,
                    COALESCE(SUM(x.BEVETEL_QTY), 0) AS BEVETEL_QTY,
                    COALESCE(SUM(x.KIADAS_QTY), 0)  AS KIADAS_QTY
                FROM
                (
                    SELECT
                        w.CODE AS WAREHOUSE_CODE,
                        w.NAME || ' (' || w.CODE || ')' AS WAREHOUSE_NAME,
                        p.ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        p.NAME AS PRODUCT_NAME,
                        p.UNIT AS UNIT,

                        t.MENNYISEG AS NYITO_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS BEVETEL_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS KIADAS_QTY
                    FROM BEVETEL_TETEL t
                    JOIN BEVETEL_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    JOIN WAREHOUSES w ON w.ID = t.WAREHOUSE_ID
                    WHERE f.DATUM <= @nyitoDate

                    UNION ALL

                    SELECT
                        w.CODE AS WAREHOUSE_CODE,
                        w.NAME || ' (' || w.CODE || ')' AS WAREHOUSE_NAME,
                        p.ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        p.NAME AS PRODUCT_NAME,
                        p.UNIT AS UNIT,

                        -t.MENNYISEG AS NYITO_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS BEVETEL_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS KIADAS_QTY
                    FROM KIADAS_TETEL t
                    JOIN KIADAS_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    JOIN WAREHOUSES w ON w.ID = t.WAREHOUSE_ID
                    WHERE f.DATUM <= @nyitoDate

                    UNION ALL

                    SELECT
                        wf.CODE AS WAREHOUSE_CODE,
                        wf.NAME || ' (' || wf.CODE || ')' AS WAREHOUSE_NAME,
                        p.ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        p.NAME AS PRODUCT_NAME,
                        p.UNIT AS UNIT,

                        -t.MENNYISEG AS NYITO_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS BEVETEL_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS KIADAS_QTY
                    FROM ATADAS_TETEL t
                    JOIN ATADAS_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    JOIN WAREHOUSES wf ON wf.ID = t.WAREHOUSE_FROM_ID
                    WHERE f.DATUM <= @nyitoDate

                    UNION ALL

                    SELECT
                        wt.CODE AS WAREHOUSE_CODE,
                        wt.NAME || ' (' || wt.CODE || ')' AS WAREHOUSE_NAME,
                        p.ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        p.NAME AS PRODUCT_NAME,
                        p.UNIT AS UNIT,

                        t.MENNYISEG AS NYITO_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS BEVETEL_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS KIADAS_QTY
                    FROM ATADAS_TETEL t
                    JOIN ATADAS_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    JOIN WAREHOUSES wt ON wt.ID = t.WAREHOUSE_TO_ID
                    WHERE f.DATUM <= @nyitoDate

                    UNION ALL

                    SELECT
                        w.CODE AS WAREHOUSE_CODE,
                        w.NAME || ' (' || w.CODE || ')' AS WAREHOUSE_NAME,
                        p.ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        p.NAME AS PRODUCT_NAME,
                        p.UNIT AS UNIT,

                        CAST(0 AS DECIMAL(18,4)) AS NYITO_QTY,
                        t.MENNYISEG AS BEVETEL_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS KIADAS_QTY
                    FROM BEVETEL_TETEL t
                    JOIN BEVETEL_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    JOIN WAREHOUSES w ON w.ID = t.WAREHOUSE_ID
                    WHERE f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo

                    UNION ALL

                    SELECT
                        w.CODE AS WAREHOUSE_CODE,
                        w.NAME || ' (' || w.CODE || ')' AS WAREHOUSE_NAME,
                        p.ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        p.NAME AS PRODUCT_NAME,
                        p.UNIT AS UNIT,

                        CAST(0 AS DECIMAL(18,4)) AS NYITO_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS BEVETEL_QTY,
                        t.MENNYISEG AS KIADAS_QTY
                    FROM KIADAS_TETEL t
                    JOIN KIADAS_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    JOIN WAREHOUSES w ON w.ID = t.WAREHOUSE_ID
                    WHERE f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo

                    UNION ALL

                    SELECT
                        wt.CODE AS WAREHOUSE_CODE,
                        wt.NAME || ' (' || wt.CODE || ')' AS WAREHOUSE_NAME,
                        p.ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        p.NAME AS PRODUCT_NAME,
                        p.UNIT AS UNIT,

                        CAST(0 AS DECIMAL(18,4)) AS NYITO_QTY,
                        t.MENNYISEG AS BEVETEL_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS KIADAS_QTY
                    FROM ATADAS_TETEL t
                    JOIN ATADAS_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    JOIN WAREHOUSES wt ON wt.ID = t.WAREHOUSE_TO_ID
                    WHERE f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo

                    UNION ALL

                    SELECT
                        wf.CODE AS WAREHOUSE_CODE,
                        wf.NAME || ' (' || wf.CODE || ')' AS WAREHOUSE_NAME,
                        p.ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        p.NAME AS PRODUCT_NAME,
                        p.UNIT AS UNIT,

                        CAST(0 AS DECIMAL(18,4)) AS NYITO_QTY,
                        CAST(0 AS DECIMAL(18,4)) AS BEVETEL_QTY,
                        t.MENNYISEG AS KIADAS_QTY
                    FROM ATADAS_TETEL t
                    JOIN ATADAS_FEJ f ON f.BIZONYLAT = t.BIZONYLAT
                    JOIN PRODUCTS p ON p.ID = t.TERMEK_ID
                    JOIN WAREHOUSES wf ON wf.ID = t.WAREHOUSE_FROM_ID
                    WHERE f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo
                ) x
            ";

            if (search is not null)
            {
                sql += @"
                WHERE
                    UPPER(COALESCE(x.WAREHOUSE_CODE, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(x.WAREHOUSE_NAME, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(x.PRODUCT_CODE, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(x.PRODUCT_NAME, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(x.UNIT, '')) CONTAINING UPPER(@s)
                    OR CAST(x.PRODUCT_ID AS VARCHAR(20)) CONTAINING @s
                ";
            }

            sql += @"
                GROUP BY
                    x.WAREHOUSE_CODE,
                    x.WAREHOUSE_NAME,
                    x.PRODUCT_ID,
                    x.PRODUCT_CODE,
                    x.PRODUCT_NAME,
                    x.UNIT
                ORDER BY
                    x.WAREHOUSE_CODE,
                    x.PRODUCT_NAME,
                    x.PRODUCT_CODE
                FETCH FIRST @take ROWS ONLY
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@nyitoDate", nyitoDate);
            cmd.Parameters.AddWithValue("@dateFrom", fromDate);
            cmd.Parameters.AddWithValue("@dateTo", toDate);
            cmd.Parameters.AddWithValue("@take", take);

            if (search is not null)
                cmd.Parameters.AddWithValue("@s", search);

            var result = new List<ForgalomSummaryRow>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                var nyito = r.IsDBNull(6) ? 0m : r.GetDecimal(6);
                var bevetel = r.IsDBNull(7) ? 0m : r.GetDecimal(7);
                var kiadas = r.IsDBNull(8) ? 0m : r.GetDecimal(8);

                result.Add(new ForgalomSummaryRow
                {
                    WarehouseCode = r.IsDBNull(0) ? "" : r.GetString(0),
                    WarehouseName = r.IsDBNull(1) ? "" : r.GetString(1),
                    ProductId = r.GetInt32(2),
                    ProductCode = r.IsDBNull(3) ? "" : r.GetString(3),
                    ProductName = r.IsDBNull(4) ? "" : r.GetString(4),
                    Unit = r.IsDBNull(5) ? "" : r.GetString(5),
                    NyitoQty = nyito,
                    BevetelQty = bevetel,
                    KiadasQty = kiadas,
                    ZaroQty = nyito + bevetel - kiadas
                });
            }

            return result;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<List<ForgalomDetailRow>> GetDetailsAsync(
        string warehouseCode,
        int productId,
        DateTime? dateFrom = null,
        DateTime? dateTo = null)
    {
        warehouseCode = (warehouseCode ?? "").Trim();
        dateFrom ??= DateTime.Today.AddMonths(-1);
        dateTo ??= DateTime.Today;

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT
                    x.TIPUS,
                    x.BIZONYLAT,
                    x.DATUM,
                    x.PRODUCT_ID,
                    x.PRODUCT_CODE,
                    x.PRODUCT_NAME,
                    x.UNIT,
                    x.QTY,
                    x.SIGNED_QTY,
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
                        t.BIZONYLAT,
                        f.DATUM,
                        t.TERMEK_ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        t.NEV AS PRODUCT_NAME,
                        t.ME AS UNIT,
                        t.MENNYISEG AS QTY,
                        t.MENNYISEG AS SIGNED_QTY,
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
                      AND w.CODE = @whCode
                      AND f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo

                    UNION ALL

                    SELECT
                        'KIADAS' AS TIPUS,
                        t.BIZONYLAT,
                        f.DATUM,
                        t.TERMEK_ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        t.NEV AS PRODUCT_NAME,
                        t.ME AS UNIT,
                        t.MENNYISEG AS QTY,
                        -t.MENNYISEG AS SIGNED_QTY,
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
                      AND w.CODE = @whCode
                      AND f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo

                    UNION ALL

                    SELECT
                        'ATADAS_KIADAS' AS TIPUS,
                        t.BIZONYLAT,
                        f.DATUM,
                        t.TERMEK_ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        t.NEV AS PRODUCT_NAME,
                        t.ME AS UNIT,
                        t.MENNYISEG AS QTY,
                        -t.MENNYISEG AS SIGNED_QTY,
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
                      AND wf.CODE = @whCode
                      AND f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo

                    UNION ALL

                    SELECT
                        'ATADAS_BEVETEL' AS TIPUS,
                        t.BIZONYLAT,
                        f.DATUM,
                        t.TERMEK_ID AS PRODUCT_ID,
                        p.CODE AS PRODUCT_CODE,
                        t.NEV AS PRODUCT_NAME,
                        t.ME AS UNIT,
                        t.MENNYISEG AS QTY,
                        t.MENNYISEG AS SIGNED_QTY,
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
                      AND wt.CODE = @whCode
                      AND f.DATUM >= @dateFrom
                      AND f.DATUM <= @dateTo
                ) x
                ORDER BY x.DATUM DESC, x.BIZONYLAT
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@pid", productId);
            cmd.Parameters.AddWithValue("@whCode", warehouseCode);
            cmd.Parameters.AddWithValue("@dateFrom", dateFrom.Value.Date);
            cmd.Parameters.AddWithValue("@dateTo", dateTo.Value.Date);

            var result = new List<ForgalomDetailRow>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new ForgalomDetailRow
                {
                    Tipus = r.IsDBNull(0) ? "" : r.GetString(0),
                    Bizonylat = r.IsDBNull(1) ? "" : r.GetString(1),
                    Datum = r.GetDateTime(2),
                    ProductId = r.GetInt32(3),
                    ProductCode = r.IsDBNull(4) ? "" : r.GetString(4),
                    ProductName = r.IsDBNull(5) ? "" : r.GetString(5),
                    Unit = r.IsDBNull(6) ? "" : r.GetString(6),
                    Quantity = r.IsDBNull(7) ? 0m : r.GetDecimal(7),
                    SignedQuantity = r.IsDBNull(8) ? 0m : r.GetDecimal(8),
                    NetUnitPrice = r.IsDBNull(9) ? 0m : r.GetDecimal(9),
                    GrossUnitPrice = r.IsDBNull(10) ? 0m : r.GetDecimal(10),
                    NetAmount = r.IsDBNull(11) ? 0m : r.GetDecimal(11),
                    GrossAmount = r.IsDBNull(12) ? 0m : r.GetDecimal(12),
                    WarehouseId = r.IsDBNull(13) ? null : r.GetInt32(13),
                    WarehouseName = r.IsDBNull(14) ? "" : r.GetString(14),
                    WarehouseFromId = r.IsDBNull(15) ? null : r.GetInt32(15),
                    WarehouseFromName = r.IsDBNull(16) ? "" : r.GetString(16),
                    WarehouseToId = r.IsDBNull(17) ? null : r.GetInt32(17),
                    WarehouseToName = r.IsDBNull(18) ? "" : r.GetString(18)
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