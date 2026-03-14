using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public sealed class PriceReportService
{
    private readonly FbConnection _connection;

    public PriceReportService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<PriceReportRow>> GetAllAsync(
        string? search = null,
        string? currency = null,
        DateTime? asOf = null,
        int take = 5000)
    {
        search = string.IsNullOrWhiteSpace(search) ? null : search.Trim();
        currency = string.IsNullOrWhiteSpace(currency) ? null : currency.Trim().ToUpperInvariant();
        asOf ??= DateTime.Today;

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT
                    p.ID,
                    p.CODE,
                    p.NAME,
                    COALESCE(g.NAME, '') AS GROUP_NAME,
                    p.UNIT,

                    COALESCE(v.CODE, '') AS VAT_CODE,
                    COALESCE(v.NAME, '') AS VAT_NAME,

                    pp.NET_PRICE,
                    pp.GROSS_PRICE,
                    pp.CURRENCY,
                    pp.VALID_FROM,
                    pp.VALID_TO
                FROM PRODUCTS p
                LEFT JOIN PRODUCT_GROUPS g ON g.ID = p.GROUP_ID
                LEFT JOIN PRODUCT_PRICES pp
                    ON pp.PRODUCT_ID = p.ID
                   AND pp.VALID_FROM = (
                        SELECT MAX(pp2.VALID_FROM)
                        FROM PRODUCT_PRICES pp2
                        WHERE pp2.PRODUCT_ID = p.ID
                          AND pp2.VALID_FROM <= @asOf
                          AND (pp2.VALID_TO IS NULL OR pp2.VALID_TO >= @asOf)
                          AND (@cur IS NULL OR pp2.CURRENCY = @cur)
                   )
                LEFT JOIN VAT v ON v.ID = pp.VAT_ID
                WHERE
                    (@cur IS NULL OR pp.PRODUCT_ID IS NOT NULL)
            ";

            if (search is not null)
            {
                sql += @"
                    AND (
                        UPPER(COALESCE(p.CODE, '')) CONTAINING UPPER(@s)
                     OR UPPER(COALESCE(p.NAME, '')) CONTAINING UPPER(@s)
                     OR UPPER(COALESCE(p.UNIT, '')) CONTAINING UPPER(@s)
                     OR UPPER(COALESCE(g.NAME, '')) CONTAINING UPPER(@s)
                     OR UPPER(COALESCE(pp.CURRENCY, '')) CONTAINING UPPER(@s)
                     OR UPPER(COALESCE(v.CODE, '')) CONTAINING UPPER(@s)
                     OR CAST(p.ID AS VARCHAR(20)) CONTAINING @s
                    )
                ";
            }

            sql += @"
                ORDER BY p.NAME, p.CODE
                FETCH FIRST @take ROWS ONLY
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@asOf", asOf.Value.Date);
            cmd.Parameters.AddWithValue("@cur", (object?)currency ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@take", take);

            if (search is not null)
                cmd.Parameters.AddWithValue("@s", search);

            var result = new List<PriceReportRow>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new PriceReportRow
                {
                    ProductId = r.GetInt32(0),
                    ProductCode = r.GetString(1),
                    ProductName = r.GetString(2),
                    GroupName = r.GetString(3),
                    Unit = r.GetString(4),

                    VatCode = r.GetString(5),
                    VatName = r.GetString(6),

                    NetPrice = r.IsDBNull(7) ? (decimal?)null : r.GetDecimal(7),
                    GrossPrice = r.IsDBNull(8) ? (decimal?)null : r.GetDecimal(8),
                    Currency = r.IsDBNull(9) ? "" : r.GetString(9),
                    ValidFrom = r.IsDBNull(10) ? (DateTime?)null : r.GetDateTime(10),
                    ValidTo = r.IsDBNull(11) ? (DateTime?)null : r.GetDateTime(11)
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