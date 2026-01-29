using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class ProductPriceService
{
    private readonly FbConnection _connection;

    public ProductPriceService(FbConnection connection)
    {
        _connection = connection;
    }

    /// <summary>
    /// Returns all prices for a product, newest first (by VALID_FROM).
    /// Includes VAT name/code for display + calculation.
    /// </summary>
    public async Task<List<ProductPriceItem>> GetByProductAsync(int productId)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT
                    pp.ID,
                    pp.PRODUCT_ID,
                    pp.VAT_ID,
                    COALESCE(v.CODE, '') AS VAT_CODE,
                    COALESCE(v.NAME, '') AS VAT_NAME,
                    pp.NET_PRICE,
                    pp.GROSS_PRICE,
                    pp.CURRENCY,
                    pp.VALID_FROM,
                    pp.VALID_TO,
                    pp.CREATED_AT
                FROM PRODUCT_PRICES pp
                LEFT JOIN VAT v ON v.ID = pp.VAT_ID
                WHERE pp.PRODUCT_ID = @pid
                ORDER BY pp.VALID_FROM DESC, pp.ID DESC
            ", _connection);

            cmd.Parameters.AddWithValue("@pid", productId);

            var list = new List<ProductPriceItem>();
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                list.Add(new ProductPriceItem
                {
                    Id = r.GetInt32(0),
                    ProductId = r.GetInt32(1),
                    VatId = r.GetInt32(2),
                    VatCode = r.GetString(3),
                    VatName = r.GetString(4),
                    NetPrice = r.GetDecimal(5),
                    GrossPrice = r.GetDecimal(6),
                    Currency = r.GetString(7),
                    ValidFrom = r.GetDateTime(8),
                    ValidTo = r.IsDBNull(9) ? null : r.GetDateTime(9),
                    CreatedAt = r.GetDateTime(10)
                });
            }

            return list;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    /// <summary>
    /// Closes current active price row (VALID_TO IS NULL) for the product,
    /// by setting VALID_TO to the provided timestamp.
    /// </summary>
    public async Task<int> CloseActiveAsync(int productId, DateTime validTo)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                UPDATE PRODUCT_PRICES
                SET VALID_TO = @to
                WHERE PRODUCT_ID = @pid
                  AND VALID_TO IS NULL
            ", _connection);

            cmd.Parameters.AddWithValue("@pid", productId);
            cmd.Parameters.AddWithValue("@to", validTo);

            return await cmd.ExecuteNonQueryAsync(); // rows affected (0 or more)
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    /// <summary>
    /// Inserts a new price row. Optionally closes any active row first.
    /// Returns new ID.
    /// </summary>
    public async Task<int> CreateAsync(
        int productId,
        int vatId,
        decimal netPrice,
        decimal grossPrice,
        string currency,
        DateTime validFrom,
        DateTime? validTo,
        bool closePreviousActive = true)
    {
        currency = (currency ?? "").Trim().ToUpperInvariant();
        if (currency.Length == 0) throw new ArgumentException("Currency is required.", nameof(currency));
        if (currency.Length > 3) throw new ArgumentException("Currency must be max 3 characters.", nameof(currency));

        await _connection.OpenAsync();
        try
        {
            using var tx = _connection.BeginTransaction();

            try
            {
                if (closePreviousActive)
                {
                    using var closeCmd = new FbCommand(@"
                        UPDATE PRODUCT_PRICES
                        SET VALID_TO = @to
                        WHERE PRODUCT_ID = @pid
                          AND VALID_TO IS NULL
                    ", _connection, tx);

                    // close at the new price start time (or now; you decide)
                    closeCmd.Parameters.AddWithValue("@pid", productId);
                    closeCmd.Parameters.AddWithValue("@to", validFrom);

                    await closeCmd.ExecuteNonQueryAsync();
                }

                using var insertCmd = new FbCommand(@"
                    INSERT INTO PRODUCT_PRICES
                        (PRODUCT_ID, VAT_ID, NET_PRICE, GROSS_PRICE, CURRENCY, VALID_FROM, VALID_TO, CREATED_AT)
                    VALUES
                        (@pid, @vat, @net, @gross, @cur, @vf, @vt, @created)
                    RETURNING ID
                ", _connection, tx);

                insertCmd.Parameters.AddWithValue("@pid", productId);
                insertCmd.Parameters.AddWithValue("@vat", vatId);
                insertCmd.Parameters.AddWithValue("@net", netPrice);
                insertCmd.Parameters.AddWithValue("@gross", grossPrice);
                insertCmd.Parameters.AddWithValue("@cur", currency);
                insertCmd.Parameters.AddWithValue("@vf", validFrom);
                insertCmd.Parameters.AddWithValue("@vt", (object?)validTo ?? DBNull.Value);
                insertCmd.Parameters.AddWithValue("@created", DateTime.Now);

                var idObj = await insertCmd.ExecuteScalarAsync();
                tx.Commit();

                return Convert.ToInt32(idObj);
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> UpdateAsync(
        int id,
        int vatId,
        decimal netPrice,
        decimal grossPrice,
        string currency,
        DateTime validFrom,
        DateTime? validTo)
    {
        currency = (currency ?? "").Trim().ToUpperInvariant();
        if (currency.Length == 0) throw new ArgumentException("Currency is required.", nameof(currency));
        if (currency.Length > 3) throw new ArgumentException("Currency must be max 3 characters.", nameof(currency));

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                UPDATE PRODUCT_PRICES
                SET VAT_ID = @vat,
                    NET_PRICE = @net,
                    GROSS_PRICE = @gross,
                    CURRENCY = @cur,
                    VALID_FROM = @vf,
                    VALID_TO = @vt
                WHERE ID = @id
            ", _connection);

            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@vat", vatId);
            cmd.Parameters.AddWithValue("@net", netPrice);
            cmd.Parameters.AddWithValue("@gross", grossPrice);
            cmd.Parameters.AddWithValue("@cur", currency);
            cmd.Parameters.AddWithValue("@vf", validFrom);
            cmd.Parameters.AddWithValue("@vt", (object?)validTo ?? DBNull.Value);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> DeleteAsync(int id)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"DELETE FROM PRODUCT_PRICES WHERE ID = @id", _connection);
            cmd.Parameters.AddWithValue("@id", id);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }
}
