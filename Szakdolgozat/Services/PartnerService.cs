using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class PartnerService
{
    private readonly FbConnection _connection;

    public PartnerService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<Partner>> GetAllAsync(string? search = null, int take = 500)
    {
        search = string.IsNullOrWhiteSpace(search) ? null : search.Trim();

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT
                    p.ID,
                    p.CODE,
                    p.NAME,
                    p.IS_CUSTOMER,
                    p.IS_SUPPLIER,
                    p.COUNTRY,
                    p.COUNTY,
                    p.CITY,
                    p.ADDRESS,
                    p.EMAIL,
                    p.PHONE,
                    p.TAX_NUMBER
                FROM PARTNERS p
            ";

            if (search is not null)
            {
                sql += @"
                WHERE
                    UPPER(COALESCE(p.CODE, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(p.NAME, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(p.COUNTRY, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(p.COUNTY, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(p.CITY, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(p.ADDRESS, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(p.EMAIL, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(p.PHONE, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(p.TAX_NUMBER, '')) CONTAINING UPPER(@s)
                    OR CAST(p.ID AS VARCHAR(20)) CONTAINING @s
                ";
            }

            sql += @"
                ORDER BY p.NAME, p.CODE
                FETCH FIRST @take ROWS ONLY
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@take", take);

            if (search is not null)
                cmd.Parameters.AddWithValue("@s", search);

            var result = new List<Partner>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new Partner
                {
                    Id = r.GetInt32(0),
                    Code = r.GetString(1),
                    Name = r.GetString(2),
                    IsCustomer = r.GetBoolean(3),
                    IsSupplier = r.GetBoolean(4),
                    Country = r.GetString(5),
                    County = r.GetString(6),
                    City = r.GetString(7),
                    Address = r.GetString(8),
                    Email = r.IsDBNull(9) ? null : r.GetString(9),
                    Phone = r.IsDBNull(10) ? null : r.GetString(10),
                    TaxNumber = r.IsDBNull(11) ? null : r.GetString(11)
                });
            }

            return result;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<int> CreateAsync(Partner p)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                INSERT INTO PARTNERS
                    (CODE, NAME, IS_CUSTOMER, IS_SUPPLIER, COUNTRY, COUNTY, CITY, ADDRESS, EMAIL, PHONE, TAX_NUMBER)
                VALUES
                    (@code, @name, @isCustomer, @isSupplier, @country, @county, @city, @address, @email, @phone, @tax)
                RETURNING ID
            ", _connection);

            cmd.Parameters.AddWithValue("@code", (p.Code ?? "").Trim());
            cmd.Parameters.AddWithValue("@name", (p.Name ?? "").Trim());
            cmd.Parameters.AddWithValue("@isCustomer", p.IsCustomer);
            cmd.Parameters.AddWithValue("@isSupplier", p.IsSupplier);
            cmd.Parameters.AddWithValue("@country", (p.Country ?? "").Trim());
            cmd.Parameters.AddWithValue("@county", (p.County ?? "").Trim());
            cmd.Parameters.AddWithValue("@city", (p.City ?? "").Trim());
            cmd.Parameters.AddWithValue("@address", (p.Address ?? "").Trim());

            var email = string.IsNullOrWhiteSpace(p.Email) ? null : p.Email.Trim();
            var phone = string.IsNullOrWhiteSpace(p.Phone) ? null : p.Phone.Trim();
            var tax = string.IsNullOrWhiteSpace(p.TaxNumber) ? null : p.TaxNumber.Trim();

            cmd.Parameters.AddWithValue("@email", (object?)email ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@phone", (object?)phone ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@tax", (object?)tax ?? DBNull.Value);

            var idObj = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(idObj);
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> UpdateAsync(Partner p)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                UPDATE PARTNERS
                SET
                    CODE = @code,
                    NAME = @name,
                    IS_CUSTOMER = @isCustomer,
                    IS_SUPPLIER = @isSupplier,
                    COUNTRY = @country,
                    COUNTY = @county,
                    CITY = @city,
                    ADDRESS = @address,
                    EMAIL = @email,
                    PHONE = @phone,
                    TAX_NUMBER = @tax
                WHERE ID = @id
            ", _connection);

            cmd.Parameters.AddWithValue("@id", p.Id);
            cmd.Parameters.AddWithValue("@code", (p.Code ?? "").Trim());
            cmd.Parameters.AddWithValue("@name", (p.Name ?? "").Trim());
            cmd.Parameters.AddWithValue("@isCustomer", p.IsCustomer);
            cmd.Parameters.AddWithValue("@isSupplier", p.IsSupplier);
            cmd.Parameters.AddWithValue("@country", (p.Country ?? "").Trim());
            cmd.Parameters.AddWithValue("@county", (p.County ?? "").Trim());
            cmd.Parameters.AddWithValue("@city", (p.City ?? "").Trim());
            cmd.Parameters.AddWithValue("@address", (p.Address ?? "").Trim());

            var email = string.IsNullOrWhiteSpace(p.Email) ? null : p.Email.Trim();
            var phone = string.IsNullOrWhiteSpace(p.Phone) ? null : p.Phone.Trim();
            var tax = string.IsNullOrWhiteSpace(p.TaxNumber) ? null : p.TaxNumber.Trim();

            cmd.Parameters.AddWithValue("@email", (object?)email ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@phone", (object?)phone ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@tax", (object?)tax ?? DBNull.Value);

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
            using var cmd = new FbCommand(@"DELETE FROM PARTNERS WHERE ID = @id", _connection);
            cmd.Parameters.AddWithValue("@id", id);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    // --------- Duplicate CODE checks ----------

    public async Task<bool> CodeExistsAsync(string code)
    {
        code = (code ?? "").Trim();
        if (code.Length == 0) return false;

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT 1
                FROM PARTNERS
                WHERE CODE = @code
                ROWS 1
            ", _connection);

            cmd.Parameters.AddWithValue("@code", code);

            var obj = await cmd.ExecuteScalarAsync();
            return obj != null;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> CodeExistsForOtherAsync(int id, string code)
    {
        code = (code ?? "").Trim();
        if (code.Length == 0) return false;

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT 1
                FROM PARTNERS
                WHERE CODE = @code
                  AND ID <> @id
                ROWS 1
            ", _connection);

            cmd.Parameters.AddWithValue("@code", code);
            cmd.Parameters.AddWithValue("@id", id);

            var obj = await cmd.ExecuteScalarAsync();
            return obj != null;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }
}
