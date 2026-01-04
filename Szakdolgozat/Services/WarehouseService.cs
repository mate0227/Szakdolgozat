using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class WarehouseService
{
    private readonly FbConnection _connection;

    public WarehouseService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<Warehouse>> GetAllAsync(string? search = null, int take = 500)
    {
        search = string.IsNullOrWhiteSpace(search) ? null : search.Trim();

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT
                    w.ID,
                    w.CODE,
                    w.NAME,
                    w.IS_AUTOMATON,
                    w.COUNTY,
                    w.CITY,
                    w.ADDRESS
                FROM WAREHOUSES w
            ";

            if (search is not null)
            {
                sql += @"
                WHERE
                    UPPER(COALESCE(w.CODE, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(w.NAME, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(w.COUNTY, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(w.CITY, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(w.ADDRESS, '')) CONTAINING UPPER(@s)
                    OR CAST(w.ID AS VARCHAR(20)) CONTAINING @s
                ";
            }

            sql += @"
                ORDER BY w.NAME, w.CODE
                FETCH FIRST @take ROWS ONLY
            ";

            using var cmd = new FbCommand(sql, _connection);
            cmd.Parameters.AddWithValue("@take", take);

            if (search is not null)
                cmd.Parameters.AddWithValue("@s", search);

            var result = new List<Warehouse>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new Warehouse
                {
                    Id = r.GetInt32(0),
                    Code = r.GetString(1),
                    Name = r.GetString(2),
                    IsAutomaton = r.GetBoolean(3),
                    County = r.GetString(4),
                    City = r.GetString(5),
                    Address = r.GetString(6)
                });
            }

            return result;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<int> CreateAsync(Warehouse w)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                INSERT INTO WAREHOUSES (CODE, NAME, IS_AUTOMATON, COUNTY, CITY, ADDRESS)
                VALUES (@code, @name, @isAuto, @county, @city, @address)
                RETURNING ID
            ", _connection);

            cmd.Parameters.AddWithValue("@code", (w.Code ?? "").Trim());
            cmd.Parameters.AddWithValue("@name", (w.Name ?? "").Trim());
            cmd.Parameters.AddWithValue("@isAuto", w.IsAutomaton);
            cmd.Parameters.AddWithValue("@county", (w.County ?? "").Trim());
            cmd.Parameters.AddWithValue("@city", (w.City ?? "").Trim());
            cmd.Parameters.AddWithValue("@address", (w.Address ?? "").Trim());

            var idObj = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(idObj);
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> UpdateAsync(Warehouse w)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                UPDATE WAREHOUSES
                SET CODE = @code,
                    NAME = @name,
                    IS_AUTOMATON = @isAuto,
                    COUNTY = @county,
                    CITY = @city,
                    ADDRESS = @address
                WHERE ID = @id
            ", _connection);

            cmd.Parameters.AddWithValue("@id", w.Id);
            cmd.Parameters.AddWithValue("@code", (w.Code ?? "").Trim());
            cmd.Parameters.AddWithValue("@name", (w.Name ?? "").Trim());
            cmd.Parameters.AddWithValue("@isAuto", w.IsAutomaton);
            cmd.Parameters.AddWithValue("@county", (w.County ?? "").Trim());
            cmd.Parameters.AddWithValue("@city", (w.City ?? "").Trim());
            cmd.Parameters.AddWithValue("@address", (w.Address ?? "").Trim());

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
            using var cmd = new FbCommand(@"DELETE FROM WAREHOUSES WHERE ID = @id", _connection);
            cmd.Parameters.AddWithValue("@id", id);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }


    public async Task<bool> CodeExistsAsync(string code)
    {
        code = (code ?? "").Trim();
        if (code.Length == 0) return false;

        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                SELECT 1
                FROM WAREHOUSES
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
                FROM WAREHOUSES
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
