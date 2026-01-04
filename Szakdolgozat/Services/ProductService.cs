using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class ProductService
{
    private readonly FbConnection _connection;

    public ProductService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<ProductItem>> GetAllAsync(string? search = null, int take = 500)
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
                    p.GROUP_ID,
                    COALESCE(g.NAME, '') AS GROUP_NAME,
                    p.UNIT
                FROM PRODUCTS p
                LEFT JOIN PRODUCT_GROUPS g ON g.ID = p.GROUP_ID
            ";

            if (search is not null)
            {
                sql += @"
                WHERE
                    UPPER(COALESCE(p.CODE, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(p.NAME, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(p.UNIT, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(g.NAME, '')) CONTAINING UPPER(@s)
                    OR CAST(p.ID AS VARCHAR(20)) CONTAINING @s
                    OR CAST(p.GROUP_ID AS VARCHAR(20)) CONTAINING @s
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

            var result = new List<ProductItem>();
            using var r = await cmd.ExecuteReaderAsync();

            while (await r.ReadAsync())
            {
                result.Add(new ProductItem
                {
                    Id = r.GetInt32(0),
                    Code = r.GetString(1),
                    Name = r.GetString(2),
                    GroupId = r.GetInt32(3),
                    GroupName = r.GetString(4),
                    Unit = r.GetString(5)
                });
            }

            return result;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<int> CreateAsync(string code, string name, int groupId, string unit)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                INSERT INTO PRODUCTS (CODE, NAME, GROUP_ID, UNIT)
                VALUES (@code, @name, @gid, @unit)
                RETURNING ID
            ", _connection);

            cmd.Parameters.AddWithValue("@code", code.Trim());
            cmd.Parameters.AddWithValue("@name", name.Trim());
            cmd.Parameters.AddWithValue("@gid", groupId);
            cmd.Parameters.AddWithValue("@unit", unit.Trim());

            var idObj = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(idObj);
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> UpdateAsync(int id, string code, string name, int groupId, string unit)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                UPDATE PRODUCTS
                SET CODE = @code,
                    NAME = @name,
                    GROUP_ID = @gid,
                    UNIT = @unit
                WHERE ID = @id
            ", _connection);

            cmd.Parameters.AddWithValue("@code", code.Trim());
            cmd.Parameters.AddWithValue("@name", name.Trim());
            cmd.Parameters.AddWithValue("@gid", groupId);
            cmd.Parameters.AddWithValue("@unit", unit.Trim());
            cmd.Parameters.AddWithValue("@id", id);

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
            using var cmd = new FbCommand(@"DELETE FROM PRODUCTS WHERE ID = @id", _connection);
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
            FROM PRODUCTS
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
            FROM PRODUCTS
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
