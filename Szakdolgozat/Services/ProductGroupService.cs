using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class ProductGroupService
{
    private readonly FbConnection _connection;

    public ProductGroupService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<ProductGroupItem>> GetAllAsync(string? search = null)
    {
        search = string.IsNullOrWhiteSpace(search) ? null : search.Trim();

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT ID, NAME
                FROM PRODUCT_GROUPS
            ";

            if (search is not null)
            {
                sql += @"
                WHERE UPPER(NAME) CONTAINING UPPER(@search)
                   OR CAST(ID AS VARCHAR(20)) CONTAINING @search
                ";
            }

            sql += " ORDER BY NAME";

            using var cmd = new FbCommand(sql, _connection);
            if (search is not null)
                cmd.Parameters.AddWithValue("@search", search);

            var list = new List<ProductGroupItem>();
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                list.Add(new ProductGroupItem
                {
                    Id = reader.GetInt32(0),
                    Name = reader.GetString(1)
                });
            }

            return list;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<int?> CreateAsync(string name)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                INSERT INTO PRODUCT_GROUPS (NAME)
                VALUES (@name)
                RETURNING ID
            ", _connection);

            cmd.Parameters.AddWithValue("@name", name.Trim());

            var idObj = await cmd.ExecuteScalarAsync();
            return idObj is null ? null : Convert.ToInt32(idObj);
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public async Task<bool> UpdateAsync(int id, string name)
    {
        await _connection.OpenAsync();
        try
        {
            using var cmd = new FbCommand(@"
                UPDATE PRODUCT_GROUPS
                SET NAME = @name
                WHERE ID = @id
            ", _connection);

            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@name", name.Trim());

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows == 1;
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
            using var cmd = new FbCommand(@"
                DELETE FROM PRODUCT_GROUPS
                WHERE ID = @id
            ", _connection);

            cmd.Parameters.AddWithValue("@id", id);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows == 1;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }
}
