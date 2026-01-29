using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class VatService
{
    private readonly FbConnection _connection;

    public VatService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<VatItem>> GetAllAsync(string? search = null)
    {
        search = string.IsNullOrWhiteSpace(search) ? null : search.Trim();

        await _connection.OpenAsync();
        try
        {
            var sql = @"
                SELECT ID, CODE, NAME
                FROM VAT
            ";

            if (search is not null)
            {
                sql += @"
                WHERE
                    UPPER(COALESCE(CODE, '')) CONTAINING UPPER(@s)
                    OR UPPER(COALESCE(NAME, '')) CONTAINING UPPER(@s)
                    OR CAST(ID AS VARCHAR(20)) CONTAINING @s
                ";
            }

            sql += " ORDER BY NAME, CODE";

            using var cmd = new FbCommand(sql, _connection);
            if (search is not null)
                cmd.Parameters.AddWithValue("@s", search);

            var list = new List<VatItem>();
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                list.Add(new VatItem
                {
                    Id = r.GetInt32(0),
                    Code = r.GetString(1),
                    Name = r.GetString(2)
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
