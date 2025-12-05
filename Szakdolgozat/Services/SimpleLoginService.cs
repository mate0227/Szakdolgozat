using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class SimpleLoginService
{
    private readonly FbConnection _connection;

    public SimpleLoginService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<User?> LoginAsync(string username, string password)
    {
        await _connection.OpenAsync();

        var cmd = new FbCommand(@"
            SELECT u.usercode, u.username, u.password, u.role_id, r.name
            FROM users u
            LEFT JOIN roles r ON r.id = u.role_id
            WHERE u.username = @username
        ", _connection);

        cmd.Parameters.AddWithValue("@username", username);

        using var reader = await cmd.ExecuteReaderAsync();

        if (!await reader.ReadAsync())
        {
            await _connection.CloseAsync();
            return null;
        }

        var dbPasswordHash = reader.GetString(2);

        // ✅ BCrypt password verification
        if (!BCrypt.Net.BCrypt.Verify(password, dbPasswordHash))
        {
            await _connection.CloseAsync();
            return null;
        }

        var user = new User
        {
            UserCode = reader.GetInt32(0),
            Username = reader.GetString(1),
            Password = dbPasswordHash, // keep hash, not plain text
            RoleId = reader.GetInt32(3),
            RoleName = reader.IsDBNull(4) ? "" : reader.GetString(4)
        };

        await _connection.CloseAsync();
        return user;
    }
}
