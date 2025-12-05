using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class AuthService
{
    private readonly FbConnection _connection;

    public AuthService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<User?> LoginAsync(string username, string password)
    {
        await _connection.OpenAsync();

        var cmd = new FbCommand(@"
            SELECT u.usercode, u.username, u.password, u.role_id, r.name
            FROM users u
            JOIN roles r ON r.id = u.role_id
            WHERE u.username = @username
        ", _connection);

        cmd.Parameters.AddWithValue("@username", username);

        using var reader = await cmd.ExecuteReaderAsync();

        if (!reader.Read())
        {
            await _connection.CloseAsync();
            return null;
        }

        var dbPassword = reader.GetString(2);

        if (!BCrypt.Net.BCrypt.Verify(password, dbPassword))
        {
            await _connection.CloseAsync();
            return null;
        }

        var user = new User
        {
            UserCode = reader.GetInt32(0),
            Username = reader.GetString(1),
            Password = dbPassword,
            RoleId = reader.GetInt32(3),
            RoleName = reader.GetString(4)
        };

        await _connection.CloseAsync();
        return user;
    }
}
