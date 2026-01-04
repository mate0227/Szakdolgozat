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

    public async Task<List<User>> GetAllUsersAsync()
    {
        await _connection.OpenAsync();

        var cmd = new FbCommand(@"
        SELECT u.usercode, u.username, u.role_id, r.name
        FROM users u
        LEFT JOIN roles r ON r.id = u.role_id
        ORDER BY u.username
    ", _connection);

        var users = new List<User>();

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            users.Add(new User
            {
                UserCode = reader.GetInt32(0),
                Username = reader.GetString(1),
                RoleId = reader.GetInt32(2),
                RoleName = reader.IsDBNull(3) ? "" : reader.GetString(3)
            });
        }

        await _connection.CloseAsync();
        return users;
    }

    public async Task<bool> CreateUserAsync(string username, string password, int roleId)
    {
        await _connection.OpenAsync();

        string hashed = BCrypt.Net.BCrypt.HashPassword(password);

        var cmd = new FbCommand(@"
        INSERT INTO users (username, password, role_id)
        VALUES (@username, @password, @role)
    ", _connection);

        cmd.Parameters.AddWithValue("@username", username);
        cmd.Parameters.AddWithValue("@password", hashed);
        cmd.Parameters.AddWithValue("@role", roleId);

        int rows = await cmd.ExecuteNonQueryAsync();

        await _connection.CloseAsync();
        return rows > 0;
    }


    public async Task<bool> UpdateUserAsync(int userCode, string username, int roleId)
    {
        await _connection.OpenAsync();

        var cmd = new FbCommand(@"
        UPDATE users
        SET username = @username,
            role_id = @role
        WHERE usercode = @usercode
    ", _connection);

        cmd.Parameters.AddWithValue("@username", username);
        cmd.Parameters.AddWithValue("@role", roleId);
        cmd.Parameters.AddWithValue("@usercode", userCode);

        int rows = await cmd.ExecuteNonQueryAsync();
        await _connection.CloseAsync();

        return rows > 0;
    }

    public async Task<bool> DeleteUserAsync(int userCode)
    {
        await _connection.OpenAsync();

        var cmd = new FbCommand(@"
        DELETE FROM users
        WHERE usercode = @usercode
    ", _connection);

        cmd.Parameters.AddWithValue("@usercode", userCode);

        int rows = await cmd.ExecuteNonQueryAsync();
        await _connection.CloseAsync();

        return rows > 0;
    }



}
