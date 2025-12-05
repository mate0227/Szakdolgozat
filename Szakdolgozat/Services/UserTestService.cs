using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class UserTestService
{
    private readonly FbConnection _connection;

    public UserTestService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<User>> GetAllUsersAsync()
    {
        var users = new List<User>();

        await _connection.OpenAsync();

        var cmd = new FbCommand(@"
            SELECT u.usercode, u.username, u.password, u.role_id, r.name
            FROM users u
            LEFT JOIN roles r ON r.id = u.role_id
        ", _connection);

        using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            users.Add(new User
            {
                UserCode = reader.GetInt32(0),
                Username = reader.GetString(1),
                Password = reader.GetString(2),
                RoleId = reader.GetInt32(3),
                RoleName = reader.IsDBNull(4) ? "NULL" : reader.GetString(4)
            });
        }

        await _connection.CloseAsync();
        return users;
    }
}
