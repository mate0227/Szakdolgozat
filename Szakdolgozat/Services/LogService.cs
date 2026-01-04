using FirebirdSql.Data.FirebirdClient;
using Szakdolgozat.Models;

namespace Szakdolgozat.Services;

public class LogService
{
    private readonly FbConnection _connection;

    public LogService(FbConnection connection)
    {
        _connection = connection;
    }

    public async Task<List<UserLogEntry>> GetLogsAsync(int take = 200, string? search = null)
    {
        await _connection.OpenAsync();

        var sql = @"
            SELECT
                l.id,
                l.usercode,
                COALESCE(u.username, '') AS username,
                l.action_type,
                l.action_text,
                l.page,
                l.entity_name,
                l.entity_id,
                l.created_at,
                l.ip_address
            FROM user_logs l
            LEFT JOIN users u ON u.usercode = l.usercode
        ";

        if (!string.IsNullOrWhiteSpace(search))
        {
            sql += @"
            WHERE
                UPPER(COALESCE(u.username, '')) CONTAINING UPPER(@search)
                OR UPPER(COALESCE(l.action_type, '')) CONTAINING UPPER(@search)
                OR UPPER(COALESCE(l.action_text, '')) CONTAINING UPPER(@search)
                OR UPPER(COALESCE(l.page, '')) CONTAINING UPPER(@search)
            ";
        }

        sql += @"
            ORDER BY l.created_at DESC, l.id DESC
            ROWS @take
        ";

        var cmd = new FbCommand(sql, _connection);

        cmd.Parameters.AddWithValue("@take", take);

        if (!string.IsNullOrWhiteSpace(search))
            cmd.Parameters.AddWithValue("@search", search);

        var logs = new List<UserLogEntry>();

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            logs.Add(new UserLogEntry
            {
                Id = reader.GetInt64(0),
                UserCode = reader.GetInt32(1),
                Username = reader.GetString(2),
                ActionType = reader.GetString(3),
                ActionText = reader.IsDBNull(4) ? null : reader.GetString(4),
                Page = reader.IsDBNull(5) ? null : reader.GetString(5),
                EntityName = reader.IsDBNull(6) ? null : reader.GetString(6),
                EntityId = reader.IsDBNull(7) ? null : reader.GetString(7),
                CreatedAt = reader.GetDateTime(8),
                IpAddress = reader.IsDBNull(9) ? null : reader.GetString(9),
            });
        }

        await _connection.CloseAsync();
        return logs;
    }

    public async Task AddLogAsync(
    int userCode,
    string actionType,
    string? actionText = null,
    string? page = null,
    string? entityName = null,
    string? entityId = null,
    string? ipAddress = null)
    {
        await _connection.OpenAsync();

        try
        {
            using var cmd = new FbCommand(@"
            INSERT INTO USER_LOGS
                (USERCODE, ACTION_TYPE, ACTION_TEXT, PAGE, ENTITY_NAME, ENTITY_ID, CREATED_AT, IP_ADDRESS)
            VALUES
                (@uc, @at, @txt, @pg, @en, @eid, CURRENT_TIMESTAMP, @ip)
        ", _connection);

            cmd.Parameters.AddWithValue("@uc", userCode);
            cmd.Parameters.AddWithValue("@at", actionType);
            cmd.Parameters.AddWithValue("@txt", (object?)actionText ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@pg", (object?)page ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@en", (object?)entityName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@eid", (object?)entityId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@ip", (object?)ipAddress ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync();
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

}
