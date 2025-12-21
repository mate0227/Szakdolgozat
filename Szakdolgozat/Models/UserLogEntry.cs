namespace Szakdolgozat.Models;

public class UserLogEntry
{
    public long Id { get; set; }
    public int UserCode { get; set; }
    public string Username { get; set; } = "";
    public string ActionType { get; set; } = "";
    public string? ActionText { get; set; }
    public string? Page { get; set; }
    public string? EntityName { get; set; }
    public string? EntityId { get; set; }
    public DateTime CreatedAt { get; set; }
    public string? IpAddress { get; set; }
}
