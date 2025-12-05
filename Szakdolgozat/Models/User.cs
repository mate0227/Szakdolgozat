namespace Szakdolgozat.Models;

public class User
{
    public int UserCode { get; set; }
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public int RoleId { get; set; }
    public string RoleName { get; set; } = "";
}
