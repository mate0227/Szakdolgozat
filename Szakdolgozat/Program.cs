using Szakdolgozat.Components;
using Szakdolgozat.Services;
using FirebirdSql.Data.FirebirdClient;
using Microsoft.AspNetCore.Authentication.Cookies;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddScoped<FbConnection>(sp =>
{
    var cs = builder.Configuration.GetConnectionString("Firebird");
    return new FbConnection(cs);
});

builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie(options =>
    {
        options.LoginPath = "/login";
        options.LogoutPath = "/logout";
        options.AccessDeniedPath = "/denied";
        options.ExpireTimeSpan = TimeSpan.FromHours(8);
    });

builder.Services.AddAuthorization();
builder.Services.AddHttpContextAccessor();
builder.Services.AddScoped<AuthService>();
builder.Services.AddScoped<UserTestService>();
builder.Services.AddScoped<SimpleLoginService>();
builder.Services.AddScoped<UserSession>();
builder.Services.AddScoped<LogService>();
builder.Services.AddScoped<ProductGroupService>();
builder.Services.AddScoped<ProductService>();
builder.Services.AddScoped<WarehouseService>();
builder.Services.AddScoped<PartnerService>();
builder.Services.AddScoped<VatService>();
builder.Services.AddScoped<ProductPriceService>();
builder.Services.AddScoped<BevetelService>();
builder.Services.AddScoped<StockService>();
builder.Services.AddScoped<KiadasService>();
builder.Services.AddScoped<PriceReportService>();
builder.Services.AddScoped<AtadasService>();
builder.Services.AddScoped<ForgalomService>();

//publish
builder.WebHost.UseUrls("http://0.0.0.0:5115", "https://0.0.0.0:7123");

var app = builder.Build();


if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}
//commented out after publishing 
//app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseAntiforgery();

app.UseAuthentication();
app.UseAuthorization();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();



app.Run();
