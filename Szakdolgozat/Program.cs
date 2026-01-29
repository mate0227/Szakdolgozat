using Szakdolgozat.Components;
using Szakdolgozat.Services;
using FirebirdSql.Data.FirebirdClient;
using Microsoft.AspNetCore.Authentication.Cookies;

var builder = WebApplication.CreateBuilder(args);

// =======================
// SERVICES
// =======================

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// Firebird Connection
builder.Services.AddScoped<FbConnection>(sp =>
{
    var cs = builder.Configuration.GetConnectionString("Firebird");
    return new FbConnection(cs);
});

// Auth system
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







var app = builder.Build();

// =======================
// MIDDLEWARE
// =======================

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseAntiforgery();

app.UseAuthentication();
app.UseAuthorization();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();



app.Run();
