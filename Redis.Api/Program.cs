using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Redis.Api;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHostedService<RedisStreamReaderBackgroundWorker>();

builder.Services.AddSingleton(serviceProvider =>
{
    var configuration = serviceProvider.GetRequiredService<IConfiguration>();
    var multiplexer = ConnectionMultiplexer.Connect(configuration["RedisConnection"]!);

    if (multiplexer.IsConnected)
        return multiplexer.GetDatabase(0);

    throw new Exception("Could not connect to Redis instance");
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

var group = app.MapGroup("/redis");

#region String DataType

group.MapPost("/set-string", async (RedisStringKeyValueModel model, IDatabase redisDatabase) =>
{
    var isValueSet = await redisDatabase.StringSetAsync(model.Key, model.Value, TimeSpan.FromSeconds(45), flags: CommandFlags.None);

    return isValueSet
        ? Results.Ok()
        : Results.Problem(new ProblemDetails()
        {
            Detail = "Could not set the Key/Value in redis",
            Status = StatusCodes.Status500InternalServerError,
            Title = "Redis Problem"
        });
});

group.MapGet("/get-string", async (string key, IDatabase redisDatabase) =>
{
    if (!await redisDatabase.KeyExistsAsync(key))
        return Results.NotFound(new { ErrorMessage = "Specified Key not found" });

    return Results.Ok((await redisDatabase.StringGetAsync(key)).ToString());
});

group.MapGet("/get-string-update-ttl", async (string key, IDatabase redisDatabase) =>
{
    if (!await redisDatabase.KeyExistsAsync(key))
        return Results.NotFound(new { ErrorMessage = "Specified Key not found" });


    await redisDatabase.KeyExpireAsync(key, TimeSpan.FromSeconds(50), CommandFlags.FireAndForget);

    return Results.Ok((await redisDatabase.StringGetAsync(key)).ToString());
});


#endregion

#region List DataType

group.MapPost("/set-user-actions-as-List", async (int userId, UserActionsModel model, IDatabase redisDatabase) =>
{
    var key = $"User_{userId}";

    await redisDatabase.ListRightPushAsync(key, JsonSerializer.Serialize(model), When.Always,
        CommandFlags.FireAndForget);
    
    return Results.Ok();
});


group.MapGet("/get-user-actions-as-List", async (int userId, IDatabase redisDatabase) =>
{
    var result = new List<UserActionsModel?>();

    var key = $"User_{userId}";

    var listLength = await redisDatabase.ListLengthAsync(key);

    for (int i = 0; i < listLength; i++)
    {

        var listItem = await redisDatabase.ListGetByIndexAsync(key, i);

        if(listItem.HasValue)
            result.Add(JsonSerializer.Deserialize<UserActionsModel>(listItem.ToString()));
    }

    return Results.Ok(result);
});

#endregion

#region Set DataType

group.MapPost("/set-user-actions-as-Set", async (int userId, UserActionsModel model, IDatabase redisDatabase) =>
{
    var key = $"User_{userId}_set";

    await redisDatabase.SetAddAsync(key, JsonSerializer.Serialize(model), CommandFlags.FireAndForget);

    return Results.Ok();
});

group.MapGet("/get-user-actions-as-set", async (int userId, IDatabase redisDatabase) =>
{
    var result = new List<UserActionsModel?>();

    var key = $"User_{userId}_set";

    var cachedUserActions = await redisDatabase.SetMembersAsync(key);

    foreach (var cachedUserAction in cachedUserActions)
    {
        if(cachedUserAction.HasValue)
            result.Add(JsonSerializer.Deserialize<UserActionsModel>(cachedUserAction.ToString()));
    }

    return Results.Ok(result);
});

#endregion

#region ZSet DataType

group.MapPost("/set-user-actions-as-ZSet", async (int userId,int priority, UserActionsModel model, IDatabase redisDatabase) =>
{
    var key = $"User_{userId}_Zset";

    await redisDatabase.SortedSetAddAsync(key, JsonSerializer.Serialize(model),priority, CommandFlags.FireAndForget);

    return Results.Ok();
});

group.MapGet("/get-user-actions-as-ZSet", async (int userId,int minimumPriority,int maximumPriority, IDatabase redisDatabase) =>
{
    var result = new List<UserActionsModel?>();

    var key = $"User_{userId}_Zset";

    var cachedUserActions = await redisDatabase.SortedSetRangeByScoreAsync(key,minimumPriority,maximumPriority);

    foreach (var cachedUserAction in cachedUserActions)
    {
        if (cachedUserAction.HasValue)
            result.Add(JsonSerializer.Deserialize<UserActionsModel>(cachedUserAction.ToString()));
    }

    return Results.Ok(result);
});

#endregion


#region Hash DataType

group.MapPost("/CacheUserInfo", async (UserInfoViewModel model, int userId, IDatabase redisDatabase) =>
{
    var key = $"User_{userId}_Hash";

    await redisDatabase.HashSetAsync(key, nameof(UserInfoViewModel.UserName), model.UserName, When.Always, CommandFlags.FireAndForget);
    await redisDatabase.HashSetAsync(key, nameof(UserInfoViewModel.FamilyName), model.FamilyName, When.Always, CommandFlags.FireAndForget);
    await redisDatabase.HashSetAsync(key, nameof(UserInfoViewModel.Name), model.Name, When.Always, CommandFlags.FireAndForget);
    return Results.Ok();
});

group.MapGet("/GetUsernameFromCache", async ( int userId, IDatabase redisDatabase) =>
{
    var key = $"User_{userId}_Hash";

    if (!await redisDatabase.HashExistsAsync(key, nameof(UserInfoViewModel.UserName)))
        return Results.NotFound("Specified user has no cached username");

    var cachedUsername = await redisDatabase.HashGetAsync(key, nameof(UserInfoViewModel.UserName));

    return Results.Ok(new {Username=cachedUsername.ToString()});
});

group.MapGet("/GetUserInfo", async (int userId, IDatabase redisDatabase) =>
{
    var key = $"User_{userId}_Hash";

    var username = string.Empty;
    var name = string.Empty;
    var familyName = string.Empty;

    var cachedUserInfo = await redisDatabase.HashGetAllAsync(key);

    foreach (var hashEntry in cachedUserInfo)
    {
        switch (hashEntry.Name.ToString())
        {
            case nameof(UserInfoViewModel.Name):
                name = hashEntry.Value.ToString();
                break;
            case nameof(UserInfoViewModel.FamilyName):
                familyName = hashEntry.Value.ToString();
                break;
            case nameof(UserInfoViewModel.UserName):
                username = hashEntry.Value.ToString();
                break;
        }
    }

    return Results.Ok(new UserInfoViewModel(username,name,familyName));
});

#endregion


#region Streaming

app.MapPost("/SimulateStreaming", async (IDatabase redisDatabase) =>
{
    var key = "Streaming_Actions";

    var streamingInfo = Enumerable.Range(1, 1000)
        .Select(n => new UserActionsModel($"Action{n}", $"Action number {n} is listed"))
        .Select(a => new NameValueEntry(a.ActionName, a.ActionDescription)).ToArray();

    var streamId = await redisDatabase.StreamAddAsync(key, streamingInfo);

    return Results.Ok(streamId.ToString());
});

#endregion
app.UseHttpsRedirection();


app.Run();


public record RedisStringKeyValueModel(string Key, string Value);

public record UserActionsModel(string ActionName,string ActionDescription);

public record UserInfoViewModel(string UserName, string Name, string FamilyName);

