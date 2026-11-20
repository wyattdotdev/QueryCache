using System.Collections.Concurrent;

namespace QueryCache;

public delegate Task<T> QueryFunctionAsync<T>();
public delegate T QuerySetter<T, T2>(T? old, T2 data);

public delegate Task MutateFunctionAsync();              // Void return
public delegate Task<T> MutateFunctionAsync<T>();        // Returns item
public delegate Task<T> MutateSuccessAsync<T>(T result); // Success mutates
public delegate Task MutateSuccessAsync();               // Success does not mutate
public delegate Task MutateError(Exception exception);


public interface IQueryService
{
    public QueryClient<T> GetQueryClient<T>(string key, QueryClientOptions<T> queryFunction);
    public void InvalidateQuery(string key);
}

public class QueryClientOptions<T>(QueryFunctionAsync<T> queryFunctionAsync)
{
    public QueryFunctionAsync<T> QueryFunctionAsync { get; set; } = queryFunctionAsync;
    public TimeSpan? FreshDuration { get; set; } = null;
}

public class MutationClientOptions<T>(MutateFunctionAsync<T> mutateAsync, MutateSuccessAsync<T> successAsync)
{
    public MutateFunctionAsync<T> MutateFunctionAsync  { get; set; } = mutateAsync;
    public MutateSuccessAsync<T>  SuccessAsync         { get; set; } = successAsync;
}

public class MutationClientOptions(MutateFunctionAsync mutate, MutateSuccessAsync success)
{
    public MutateFunctionAsync MutateFunctionAsync { get; set; } = mutate;
    public MutateSuccessAsync  SuccessAsync        { get; set; } = success;
}

public class QueryService : IQueryService
{
    private readonly ConcurrentDictionary<string, QueryClientBase> _clients = new();

    public QueryClient<T> GetQueryClient<T>(string key, QueryClientOptions<T> options)
    {
        if (_clients.TryGetValue(key, out var queryClient))
        {
            if (queryClient.Type != typeof(T)) throw new Exception($"key {key} already defined as type {typeof(T)}");
            return (QueryClient<T>)queryClient;
        }

        var client = new QueryClient<T>(key, options);
        _clients[key] = client;

        return client;
    }

    public void InvalidateQuery(string key)
    {
        if (!_clients.TryGetValue(key, out var queryClient)) throw new Exception($"key {key} is not set");

        var _ = queryClient.Invalidate();
    }

    // TODO: Autorefresh 
}

public abstract class QueryClientBase(string key, Type type, TimeSpan? fresh)
{
    public string Key { get; internal set; } = key;
    internal Type Type { get; set; } = type;
    public TimeSpan? FreshDuration { get; set; } = fresh;
    internal DateTime? FreshTime { get; set; } = fresh is not null ? DateTime.Now.Add((TimeSpan) fresh) : null;
    public QueryState State { get; internal set; } = QueryState.IsLoading;

    protected internal abstract Task QueryInternalAsync();
    public abstract Task Invalidate();
}

public abstract class MutationClientBase(string key, Type type)
{
    public string Key { get; internal set; } = key;
    internal Type Type { get; set; } = type;
    public QueryState State { get; internal set; } = QueryState.IsInactive;
}

public class MutationClient<T>(string key, MutationClientOptions<T> options) : MutationClientBase(key, typeof(T))
{
}

public class MutationClient(string key, MutationClientOptions options) : MutationClientBase(key, typeof(T))
{
}

public class QueryClient<T>(string key, QueryClientOptions<T> options) : QueryClientBase(key, typeof(T), options.FreshDuration)
{
    public T? Data { get; internal set; } = default;
    public Exception? Error { get; set; } = default;
    public Type? ErrorType { get; set; } = default;
    private Action? OnChanged { get; set; }
    private int Subscribers { get; set; } = 0;
    public bool IsFetching { get; set; } = false;
    private readonly QueryFunctionAsync<T> QueryFunc = options.QueryFunctionAsync;

    public void SubscribeQuery(Action onChanged)
    {
        OnChanged += onChanged;
        Subscribers++;
        if (State == QueryState.IsLoading)
        {
            _ = QueryInternalAsync();
        }
    }

    public void UnsubscribeQuery(Action onChanged)
    {
        OnChanged -= onChanged;
        Subscribers = Math.Max(Subscribers - 1, 0);
    }

    protected internal override async Task QueryInternalAsync()
    {
        if (IsFetching) return;

        try
        {
            IsFetching = true;
            RaiseChanged();
            Data = await QueryFunc();
            State = QueryState.IsDone;
        }
        catch (Exception ex)
        {
            State = QueryState.IsError;
            Error = ex;
            ErrorType = ex.GetType();
        }
        finally
        {
            IsFetching = false;
            RaiseChanged();
        }
    }

    // Limitation cannot put in base because it does not know what T is 
    public void SetQueryData<T1>(T1 data, QuerySetter<T?, T1> setter)
    {
        Data = setter.Invoke(Data, data);
        RaiseChanged();
    }

    private void RaiseChanged() => OnChanged?.Invoke();

    public override async Task Invalidate()
    {
        _ = QueryInternalAsync();
    }
}

public enum QueryState
{
    IsInactive,
    IsLoading, // Empty data
    IsError,   // Has an error
    IsDone,    
}
