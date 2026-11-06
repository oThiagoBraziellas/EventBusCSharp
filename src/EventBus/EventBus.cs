using System.Threading.Channels;

namespace EventBus;

/// <summary>What to do when a subscriber's queue is full.</summary>
public enum OverflowMode
{
    /// <summary>Publishers wait asynchronously until there is room (back-pressure).</summary>
    Wait,
    /// <summary>The oldest queued event of that subscriber is discarded.</summary>
    DropOldest,
    /// <summary>The new event is discarded for that subscriber.</summary>
    DropNewest
}

public sealed record EventBusOptions
{
    /// <summary>Per-subscriber queue size.</summary>
    public int QueueCapacity { get; init; } = 1024;
    public OverflowMode Overflow { get; init; } = OverflowMode.Wait;
    /// <summary>Invoked when a handler throws. Exceptions thrown by this callback are swallowed.</summary>
    public Action<HandlerError>? OnError { get; init; }
}

/// <summary>Describes a failure inside a handler. The failure never reaches other subscribers or the publisher.</summary>
public sealed record HandlerError(object Event, Exception Exception, Guid SubscriptionId, Type SubscribedType);

/// <summary>
/// In-process asynchronous publish/subscribe bus.
/// Every subscription owns a bounded <see cref="Channel{T}"/> and a dedicated worker, which gives:
/// per-subscriber ordering, isolation (a slow or failing handler affects nobody else) and back-pressure.
/// Handlers registered for a base type or interface also receive derived events.
/// </summary>
public sealed class EventBus : IAsyncDisposable
{
    private readonly EventBusOptions _options;
    private readonly object _gate = new();
    private readonly CancellationTokenSource _handlerCts = new();
    private Subscription[] _subscriptions = Array.Empty<Subscription>(); // copy-on-write: publish path is lock-free
    private bool _closed;
    private Task? _shutdown;

    public EventBus(EventBusOptions? options = null)
    {
        _options = options ?? new EventBusOptions();
        if (_options.QueueCapacity <= 0) throw new ArgumentOutOfRangeException(nameof(options), "QueueCapacity must be positive.");
    }

    public int SubscriptionCount => Volatile.Read(ref _subscriptions).Length;

    /// <summary>Registers an async handler. Dispose the result to unsubscribe (queued events are still delivered).</summary>
    public IDisposable Subscribe<TEvent>(Func<TEvent, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var sub = new Subscription(typeof(TEvent), (e, ct) => handler((TEvent)e, ct), this);
        lock (_gate)
        {
            if (_closed) throw new ObjectDisposedException(nameof(EventBus));
            _subscriptions = _subscriptions.Append(sub).ToArray();
        }
        sub.Start();
        return sub;
    }

    public IDisposable Subscribe<TEvent>(Action<TEvent> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return Subscribe<TEvent>((e, _) => { handler(e); return ValueTask.CompletedTask; });
    }

    /// <summary>
    /// Enqueues the event for every matching subscription. Completes when the event is queued, not when it is handled.
    /// Returns the number of subscriptions that accepted it.
    /// </summary>
    public async ValueTask<int> PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@event);
        if (Volatile.Read(ref _closed)) throw new ObjectDisposedException(nameof(EventBus));
        var runtimeType = @event.GetType();
        var accepted = 0;
        foreach (var sub in Volatile.Read(ref _subscriptions))
        {
            if (!sub.EventType.IsAssignableFrom(runtimeType)) continue;
            if (await sub.EnqueueAsync(@event, _options.Overflow, cancellationToken).ConfigureAwait(false)) accepted++;
        }
        return accepted;
    }

    /// <summary>
    /// Stops accepting events, lets queued events drain and waits for the workers.
    /// If <paramref name="drainToken"/> is cancelled, handlers receive a cancelled token so shutdown can be forced.
    /// </summary>
    public Task ShutdownAsync(CancellationToken drainToken = default)
    {
        lock (_gate)
        {
            if (_shutdown is not null) return _shutdown;
            _closed = true;
            _shutdown = DrainAsync(_subscriptions, drainToken);
            return _shutdown;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await ShutdownAsync().ConfigureAwait(false);
        _handlerCts.Dispose();
    }

    private async Task DrainAsync(Subscription[] subs, CancellationToken drainToken)
    {
        using var registration = drainToken.Register(() => _handlerCts.Cancel());
        foreach (var s in subs) s.Complete();
        await Task.WhenAll(subs.Select(s => s.Worker)).ConfigureAwait(false);
    }

    private void Remove(Subscription sub)
    {
        lock (_gate) _subscriptions = _subscriptions.Where(s => s != sub).ToArray();
    }

    private void ReportError(Subscription sub, object @event, Exception ex)
    {
        try { _options.OnError?.Invoke(new HandlerError(@event, ex, sub.Id, sub.EventType)); }
        catch { /* the error callback must never break the worker */ }
    }

    private sealed class Subscription : IDisposable
    {
        private readonly Func<object, CancellationToken, ValueTask> _handler;
        private readonly EventBus _bus;
        private readonly Channel<object> _channel;

        public Subscription(Type eventType, Func<object, CancellationToken, ValueTask> handler, EventBus bus)
        {
            EventType = eventType;
            _handler = handler;
            _bus = bus;
            _channel = Channel.CreateBounded<object>(new BoundedChannelOptions(bus._options.QueueCapacity)
            {
                SingleReader = true,
                FullMode = bus._options.Overflow switch
                {
                    OverflowMode.DropOldest => BoundedChannelFullMode.DropOldest,
                    OverflowMode.DropNewest => BoundedChannelFullMode.DropWrite,
                    _ => BoundedChannelFullMode.Wait
                }
            });
        }

        public Guid Id { get; } = Guid.NewGuid();
        public Type EventType { get; }
        public Task Worker { get; private set; } = Task.CompletedTask;

        public void Start() => Worker = Task.Run(RunAsync);

        public async ValueTask<bool> EnqueueAsync(object e, OverflowMode mode, CancellationToken ct)
        {
            try
            {
                if (mode == OverflowMode.Wait) { await _channel.Writer.WriteAsync(e, ct).ConfigureAwait(false); return true; }
                return _channel.Writer.TryWrite(e);
            }
            catch (ChannelClosedException) { return false; } // unsubscribed concurrently
        }

        public void Complete() => _channel.Writer.TryComplete();

        public void Dispose()
        {
            _bus.Remove(this);
            Complete();
        }

        private async Task RunAsync()
        {
            await foreach (var e in _channel.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                try
                {
                    await _handler(e, _bus._handlerCts.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _bus.ReportError(this, e, ex);
                }
            }
        }
    }
}
