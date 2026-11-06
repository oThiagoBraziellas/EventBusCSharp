namespace EventBus.Tests;

public record OrderPlaced(int Id);
public record OrderShipped(int Id);
public interface IAudited { }
public record AuditedEvent(string Name) : IAudited;

public class EventBusTests
{
    [Fact]
    public async Task DeliversToMatchingTypedHandlersOnly()
    {
        var placed = new List<int>();
        var shipped = new List<int>();
        await using (var bus = new EventBus())
        {
            bus.Subscribe<OrderPlaced>(e => placed.Add(e.Id));
            bus.Subscribe<OrderShipped>(e => shipped.Add(e.Id));
            await bus.PublishAsync(new OrderPlaced(1));
            await bus.PublishAsync(new OrderShipped(2));
        }
        Assert.Equal(new[] { 1 }, placed);
        Assert.Equal(new[] { 2 }, shipped);
    }

    [Fact]
    public async Task PreservesOrderPerSubscriber()
    {
        var seen = new List<int>();
        await using (var bus = new EventBus())
        {
            bus.Subscribe<OrderPlaced>(async (e, _) => { await Task.Yield(); seen.Add(e.Id); });
            for (var i = 0; i < 500; i++) await bus.PublishAsync(new OrderPlaced(i));
        }
        Assert.Equal(Enumerable.Range(0, 500), seen);
    }

    [Fact]
    public async Task FailingHandler_IsIsolated_AndReported()
    {
        var errors = new List<HandlerError>();
        var good = new List<int>();
        await using (var bus = new EventBus(new EventBusOptions { OnError = errors.Add }))
        {
            bus.Subscribe<OrderPlaced>(_ => throw new InvalidOperationException("boom"));
            bus.Subscribe<OrderPlaced>(e => good.Add(e.Id));
            await bus.PublishAsync(new OrderPlaced(1));
            await bus.PublishAsync(new OrderPlaced(2));
        }
        Assert.Equal(new[] { 1, 2 }, good);
        Assert.Equal(2, errors.Count);
        Assert.All(errors, e => Assert.IsType<InvalidOperationException>(e.Exception));
        Assert.Equal(new OrderPlaced(1), errors[0].Event);
    }

    [Fact]
    public async Task ThrowingErrorCallback_DoesNotKillWorker()
    {
        var seen = new List<int>();
        await using (var bus = new EventBus(new EventBusOptions { OnError = _ => throw new Exception("callback bug") }))
        {
            bus.Subscribe<OrderPlaced>(e => { seen.Add(e.Id); if (e.Id == 1) throw new Exception("x"); });
            await bus.PublishAsync(new OrderPlaced(1));
            await bus.PublishAsync(new OrderPlaced(2));
        }
        Assert.Equal(new[] { 1, 2 }, seen);
    }

    [Fact]
    public async Task SlowSubscriber_DoesNotBlockFastOne()
    {
        var release = new TaskCompletionSource();
        var fastGot = new TaskCompletionSource();
        await using var bus = new EventBus();
        bus.Subscribe<OrderPlaced>(async (_, _) => await release.Task);
        bus.Subscribe<OrderPlaced>(_ => fastGot.TrySetResult());
        await bus.PublishAsync(new OrderPlaced(1));
        await fastGot.Task.WaitAsync(TimeSpan.FromSeconds(5));
        release.SetResult();
    }

    [Fact]
    public async Task BaseTypeAndInterfaceHandlers_ReceiveDerivedEvents()
    {
        var viaInterface = new List<string>();
        var viaObject = 0;
        await using (var bus = new EventBus())
        {
            bus.Subscribe<IAudited>(e => viaInterface.Add(e.ToString()!));
            bus.Subscribe<object>(_ => Interlocked.Increment(ref viaObject));
            await bus.PublishAsync(new AuditedEvent("a"));
            await bus.PublishAsync(new OrderPlaced(1));
        }
        Assert.Single(viaInterface);
        Assert.Equal(2, viaObject);
    }

    [Fact]
    public async Task Unsubscribe_StopsFutureDelivery_ButDrainsQueued()
    {
        var count = 0;
        await using var bus = new EventBus();
        var sub = bus.Subscribe<OrderPlaced>(_ => Interlocked.Increment(ref count));
        await bus.PublishAsync(new OrderPlaced(1));
        sub.Dispose();
        var accepted = await bus.PublishAsync(new OrderPlaced(2));
        Assert.Equal(0, accepted);
        Assert.Equal(0, bus.SubscriptionCount);
        await bus.ShutdownAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Backpressure_PublisherWaitsWhenQueueFull()
    {
        var gate = new TaskCompletionSource();
        await using var bus = new EventBus(new EventBusOptions { QueueCapacity = 1 });
        bus.Subscribe<OrderPlaced>(async (_, _) => await gate.Task);
        await bus.PublishAsync(new OrderPlaced(1)); // picked up by the worker, which then blocks
        await bus.PublishAsync(new OrderPlaced(2)); // fills the queue (retries until worker took #1)
        var third = bus.PublishAsync(new OrderPlaced(3)).AsTask();
        await Task.Delay(100);
        Assert.False(third.IsCompleted);
        gate.SetResult();
        await third.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task DropNewest_DiscardsWhenFull()
    {
        var gate = new TaskCompletionSource();
        var seen = new List<int>();
        var bus = new EventBus(new EventBusOptions { QueueCapacity = 2, Overflow = OverflowMode.DropNewest });
        bus.Subscribe<OrderPlaced>(async (e, _) => { seen.Add(e.Id); await gate.Task; });
        await bus.PublishAsync(new OrderPlaced(0));
        await Task.Delay(100); // worker now holds #0
        for (var i = 1; i <= 5; i++) await bus.PublishAsync(new OrderPlaced(i));
        gate.SetResult();
        await bus.DisposeAsync();
        Assert.Equal(new[] { 0, 1, 2 }, seen);
    }

    [Fact]
    public async Task Publish_AfterShutdown_Throws()
    {
        var bus = new EventBus();
        await bus.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await bus.PublishAsync(new OrderPlaced(1)));
        Assert.Throws<ObjectDisposedException>(() => bus.Subscribe<OrderPlaced>(_ => { }));
    }

    [Fact]
    public async Task ForcedShutdown_CancelsHandlers()
    {
        var started = new TaskCompletionSource();
        var cancelled = new TaskCompletionSource<bool>();
        var bus = new EventBus();
        bus.Subscribe<OrderPlaced>(async (_, ct) =>
        {
            started.SetResult();
            try { await Task.Delay(Timeout.Infinite, ct); }
            catch (OperationCanceledException) { cancelled.SetResult(true); }
        });
        await bus.PublishAsync(new OrderPlaced(1));
        await started.Task;
        using var cts = new CancellationTokenSource();
        var shutdown = bus.ShutdownAsync(cts.Token);
        cts.Cancel();
        await shutdown.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(await cancelled.Task);
    }

    [Fact]
    public async Task ConcurrentPublishers_AllEventsDelivered()
    {
        var count = 0;
        await using (var bus = new EventBus())
        {
            bus.Subscribe<OrderPlaced>(_ => Interlocked.Increment(ref count));
            await Task.WhenAll(Enumerable.Range(0, 10).Select(p => Task.Run(async () =>
            {
                for (var i = 0; i < 1000; i++) await bus.PublishAsync(new OrderPlaced(i));
            })));
        }
        Assert.Equal(10_000, count);
    }

    [Fact]
    public void InvalidCapacity_Throws() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new EventBus(new EventBusOptions { QueueCapacity = 0 }));
}
