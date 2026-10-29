using crypto_ticker.Contracts;
using Microsoft.AspNetCore.SignalR;

namespace CandlestickApi;

public class IntervalAggregateHelper
{
    private readonly Dictionary<string, Candlestick> _candleStates = new();
    private readonly IHubContext<CandlestickHub> _hub;
    private readonly int _minuteInterval;

    public IntervalAggregateHelper(IHubContext<CandlestickHub> hub, int minuteInterval)
    {
        _hub = hub;
        _minuteInterval = minuteInterval;
    }

    public async Task Handle(AggregateTradeStream trade)
    {
        var tradeMs = trade.TradeTime.ToUnixTimeMilliseconds();
        var startMs = tradeMs - (tradeMs % (60000 * _minuteInterval));
        var closeMs = startMs + 60000 * _minuteInterval;
        
        if(!_candleStates.TryGetValue(trade.Symbol, out var candle))
        {
            _candleStates[trade.Symbol] = new Candlestick
            {
                Symbol = trade.Symbol,
                StartTime = DateTimeOffset.FromUnixTimeMilliseconds(startMs),
                CloseTime = DateTimeOffset.FromUnixTimeMilliseconds(closeMs),
                Open = trade.Price,
                High = trade.Price,
                Low = trade.Price,
                Close = trade.Price,
                Volume = trade.Quantity,
                Trades = 1,
                IsClosed = DateTimeOffset.FromUnixTimeMilliseconds(closeMs) < DateTimeOffset.UtcNow
            };
            
            // Send message about initialized candle
            await _hub.Clients.Group(trade.Symbol + $"_{_minuteInterval}m")
                .SendAsync("Candlestick", _candleStates[trade.Symbol]);
            
            return;
        }
        
        // Skip late trades
        if (startMs < candle.StartTime.ToUnixTimeMilliseconds()) return;
        
        // startMs belongs to a newer interval
        if(startMs >= candle.CloseTime.ToUnixTimeMilliseconds())
        {
            // Send message about the closure the current closed candle
            candle.IsClosed = true;
            await _hub.Clients.Group(candle.Symbol + $"_{_minuteInterval}m")
                .SendAsync("Candlestick", candle);
            
            // Update current candle to new interval
            candle.StartTime = DateTimeOffset.FromUnixTimeMilliseconds(startMs);
            candle.CloseTime = DateTimeOffset.FromUnixTimeMilliseconds(closeMs);
            candle.Open = trade.Price;
            candle.High = trade.Price;
            candle.Low = trade.Price;
            candle.Close = trade.Price;
            candle.Volume = trade.Quantity;
            candle.Trades = 1;
            candle.IsClosed = false;
            
            await _hub.Clients.Group(candle.Symbol + $"_{_minuteInterval}m")
                .SendAsync("Candlestick", candle);
            
            return;
        }
        
        // If neither, just update the candle
        candle.High = Math.Max(candle.High, trade.Price);
        candle.Low  = Math.Min(candle.Low, trade.Price);
        candle.Close = trade.Price;
        candle.Volume += trade.Quantity;
        candle.Trades++;
        
        await _hub.Clients.Group(candle.Symbol + $"_{_minuteInterval}m")
            .SendAsync("Candlestick", candle);
    }
}