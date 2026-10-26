using Microsoft.AspNetCore.SignalR;

namespace CandlestickApi;

public class CandlestickHub:Hub
{
    public async Task SendMessage(Candlestick candlestick)
    {
        await Clients.All.SendAsync("ReceiveMessage", candlestick);
    }
    
    public Task JoinRoom(string roomName)
    {
        return Groups.AddToGroupAsync(Context.ConnectionId, roomName);
    }
    
    public Task LeaveRoom(string roomName)
    {
        return Groups.RemoveFromGroupAsync(Context.ConnectionId, roomName);
    }
}