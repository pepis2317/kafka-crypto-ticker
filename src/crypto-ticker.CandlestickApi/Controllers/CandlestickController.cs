using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace CandlestickApi;

[ApiController]
[Route("api/v1/[controller]")]
public class CandlestickController:ControllerBase
{
    private readonly IMediator _mediator;

    public CandlestickController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpGet("get-weekly-candlesticks")]
    public async Task<IActionResult> GetWeeklyCandlesticks([FromQuery] GetWeeklyCandlestickRequest request)
    {
        var result = await _mediator.Send(request);
        return Ok(result);
    }
    
    [HttpGet("get-yearly-or-monthly-candlesticks")]
    public async Task<IActionResult> GetYearlyOrMonthlyCandlesticks([FromQuery] GetYearlyOrMonthlyCandlestickRequest request)
    {
        var result = await _mediator.Send(request);
        return Ok(result);
    }
}