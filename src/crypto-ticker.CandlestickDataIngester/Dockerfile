FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["src/crypto-ticker.CandleService/crypto-ticker.CandleService.csproj", "src/crypto-ticker.CandleService/"]
RUN dotnet restore "src/crypto-ticker.CandleService/crypto-ticker.CandleService.csproj"
COPY . .
WORKDIR "/src/src/crypto-ticker.CandleService"
RUN dotnet build "./crypto-ticker.CandleService.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./crypto-ticker.CandleService.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "crypto-ticker.CandleService.dll"]
