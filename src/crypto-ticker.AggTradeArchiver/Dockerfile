FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["src/crypto-ticker.AggTradeArchiver/crypto-ticker.AggTradeArchiver.csproj", "src/crypto-ticker.AggTradeArchiver/"]
RUN dotnet restore "src/crypto-ticker.AggTradeArchiver/crypto-ticker.AggTradeArchiver.csproj"
COPY . .
WORKDIR "/src/src/crypto-ticker.AggTradeArchiver"
RUN dotnet build "./crypto-ticker.AggTradeArchiver.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./crypto-ticker.AggTradeArchiver.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "crypto-ticker.AggTradeArchiver.dll"]
