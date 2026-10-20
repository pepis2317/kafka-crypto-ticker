FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["src/crypto-ticker.PriceService/crypto-ticker.PriceService.csproj", "src/crypto-ticker.PriceService/"]
RUN dotnet restore "src/crypto-ticker.PriceService/crypto-ticker.PriceService.csproj"
COPY . .
WORKDIR "/src/src/crypto-ticker.PriceService"
RUN dotnet build "./crypto-ticker.PriceService.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./crypto-ticker.PriceService.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "crypto-ticker.PriceService.dll"]
