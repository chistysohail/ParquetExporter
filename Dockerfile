FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# copy csproj and restore
COPY src/ParquetExporter/ParquetExporter.csproj ./ParquetExporter/
RUN dotnet restore ParquetExporter/ParquetExporter.csproj

# copy source
COPY src/ParquetExporter ./ParquetExporter

WORKDIR /src/ParquetExporter
RUN dotnet publish ParquetExporter.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
RUN mkdir -p /logs

COPY --from=build /app/publish ./

ENV DOTNET_ENVIRONMENT=Production

ENTRYPOINT ["dotnet", "ParquetExporter.dll"]
