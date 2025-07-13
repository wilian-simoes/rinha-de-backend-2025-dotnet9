# Estágio de build
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build-env
WORKDIR /App

# Copiar e restaurar
COPY *.csproj ./
RUN dotnet restore

# Copiar o restante e publicar
COPY . ./
RUN dotnet publish -c Release -o out

# Estágio final de runtime
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /App
COPY --from=build-env /App/out .
ENTRYPOINT ["dotnet", "rinha-de-backend-2025-dotnet9.dll"]