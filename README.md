# 🐔 Rinha de Backend 2025 - .NET 9

Este repositório contém a minha participação na **Rinha de Backend 2025**, implementada em **.NET 9**.

## 🚀 Tecnologias Utilizadas

- **Linguagem:** C# / .NET 9
- **Armazenamento/Fila:** Redis
- **Balanceador de carga:** Nginx
- **Orquestração:** Docker, Docker Compose
- **Teste de carga:** K6

## Como rodar ##

- Clone o repositório oficial da rinha-backend-2025: https://github.com/zanfranceschi/rinha-de-backend-2025
- Dentro da pasta "payment-processor" rode:
    ```sh
    "docker-compose up --build"
    ```
- Na pasta raiz do projeto em .NET /rinha-de-backend-2025-dotnet9 rode:
    ```sh
    "docker-compose up --build"
    ```
- Após isso as aplicações estão prontas para realizações dos testes com o k6.

## Sobre a solução ##

Back-end desenvolvido em .NET 9 com Minimal APIs, Redis para fila e persistência de dados, Nginx como balanceador de carga e testes de carga realizados com k6.

## Repositório ##
https://github.com/wilian-simoes/rinha-de-backend-2025-dotnet9