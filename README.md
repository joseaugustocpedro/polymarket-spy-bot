# Polymarket Spy Bot

Bot em Python para monitoramento de atividade pública na Polymarket, com alertas via Telegram e persistência em PostgreSQL.

O projeto foi desenvolvido com foco em **monitoramento assíncrono, consumo reduzido de recursos, tratamento de falhas de rede e parametrização por variáveis de ambiente**.

> Projeto voltado a monitoramento e análise de dados públicos. Não constitui aconselhamento financeiro e não garante resultados.

## Principais recursos

- monitoramento de carteiras públicas;
- alertas por faixas de valor;
- persistência em PostgreSQL;
- comandos e notificações via Telegram;
- retries HTTP para falhas temporárias;
- limites específicos para execução em ambientes gratuitos;
- configuração de timezone;
- parâmetros de execução via variáveis de ambiente;
- serviço HTTP com Flask para health check/hosting.

## Tecnologias

- Python
- Flask
- PostgreSQL / psycopg2
- python-telegram-bot
- Requests
- APIs públicas da Polymarket

## Estrutura atual

```text
polymarket-spy-bot/
├── bot.py
├── requirements.txt
└── README.md
```

## Configuração

Crie as variáveis de ambiente necessárias antes de executar o projeto.

Principais variáveis:

```text
BOT_TOKEN
DATABASE_URL
ALERT_CHAT_ID
FREE_TIER_MODE
MONITORING_ENABLED
BOT_TIMEZONE
```

Outros limites e parâmetros possuem valores padrão no próprio código e podem ser sobrescritos por ambiente.

## Instalação

```bash
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

Depois configure as variáveis de ambiente e execute:

```bash
python bot.py
```

## Decisões de engenharia

O código inclui:

- funções auxiliares para leitura segura de configurações;
- limites de polling e orçamento de chamadas para reduzir consumo de API;
- retentativas HTTP com backoff;
- validação de configurações obrigatórias na inicialização;
- separação entre configuração, comunicação HTTP, persistência e comandos do bot.

## Melhorias futuras

- modularizar `bot.py` em pacotes menores;
- adicionar testes automatizados;
- criar `.env.example` documentado;
- adicionar CI para lint e testes;
- adicionar métricas e observabilidade.

## Autor

**José Augusto Cevidanes Pedro**  
Estudante de Engenharia de Controle e Automação — IFSP
