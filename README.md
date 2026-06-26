# 📊 ProactiveDB – Agent

## 🧩 O que é?

O **ProactiveDB Agent** é um serviço Windows leve e open source que recolhe telemetria de instâncias SQL Server e envia esses dados de forma segura para a plataforma **ProactiveDB**, um serviço SaaS especializado em observabilidade e análise de desempenho de bases de dados.

O Agent foi desenhado para ser simples, eficiente e com baixo consumo de recursos, funcionando de forma semelhante a agentes como o Zabbix Agent — mas totalmente focado em SQL Server.

A partir da versão 1.0.5, um único Agent pode monitorizar **múltiplas instâncias SQL Server** em simultâneo (cada instância com a sua própria connection string e lista de tabelas), através da secção `Instances` em `appsettings.json`. A configuração legada de instância única continua a funcionar sem alterações.

---

## 🚀 Comece

### 1. Descarregue a última versão  
Aceda à secção **Releases** deste repositório e obtenha a versão mais recente.

### 2. Obtenha a sua Agent KEY  
Para que o Agent possa enviar telemetria para o ProactiveDB, é necessário um **Agent Token (KEY)**.  
Entre em contacto com a **Singular Data** para solicitar a sua chave de integração.

---

## 📜 Licença

Este projeto é distribuído sob os termos da **Apache License Version 2.0**, que permite usar, modificar e distribuir o software — inclusive para fins comerciais — desde que sejam mantidas as declarações de copyright, a licença e as notificações de isenção de responsabilidade.  
A licença inclui ainda uma concessão explícita de patente e limita a responsabilidade dos autores. Para mais detalhes, consulte o ficheiro `LICENSE`.
