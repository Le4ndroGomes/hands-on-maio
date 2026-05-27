# Hands-On: Lakeflow Design + IA + Apps

> 🏥 **Healthcare Lakehouse Demo** — Workshop técnico-executivo Databricks

<img src="./assets/databricks-academy.png" alt="Databricks Academy" height="55"/>

Este repositório contém o **Módulo 1** de um hands-on guiado de Lakehouse aplicado à saúde — setup inicial de schemas, cadastros base (hospitais + pacientes) e landing zone com dados sintéticos prontos para o **Lakeflow Designer**.

---

## 🎯 O que este notebook entrega

Ao final do Módulo 1 você terá no Unity Catalog:

```
healthcare_lakehouse
├── bronze
│   ├── hospitais       ← 140 hospitais BR realistas (Santa Casa, HC, Municipal...)
│   ├── pacientes       ← 1.500 pacientes (com PII fake mas formatado validamente)
│   └── landing_zone/   ← Volume com 2 datasets brutos:
│       ├── consultas/     (1.000 consultas com diagnóstico CID-10)
│       └── avaliacoes/    (500 avaliações textuais com sinergia tempo-sentimento)
├── silver              (vazio — será preenchido pelo Lakeflow Designer)
└── gold                (vazio — será preenchido pelo Lakeflow Designer)
```

### Características dos dados sintéticos

| Tabela | Volume | Highlights |
|--------|--------|-----------|
| **hospitais** | 140 registros × 12 colunas | 28 cidades BR, 18 especialidades, dados realistas (endereço, leitos, médicos, descrição NLP-ready) |
| **pacientes** | 1.500 registros × 12 colunas | Demografia (idade, sexo, estado civil) + PII LGPD (CPF, CNS, RG, e-mail, telefone). 70% < 35 anos no segundo lote (perfil mais jovem) |
| **consultas.csv** | 1.000 linhas × 12 colunas | 10% com `id_paciente` nulo (qualidade proposital), médico/CRM/modalidade/CID-10 alinhado à especialidade do hospital |
| **avaliacoes.csv** | 500 linhas | Templates BR realistas (positivos/negativos/neutros) **com viés baseado no tempo de espera** (> 30 min → 70% negativas) |

---

## 🚀 Como usar este repositório no Databricks

### Opção 1 — Clone via "Git folder" no workspace (recomendado)

1. No Databricks workspace, navegue até sua pasta de usuário (sidebar **Workspace → Users → seu_email**)
2. Clique no botão **"+"** no topo da pasta e selecione **"Git folder"**
3. Cole a URL do repositório:
   ```
   https://github.com/Databricks-BR/hands-on-maio.git
   ```
4. Clique em **"Create Git folder"**
5. Abra o notebook **`Healthcare_Lakehouse_Demo`** e clique em **"Run all"**

### Opção 2 — Clone via terminal (Databricks CLI)

```bash
databricks repos create \
  --url https://github.com/Databricks-BR/hands-on-maio.git \
  --provider gitHub
```

### Opção 3 — Download direto (sem Git)

1. Acesse [github.com/Databricks-BR/hands-on-maio](https://github.com/Databricks-BR/hands-on-maio)
2. Clique em **"Code → Download ZIP"**
3. Faça upload do `Healthcare_Lakehouse_Demo.py` no seu workspace via **Workspace → Import**

---

## ⚙️ Pré-requisitos

| Item | Requisito |
|------|-----------|
| **Workspace** | Databricks com Unity Catalog habilitado |
| **Compute** | SQL Warehouse Serverless **OU** Serverless Compute (qualquer um funciona) |
| **Permissões** | `CREATE CATALOG` no metastore (ou já ter o catálogo criado pelo admin) |
| **Runtime** | DBR 13+ (qualquer versão moderna) |

> 💡 **Não precisa de pip install nem Python**. O notebook é 100% SQL e executa direto em qualquer SQL Warehouse Serverless.

---

## 📋 Estrutura do repositório

```
hands-on-maio/
├── README.md                              # Este arquivo
├── Healthcare_Lakehouse_Demo.py           # Notebook principal (Databricks source format)
└── assets/
    └── databricks-academy.png             # Logo exibido no header do notebook
```

---

## 📚 Roadmap completo da demo

| Módulo | Onde acontece | Conteúdo |
|--------|--------------|----------|
| **1 — Setup (este notebook)** | Notebook SQL | Catálogo, schemas, cadastros base (hospitais + pacientes), landing zone com CSVs |
| 2 — Ingestão de consultas | **Lakeflow Designer** | Pipeline visual: CSV → Bronze → Silver com data quality |
| 3 — Ingestão de avaliações | **Lakeflow Designer** | Pipeline visual: CSV → Bronze |
| 4 — Enriquecimento (Visão 360) | **Lakeflow Designer** | Joins visuais entre fato e dimensões |
| 5 — IA Generativa / NLP | **Lakeflow Designer + AI Functions** | `ai_analyze_sentiment` em coluna calculada |
| 6 — Gold & Dashboards | **AI/BI Dashboards** | KPIs e visualizações executivas |
| 7 — Apps & Genie | **Databricks Apps + Genie Spaces** | App de patient experience + perguntas em linguagem natural |

---

## 💡 Por que essa abordagem híbrida?

| Notebook SQL (este) | Lakeflow Designer (próximos módulos) |
|--------------------|--------------------------------------|
| Setup governado (catálogo, schemas) | Pipelines visuais |
| Cadastros base estáveis | Fatos transacionais |
| SQL versionável | Self-service para analistas |
| Reprodutível em qualquer ambiente | Drag-and-drop, fácil de demonstrar |

> 🎯 **Mensagem-chave:** O Lakehouse não é só código — é também uma **plataforma visual**.
> O Lakeflow Designer permite que **engenheiros, analistas e até gestores** construam pipelines
> sem precisar saber Spark, mantendo toda a governança do Unity Catalog.

---

## 🆘 Troubleshooting

**Erro: "Unsupported cell during execution. SQL warehouses only support executing SQL cells."**

> O notebook é 100% SQL e funciona em SQL Warehouse. Se você ver esse erro, é porque há uma célula Python residual. Use a versão mais recente do notebook neste repositório.

**Erro: "Metastore storage root URL does not exist"**

> Seu workspace precisa ter **Default Storage** habilitado no Unity Catalog. Peça ao admin para configurar, ou use um catálogo já existente substituindo `healthcare_lakehouse` pelo nome do seu catálogo nas células de setup.

**Erro: "table or view not found"**

> Execute as células **em ordem**. A Parte 2 (avaliações) depende dos dados da Parte 2 (consultas), que depende da tabela `hospitais` da Parte 1.

---

## 🤝 Contribuindo

Sugestões, correções e melhorias são bem-vindas — abra uma **Issue** ou **Pull Request**.

---

## 📄 Licença

Material de treinamento Databricks Brasil. Uso livre para fins educacionais.

---

**Databricks Field Engineering — Healthcare**
