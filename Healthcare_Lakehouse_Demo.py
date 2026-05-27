# Databricks notebook source
# MAGIC %md
# MAGIC <img src="./assets/databricks-academy.png" alt="Databricks Academy" height="55"/>
# MAGIC
# MAGIC # Hands-On: Lakeflow Design + IA + Apps
# MAGIC
# MAGIC ### Módulo 1 — Setup Inicial: Schemas & Cadastros Base (Bronze)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo deste notebook
# MAGIC
# MAGIC Este é o **primeiro módulo** de uma demo guiada de Lakehouse em saúde.
# MAGIC Aqui criamos apenas a **fundação** do ambiente:
# MAGIC
# MAGIC 1. Catálogo e schemas (Bronze / Silver / Gold)
# MAGIC 2. Cadastro base **`hospitais`**
# MAGIC 3. Cadastro base **`pacientes`**
# MAGIC
# MAGIC > **As tabelas transacionais (consultas, avaliações) NÃO são criadas aqui.**
# MAGIC > Elas serão construídas posteriormente, de forma **visual e interativa**, no **Lakeflow Designer**
# MAGIC > — o ambiente low-code/no-code de pipelines do Databricks.
# MAGIC
# MAGIC ## 🗺️ Roadmap completo da demo
# MAGIC
# MAGIC | Módulo | Onde acontece | Conteúdo |
# MAGIC |--------|--------------|----------|
# MAGIC | **1 — Setup (este notebook)** | Notebook SQL | Catálogo, schemas, cadastros base `hospitais` e `pacientes` |
# MAGIC | 2a — Landing Zone (este notebook) | Notebook SQL | Criar Volume + carregar CSVs brutos |
# MAGIC | 2b — Ingestão de consultas | **Lakeflow Designer** | Pipeline visual: CSV → Bronze → Silver |
# MAGIC | 3 — Ingestão de avaliações | **Lakeflow Designer** | Pipeline visual: CSV → Bronze |
# MAGIC | 4 — Enriquecimento (Visão 360) | **Lakeflow Designer** | Joins visuais entre fato e dimensões |
# MAGIC | 5 — IA Generativa / NLP | **Lakeflow Designer + AI Functions** | `ai_analyze_sentiment` em coluna calculada |
# MAGIC | 6 — Gold & Dashboards | **AI/BI Dashboards** | KPIs e visualizações executivas |
# MAGIC
# MAGIC > **Por que esse notebook é todo em SQL?**
# MAGIC > SQL é a linguagem **universal** dos times de dados — DBAs, analistas, engenheiros e cientistas
# MAGIC > entendem. No Databricks, SQL roda nativo, com performance idêntica a PySpark, e ainda permite
# MAGIC > usar **AI Functions** (`ai_analyze_sentiment`, `ai_classify`, `ai_extract`) direto na query.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup do Ambiente
# MAGIC
# MAGIC ### Objetivo
# MAGIC Preparar **catálogo** e **schemas** seguindo a arquitetura **Medallion**.
# MAGIC
# MAGIC ### O que o código fará
# MAGIC 1. Criar (idempotente) o catálogo `healthcare_lakehouse`
# MAGIC 2. Criar os schemas `bronze`, `silver` e `gold`
# MAGIC 3. Definir `bronze` como schema padrão da sessão (onde criaremos os cadastros base)
# MAGIC
# MAGIC ### Conceitos envolvidos
# MAGIC - **Unity Catalog**: governança centralizada de dados no Databricks
# MAGIC - **Three-level namespace**: `catalog.schema.table` — boa prática para organização
# MAGIC - **Arquitetura Medallion**: Bronze (cru) → Silver (limpo) → Gold (agregado)
# MAGIC - **`IF NOT EXISTS`**: idempotência — você pode rodar a célula várias vezes sem erro
# MAGIC
# MAGIC ### Resultado esperado
# MAGIC Catálogo e schemas criados, prontos para receber os cadastros base.

# COMMAND ----------

# DBTITLE 1,Criar catálogo
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS healthcare_lakehouse
# MAGIC COMMENT 'Healthcare Lakehouse Demo — Módulo 1';
# MAGIC
# MAGIC USE CATALOG healthcare_lakehouse;

# COMMAND ----------

# DBTITLE 1,Criar schemas Medallion
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Dados crus, ingeridos como vieram da fonte';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Dados limpos, validados e padronizados';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'KPIs agregados, prontos para BI';
# MAGIC
# MAGIC -- Define o schema padrão da sessão — evita prefixar tudo
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# DBTITLE 1,Confirmar setup
# MAGIC %sql
# MAGIC SELECT
# MAGIC   current_catalog() AS catalogo,
# MAGIC   current_schema()  AS schema_atual,
# MAGIC   'Catálogo e schemas criados — sessão configurada' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PARTE 1 — Camada Bronze: Cadastros Base
# MAGIC
# MAGIC ## O conceito de Camada Bronze
# MAGIC
# MAGIC A **camada Bronze** é a primeira camada do padrão arquitetural **Medallion** (Bronze → Silver → Gold).
# MAGIC Ela contém **dados brutos**, idealmente *imutáveis*, exatamente como vieram do sistema-fonte.
# MAGIC
# MAGIC ### Por que usar Delta Lake?
# MAGIC - **ACID transactions** → leituras e escritas seguras simultâneas
# MAGIC - **Time travel** → consultar versões anteriores da tabela
# MAGIC - **Schema enforcement** → impede dados corrompidos
# MAGIC - **Performance** → indexação e otimizações automáticas
# MAGIC
# MAGIC ### Por que cadastros base ficam persistidos desde já?
# MAGIC - **Hospitais** e **Pacientes** são entidades **estáveis** — mudam pouco
# MAGIC - Servem de **referência** (lookup) para todas as transações que virão depois
# MAGIC - Garantem **integridade referencial** quando o Lakeflow Designer for fazer os joins
# MAGIC - São o **"vocabulário" do hospital**: precisam estar prontas antes de qualquer ingestão transacional
# MAGIC
# MAGIC ### Governança & rastreabilidade no Unity Catalog
# MAGIC Cada tabela Delta no UC tem:
# MAGIC - Lineage automática
# MAGIC - Controle de acesso fino (`GRANT SELECT ON TABLE ...`)
# MAGIC - Histórico completo de alterações (`DESCRIBE HISTORY ...`)
# MAGIC - Tags e descrições para data discovery

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabela `hospitais` — enriquecida com dados de negócio
# MAGIC
# MAGIC **Objetivo:** Persistir o cadastro de **140 hospitais** com nomes brasileiros realistas (Santa Casa, Hospital Municipal, HC, Universitário, etc.) distribuídos em **30 cidades** das 5 regiões do Brasil.
# MAGIC
# MAGIC **Schema enriquecido — 12 colunas:**
# MAGIC
# MAGIC | Categoria | Colunas |
# MAGIC |-----------|---------|
# MAGIC | **Identificação** | `id_hospital`, `nome_hospital`, `especialidade` |
# MAGIC | **Localização** | `cidade`, `endereco`, `bairro`, `estado`, `regiao` |
# MAGIC | **Operacional** | `qtd_leitos`, `qtd_medicos`, `horario_funcionamento` |
# MAGIC | **NLP-ready** | `descricao_servicos` (texto livre para IA) |
# MAGIC
# MAGIC **Decisões de modelagem:**
# MAGIC - Cada hospital tem **identidade coerente** (ex: Instituto do Coração = Cardiologia)
# MAGIC - **Endereços realistas BR** (Av. Paulista em SP, Av. Atlântica no RJ, etc.)
# MAGIC - **Capacidade** alinhada à especialidade (Oncologia tem mais leitos que Dermatologia)
# MAGIC - **`descricao_servicos`** habilita demos de IA: `ai_extract`, `ai_classify`, vector search
# MAGIC
# MAGIC > **Casos de uso no Lakeflow Designer com esse schema:**
# MAGIC > - Dashboards Gold por **região** ou **estado** (filtros geográficos)
# MAGIC > - KPIs de **ocupação por leito** ou **consultas/médico**
# MAGIC > - **IA generativa** sobre `descricao_servicos` para classificar serviços oferecidos
# MAGIC
# MAGIC **Resultado esperado:** Tabela `healthcare_lakehouse.bronze.hospitais` com **140 registros** × **12 colunas**.

# COMMAND ----------

# DBTITLE 1,Criar e popular bronze.hospitais
# MAGIC %sql
# MAGIC -- Schema explícito enriquecido com dados de localização, capacidade e descrição
# MAGIC CREATE OR REPLACE TABLE hospitais (
# MAGIC   id_hospital            INT     NOT NULL COMMENT 'Chave primária — ID único do hospital',
# MAGIC   nome_hospital          STRING  NOT NULL COMMENT 'Nome comercial do hospital',
# MAGIC   especialidade         STRING  NOT NULL COMMENT 'Especialidade médica principal',
# MAGIC   cidade                STRING  NOT NULL COMMENT 'Cidade onde o hospital está localizado',
# MAGIC   endereco              STRING  NOT NULL COMMENT 'Endereço completo (logradouro + número)',
# MAGIC   bairro                STRING  NOT NULL COMMENT 'Bairro do hospital',
# MAGIC   estado                STRING  NOT NULL COMMENT 'Estado (UF) — para filtros geográficos',
# MAGIC   regiao                STRING  NOT NULL COMMENT 'Região (Norte/NE/CO/SE/Sul) — para agregações regionais',
# MAGIC   qtd_leitos            INT     NOT NULL COMMENT 'Capacidade de leitos para internação',
# MAGIC   qtd_medicos           INT     NOT NULL COMMENT 'Quantidade de médicos no corpo clínico',
# MAGIC   horario_funcionamento STRING  NOT NULL COMMENT 'Horário de funcionamento (24h, 07h-19h, etc)',
# MAGIC   descricao_servicos    STRING  NOT NULL COMMENT 'Descrição textual dos serviços (campo para análise NLP/IA)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Cadastro base de hospitais enriquecido — localização, capacidade, descrição (NLP-ready)';
# MAGIC
# MAGIC -- 140 hospitais com nomes brasileiros realistas (Santa Casa, HC, Municipal, Universitário, Maternidade, Pronto-Socorro, Centro Médico, Instituto)
# MAGIC -- Endereços, bairros e tamanhos variam por cidade e especialidade
# MAGIC -- descricao_servicos é texto livre rico (pronto para ai_extract, ai_classify, vector search)
# MAGIC INSERT INTO hospitais VALUES
# MAGIC   ( 1, 'Hospital de Clínicas de São Paulo', 'Clínica Geral', 'São Paulo', 'R. da Consolação, 147', 'Pinheiros', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 2, 'Santa Casa de Misericórdia de São Paulo', 'Clínica Geral', 'São Paulo', 'Av. Faria Lima, 194', 'Vila Mariana', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 3, 'Hospital Municipal Dr. Arthur Ribeiro', 'Pediatria', 'São Paulo', 'R. Augusta, 241', 'Itaim Bibi', 'SP', 'Sudeste', 50, 30, '24h', 'Pediatria geral, vacinação infantil, acompanhamento de desenvolvimento, urgências pediátricas e consultoria nutricional.'),
# MAGIC   ( 4, 'Instituto de Cardiologia Paulista', 'Cardiologia', 'São Paulo', 'Av. 23 de Maio, 288', 'Higienópolis', 'SP', 'Sudeste', 60, 35, '24h', 'Especializada em cardiologia clínica e intervencionista, com exames de ecocardiograma, holter, teste ergométrico e cateterismo.'),
# MAGIC   ( 5, 'Centro de Oncologia da Vila Mariana', 'Oncologia', 'São Paulo', 'Av. Paulista, 335', 'Jardins', 'SP', 'Sudeste', 120, 60, '24h', 'Tratamento oncológico completo com quimioterapia, radioterapia, imunoterapia e acompanhamento psicológico ao paciente e família.'),
# MAGIC   ( 6, 'Maternidade Nossa Senhora de Fátima', 'Ginecologia', 'São Paulo', 'R. da Consolação, 382', 'Liberdade', 'SP', 'Sudeste', 35, 22, '07h-19h', 'Saúde da mulher em todas as fases: consultas, exames preventivos, gestação de alto risco, planejamento familiar e cirurgias ginecológicas.'),
# MAGIC   ( 7, 'Pronto-Socorro Central da Paulista', 'Clínica Geral', 'São Paulo', 'Av. Faria Lima, 429', 'Mooca', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 8, 'Hematologia Brasil', 'Hematologia', 'São Paulo', 'R. Augusta, 476', 'Bela Vista', 'SP', 'Sudeste', 70, 35, '24h', 'Hematologia e oncohematologia: tratamento de anemias, leucemias, linfomas, distúrbios de coagulação e transplante de medula.'),
# MAGIC   ( 9, 'Santa Casa da Misericórdia do Rio', 'Clínica Geral', 'Rio de Janeiro', 'R. das Laranjeiras, 523', 'Botafogo', 'RJ', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (10, 'Hospital Municipal Souza Aguiar', 'Ortopedia', 'Rio de Janeiro', 'Av. Rio Branco, 570', 'Barra da Tijuca', 'RJ', 'Sudeste', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   (11, 'Instituto Estadual de Cardiologia', 'Cardiologia', 'Rio de Janeiro', 'R. Marquês de S. Vicente, 617', 'Ipanema', 'RJ', 'Sudeste', 60, 35, '24h', 'Especializada em cardiologia clínica e intervencionista, com exames de ecocardiograma, holter, teste ergométrico e cateterismo.'),
# MAGIC   (12, 'Hospital Regional de Botafogo', 'Psiquiatria', 'Rio de Janeiro', 'Av. Atlântica, 664', 'Centro', 'RJ', 'Sudeste', 40, 22, '07h-19h', 'Psiquiatria adulta e infantil. Tratamento de depressão, ansiedade, transtorno bipolar, TDAH, dependência química e psicoterapia.'),
# MAGIC   (13, 'Hospital de Nefrologia da Tijuca', 'Nefrologia', 'Rio de Janeiro', 'R. das Laranjeiras, 711', 'Méier', 'RJ', 'Sudeste', 50, 28, '07h-22h', 'Tratamento de doenças renais, hipertensão, hemodiálise, transplante renal e acompanhamento de pacientes com insuficiência renal crônica.'),
# MAGIC   (14, 'Hospital de Clínicas da UFMG', 'Clínica Geral', 'Belo Horizonte', 'R. da Bahia, 758', 'Funcionários', 'MG', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (15, 'Hospital Municipal Odilon Behrens', 'Pediatria', 'Belo Horizonte', 'Av. Afonso Pena, 805', 'Santa Efigênia', 'MG', 'Sudeste', 50, 30, '24h', 'Pediatria geral, vacinação infantil, acompanhamento de desenvolvimento, urgências pediátricas e consultoria nutricional.'),
# MAGIC   (16, 'Hospital Regional de Gastroenterologia', 'Gastroenterologia', 'Belo Horizonte', 'Av. do Contorno, 852', 'Savassi', 'MG', 'Sudeste', 30, 22, '07h-19h', 'Gastroenterologia clínica com endoscopia, colonoscopia, tratamento de refluxo, gastrite, doenças intestinais e hepáticas.'),
# MAGIC   (17, 'Hospital Universitário de Brasília', 'Neurologia', 'Brasília', 'SHIS QI 17, 899', 'Lago Sul', 'DF', 'Centro-Oeste', 45, 28, '07h-21h', 'Diagnóstico e tratamento de distúrbios neurológicos: enxaquecas, epilepsia, AVC, Parkinson, Alzheimer. Eletroencefalograma disponível.'),
# MAGIC   (18, 'Hospital Regional da Asa Sul', 'Clínica Geral', 'Brasília', 'SCS Quadra 4, 946', 'Asa Sul', 'DF', 'Centro-Oeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (19, 'Hospital de Clínicas da UFPR', 'Pediatria', 'Curitiba', 'R. XV de Novembro, 993', 'Centro', 'PR', 'Sul', 50, 30, '24h', 'Pediatria geral, vacinação infantil, acompanhamento de desenvolvimento, urgências pediátricas e consultoria nutricional.'),
# MAGIC   (20, 'Santa Casa de Curitiba', 'Clínica Geral', 'Curitiba', 'Av. Vicente Machado, 1040', 'Batel', 'PR', 'Sul', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (21, 'Hospital de Clínicas de Porto Alegre', 'Ortopedia', 'Porto Alegre', 'Av. Ipiranga, 1087', 'Bela Vista', 'RS', 'Sul', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   (22, 'Santa Casa de Porto Alegre', 'Clínica Geral', 'Porto Alegre', 'Av. Independência, 1134', 'Petrópolis', 'RS', 'Sul', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (23, 'Hospital Universitário Polydoro Ernani', 'Ortopedia', 'Florianópolis', 'R. Lauro Linhares, 1181', 'Agronômica', 'SC', 'Sul', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   (24, 'Hospital Geral Roberto Santos', 'Dermatologia', 'Salvador', 'Av. Tancredo Neves, 1228', 'Pituba', 'BA', 'Nordeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   (25, 'Santa Casa de Misericórdia da Bahia', 'Clínica Geral', 'Salvador', 'Av. ACM, 1275', 'Itaigara', 'BA', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (26, 'Hospital das Clínicas da UFPE', 'Ginecologia', 'Recife', 'Av. Boa Viagem, 1322', 'Casa Forte', 'PE', 'Nordeste', 35, 22, '07h-19h', 'Saúde da mulher em todas as fases: consultas, exames preventivos, gestação de alto risco, planejamento familiar e cirurgias ginecológicas.'),
# MAGIC   (27, 'Maternidade Bandeira Filho', 'Ginecologia', 'Recife', 'Av. Conde da Boa Vista, 1369', 'Boa Viagem', 'PE', 'Nordeste', 35, 22, '07h-19h', 'Saúde da mulher em todas as fases: consultas, exames preventivos, gestação de alto risco, planejamento familiar e cirurgias ginecológicas.'),
# MAGIC   (28, 'Hospital Geral de Fortaleza', 'Oftalmologia', 'Fortaleza', 'Av. Santos Dumont, 1416', 'Meireles', 'CE', 'Nordeste', 10, 18, '07h-19h', 'Oftalmologia completa: exames de refração, tratamento de catarata, glaucoma, retina, cirurgias refrativas (LASIK) e atendimento pediátrico.'),
# MAGIC   (29, 'Instituto Dr. José Frota', 'Clínica Geral', 'Fortaleza', 'Av. Dom Luís, 1463', 'Cocó', 'CE', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (30, 'Hospital de Clínicas da UFG', 'Endocrinologia', 'Goiânia', 'Av. T-63, 1510', 'Setor Bueno', 'GO', 'Centro-Oeste', 5, 15, '07h-19h', 'Tratamento de diabetes, tireoide, obesidade, distúrbios hormonais e metabolismo. Acompanhamento nutricional integrado.'),
# MAGIC   (31, 'Fundação Hospital Adriano Jorge', 'Pneumologia', 'Manaus', 'Av. Eduardo Ribeiro, 1557', 'Centro', 'AM', 'Norte', 30, 18, '07h-21h', 'Especializada em doenças respiratórias: asma, DPOC, apneia do sono, tuberculose. Espirometria e polissonografia.'),
# MAGIC   (32, 'Hospital de Clínicas da UNICAMP', 'Reumatologia', 'Campinas', 'Av. Norte-Sul, 1604', 'Mansões Santo Antônio', 'SP', 'Sudeste', 5, 12, '07h-19h', 'Diagnóstico e tratamento de artrite, artrose, lúpus, fibromialgia e outras doenças reumatológicas. Infiltrações disponíveis.'),
# MAGIC   (33, 'Pronto-Socorro Mário Gatti', 'Clínica Geral', 'Campinas', 'Av. Brasil, 1651', 'Cambuí', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (34, 'Hospital Universitário Cassiano Antônio', 'Otorrinolaringologia', 'Vitória', 'Av. Reta da Penha, 1698', 'Jardim Camburi', 'ES', 'Sudeste', 15, 15, '07h-19h', 'Otorrinolaringologia completa: tratamento de sinusite, amigdalite, distúrbios auditivos, vertigem e cirurgias do ouvido, nariz e garganta.'),
# MAGIC   (35, 'Hospital de Oftalmologia Visão Clara', 'Oftalmologia', 'São Paulo', 'Av. Paulista, 1745', 'Itaim Bibi', 'SP', 'Sudeste', 10, 18, '07h-19h', 'Oftalmologia completa: exames de refração, tratamento de catarata, glaucoma, retina, cirurgias refrativas (LASIK) e atendimento pediátrico.'),
# MAGIC   (36, 'Centro de Urologia Avançada', 'Urologia', 'São Paulo', 'R. da Consolação, 1792', 'Higienópolis', 'SP', 'Sudeste', 25, 18, '07h-19h', 'Urologia clínica e cirúrgica. Tratamento de cálculo renal, próstata, incontinência urinária e atendimento andrológico.'),
# MAGIC   (37, 'Clínica Médica Bem-Estar', 'Clínica Geral', 'Rio de Janeiro', 'R. das Laranjeiras, 1839', 'Botafogo', 'RJ', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (38, 'Centro de Dermatologia da Pituba', 'Dermatologia', 'Salvador', 'Av. Tancredo Neves, 1886', 'Graça', 'BA', 'Nordeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   (39, 'Hospital de Endocrinologia da Aldeota', 'Endocrinologia', 'Fortaleza', 'Av. Dom Luís, 1933', 'Aldeota', 'CE', 'Nordeste', 5, 15, '07h-19h', 'Tratamento de diabetes, tireoide, obesidade, distúrbios hormonais e metabolismo. Acompanhamento nutricional integrado.'),
# MAGIC   (40, 'Hospital São Lucas', 'Hematologia', 'Curitiba', 'R. XV de Novembro, 1980', 'Batel', 'PR', 'Sul', 70, 35, '24h', 'Hematologia e oncohematologia: tratamento de anemias, leucemias, linfomas, distúrbios de coagulação e transplante de medula.'),
# MAGIC   ( 41, 'Instituto Regional de Neurologia', 'Neurologia', 'São Paulo', 'Av. 23 de Maio, 2027', 'Bela Vista', 'SP', 'Sudeste', 45, 28, '07h-21h', 'Diagnóstico e tratamento de distúrbios neurológicos: enxaquecas, epilepsia, AVC, Parkinson, Alzheimer. Eletroencefalograma disponível.'),
# MAGIC   ( 42, 'Hospital Municipal Tatuapé', 'Clínica Geral', 'São Paulo', 'Av. Paulista, 2074', 'Liberdade', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 43, 'Instituto de Oncologia de São Paulo', 'Oncologia', 'São Paulo', 'R. da Consolação, 2121', 'Mooca', 'SP', 'Sudeste', 120, 60, '24h', 'Tratamento oncológico completo com quimioterapia, radioterapia, imunoterapia e acompanhamento psicológico ao paciente e família.'),
# MAGIC   ( 44, 'Santa Casa de Misericórdia de São Paulo', 'Clínica Geral', 'São Paulo', 'R. da Consolação, 2168', 'Bela Vista', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 45, 'Pronto-Socorro Central de São Paulo', 'Clínica Geral', 'São Paulo', 'Av. Faria Lima, 2215', 'Liberdade', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 46, 'Instituto de Ortopedia de São Paulo', 'Ortopedia', 'São Paulo', 'Av. Faria Lima, 2262', 'Itaim Bibi', 'SP', 'Sudeste', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   ( 47, 'Hospital Beneficência São Francisco', 'Clínica Geral', 'São Paulo', 'R. Augusta, 2309', 'Jardins', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 48, 'Instituto Regional de Oncologia', 'Oncologia', 'São Paulo', 'Av. Faria Lima, 2356', 'Liberdade', 'SP', 'Sudeste', 120, 60, '24h', 'Tratamento oncológico completo com quimioterapia, radioterapia, imunoterapia e acompanhamento psicológico ao paciente e família.'),
# MAGIC   ( 49, 'Centro Médico Tatuapé', 'Nefrologia', 'São Paulo', 'Av. Faria Lima, 2403', 'Liberdade', 'SP', 'Sudeste', 50, 28, '07h-22h', 'Tratamento de doenças renais, hipertensão, hemodiálise, transplante renal e acompanhamento de pacientes com insuficiência renal crônica.'),
# MAGIC   ( 50, 'Instituto de Pneumologia de São Paulo', 'Pneumologia', 'São Paulo', 'Av. Faria Lima, 2450', 'Pinheiros', 'SP', 'Sudeste', 30, 18, '07h-21h', 'Especializada em doenças respiratórias: asma, DPOC, apneia do sono, tuberculose. Espirometria e polissonografia.'),
# MAGIC   ( 51, 'Instituto de Dermatologia de São Paulo', 'Dermatologia', 'São Paulo', 'R. Augusta, 2497', 'Bela Vista', 'SP', 'Sudeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   ( 52, 'Instituto Regional de Ortopedia', 'Ortopedia', 'São Paulo', 'Av. 23 de Maio, 2544', 'Bela Vista', 'SP', 'Sudeste', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   ( 53, 'Hospital de Clínicas de São Paulo', 'Cardiologia', 'São Paulo', 'Av. Faria Lima, 2591', 'Itaim Bibi', 'SP', 'Sudeste', 60, 35, '24h', 'Especializada em cardiologia clínica e intervencionista, com exames de ecocardiograma, holter, teste ergométrico e cateterismo.'),
# MAGIC   ( 54, 'Hospital Universitário Itaim Bibi', 'Neurologia', 'São Paulo', 'Av. Paulista, 2638', 'Bela Vista', 'SP', 'Sudeste', 45, 28, '07h-21h', 'Diagnóstico e tratamento de distúrbios neurológicos: enxaquecas, epilepsia, AVC, Parkinson, Alzheimer. Eletroencefalograma disponível.'),
# MAGIC   ( 55, 'Instituto de Pediatria de São Paulo', 'Pediatria', 'São Paulo', 'Av. Paulista, 2685', 'Itaim Bibi', 'SP', 'Sudeste', 50, 30, '24h', 'Pediatria geral, vacinação infantil, acompanhamento de desenvolvimento, urgências pediátricas e consultoria nutricional.'),
# MAGIC   ( 56, 'Santa Casa de São Paulo', 'Clínica Geral', 'São Paulo', 'R. da Consolação, 2732', 'Bela Vista', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 57, 'Fundação Hospitalar de São Paulo', 'Clínica Geral', 'São Paulo', 'Av. 23 de Maio, 2779', 'Vila Mariana', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 58, 'Instituto de Otorrinolaringologia de São Paulo', 'Otorrinolaringologia', 'São Paulo', 'R. da Consolação, 2826', 'Vila Mariana', 'SP', 'Sudeste', 15, 15, '07h-19h', 'Otorrinolaringologia completa: tratamento de sinusite, amigdalite, distúrbios auditivos, vertigem e cirurgias do ouvido, nariz e garganta.'),
# MAGIC   ( 59, 'Pronto-Socorro Municipal', 'Clínica Geral', 'São Paulo', 'R. Augusta, 2873', 'Pinheiros', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 60, 'Instituto Regional de Cardiologia', 'Cardiologia', 'São Paulo', 'Av. 23 de Maio, 120', 'Itaim Bibi', 'SP', 'Sudeste', 60, 35, '24h', 'Especializada em cardiologia clínica e intervencionista, com exames de ecocardiograma, holter, teste ergométrico e cateterismo.'),
# MAGIC   ( 61, 'Centro Clínico São Lucas', 'Reumatologia', 'Rio de Janeiro', 'R. Marquês de S. Vicente, 167', 'Ipanema', 'RJ', 'Sudeste', 5, 12, '07h-19h', 'Diagnóstico e tratamento de artrite, artrose, lúpus, fibromialgia e outras doenças reumatológicas. Infiltrações disponíveis.'),
# MAGIC   ( 62, 'Centro de Saúde Méier', 'Ortopedia', 'Rio de Janeiro', 'Av. Rio Branco, 214', 'Botafogo', 'RJ', 'Sudeste', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   ( 63, 'Hospital de Clínicas de Rio de Janeiro', 'Hematologia', 'Rio de Janeiro', 'Av. Atlântica, 261', 'Botafogo', 'RJ', 'Sudeste', 70, 35, '24h', 'Hematologia e oncohematologia: tratamento de anemias, leucemias, linfomas, distúrbios de coagulação e transplante de medula.'),
# MAGIC   ( 64, 'Centro Clínico Nossa Senhora Aparecida', 'Dermatologia', 'Rio de Janeiro', 'Av. Rio Branco, 308', 'Tijuca', 'RJ', 'Sudeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   ( 65, 'Instituto de Urologia de Rio de Janeiro', 'Urologia', 'Rio de Janeiro', 'Av. Atlântica, 355', 'Ipanema', 'RJ', 'Sudeste', 25, 18, '07h-19h', 'Urologia clínica e cirúrgica. Tratamento de cálculo renal, próstata, incontinência urinária e atendimento andrológico.'),
# MAGIC   ( 66, 'Santa Casa de Rio de Janeiro', 'Clínica Geral', 'Rio de Janeiro', 'R. Marquês de S. Vicente, 402', 'Méier', 'RJ', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 67, 'Hospital Municipal de Rio de Janeiro', 'Pediatria', 'Rio de Janeiro', 'R. das Laranjeiras, 449', 'Tijuca', 'RJ', 'Sudeste', 50, 30, '24h', 'Pediatria geral, vacinação infantil, acompanhamento de desenvolvimento, urgências pediátricas e consultoria nutricional.'),
# MAGIC   ( 68, 'Instituto de Ginecologia de Rio de Janeiro', 'Ginecologia', 'Rio de Janeiro', 'R. das Laranjeiras, 496', 'Tijuca', 'RJ', 'Sudeste', 35, 22, '07h-19h', 'Saúde da mulher em todas as fases: consultas, exames preventivos, gestação de alto risco, planejamento familiar e cirurgias ginecológicas.'),
# MAGIC   ( 69, 'Fundação Beneficente Barra da Tijuca', 'Clínica Geral', 'Rio de Janeiro', 'Av. Rio Branco, 543', 'Tijuca', 'RJ', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 70, 'Hospital Regional Norte', 'Clínica Geral', 'Rio de Janeiro', 'Av. Atlântica, 590', 'Centro', 'RJ', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 71, 'Hospital Beneficência São Camilo', 'Clínica Geral', 'Rio de Janeiro', 'R. Marquês de S. Vicente, 637', 'Centro', 'RJ', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 72, 'Instituto Regional de Dermatologia', 'Dermatologia', 'Rio de Janeiro', 'Av. Atlântica, 684', 'Leblon', 'RJ', 'Sudeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   ( 73, 'Instituto de Otorrinolaringologia de Belo Horizonte', 'Otorrinolaringologia', 'Belo Horizonte', 'R. da Bahia, 731', 'Funcionários', 'MG', 'Sudeste', 15, 15, '07h-19h', 'Otorrinolaringologia completa: tratamento de sinusite, amigdalite, distúrbios auditivos, vertigem e cirurgias do ouvido, nariz e garganta.'),
# MAGIC   ( 74, 'Santa Casa de Misericórdia de Belo Horizonte', 'Clínica Geral', 'Belo Horizonte', 'Av. Afonso Pena, 778', 'Santa Efigênia', 'MG', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 75, 'Fundação Hospitalar de Belo Horizonte', 'Clínica Geral', 'Belo Horizonte', 'Av. do Contorno, 825', 'Savassi', 'MG', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 76, 'Hospital Universitário de Belo Horizonte', 'Clínica Geral', 'Belo Horizonte', 'Av. do Contorno, 872', 'Funcionários', 'MG', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 77, 'Instituto de Oftalmologia de Belo Horizonte', 'Oftalmologia', 'Belo Horizonte', 'R. da Bahia, 919', 'Santa Efigênia', 'MG', 'Sudeste', 10, 18, '07h-19h', 'Oftalmologia completa: exames de refração, tratamento de catarata, glaucoma, retina, cirurgias refrativas (LASIK) e atendimento pediátrico.'),
# MAGIC   ( 78, 'Hospital Municipal Águas Claras', 'Clínica Geral', 'Brasília', 'SHIS QI 17, 966', 'Asa Norte', 'DF', 'Centro-Oeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 79, 'Instituto de Ortopedia de Brasília', 'Ortopedia', 'Brasília', 'SQS 308, 1013', 'Asa Norte', 'DF', 'Centro-Oeste', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   ( 80, 'Centro Clínico Padre Cícero', 'Ortopedia', 'Brasília', 'SHIS QI 17, 1060', 'Sudoeste', 'DF', 'Centro-Oeste', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   ( 81, 'Centro Médico Asa Norte', 'Ortopedia', 'Brasília', 'SHIS QI 17, 1107', 'Lago Sul', 'DF', 'Centro-Oeste', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   ( 82, 'Instituto de Ortopedia de Curitiba', 'Ortopedia', 'Curitiba', 'Av. Vicente Machado, 1154', 'Cabral', 'PR', 'Sul', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   ( 83, 'Santa Casa de Curitiba', 'Clínica Geral', 'Curitiba', 'R. XV de Novembro, 1201', 'Centro', 'PR', 'Sul', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 84, 'Hospital Universitário de Curitiba', 'Clínica Geral', 'Curitiba', 'R. XV de Novembro, 1248', 'Cabral', 'PR', 'Sul', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 85, 'Santa Casa de Misericórdia de Porto Alegre', 'Clínica Geral', 'Porto Alegre', 'Av. Independência, 1295', 'Petrópolis', 'RS', 'Sul', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 86, 'Hospital Universitário Boa Vista', 'Clínica Geral', 'Porto Alegre', 'Av. Ipiranga, 1342', 'Moinhos de Vento', 'RS', 'Sul', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 87, 'Hospital Municipal de Porto Alegre', 'Clínica Geral', 'Porto Alegre', 'Av. Independência, 1389', 'Centro', 'RS', 'Sul', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 88, 'Hospital Estadual Moinhos de Vento', 'Clínica Geral', 'Porto Alegre', 'Av. Ipiranga, 1436', 'Centro', 'RS', 'Sul', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 89, 'Santa Casa de Misericórdia de Salvador', 'Clínica Geral', 'Salvador', 'Av. ACM, 1483', 'Graça', 'BA', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 90, 'Hospital de Clínicas de Salvador', 'Oncologia', 'Salvador', 'Av. Tancredo Neves, 1530', 'Barra', 'BA', 'Nordeste', 120, 60, '24h', 'Tratamento oncológico completo com quimioterapia, radioterapia, imunoterapia e acompanhamento psicológico ao paciente e família.'),
# MAGIC   ( 91, 'Instituto de Dermatologia de Salvador', 'Dermatologia', 'Salvador', 'Av. ACM, 1577', 'Itaigara', 'BA', 'Nordeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   ( 92, 'Santa Casa de Salvador', 'Clínica Geral', 'Salvador', 'Av. ACM, 1624', 'Itaigara', 'BA', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 93, 'Instituto Regional de Ginecologia', 'Ginecologia', 'Recife', 'Av. Conde da Boa Vista, 1671', 'Casa Forte', 'PE', 'Nordeste', 35, 22, '07h-19h', 'Saúde da mulher em todas as fases: consultas, exames preventivos, gestação de alto risco, planejamento familiar e cirurgias ginecológicas.'),
# MAGIC   ( 94, 'Centro de Saúde Casa Forte', 'Otorrinolaringologia', 'Recife', 'Av. Conde da Boa Vista, 1718', 'Casa Forte', 'PE', 'Nordeste', 15, 15, '07h-19h', 'Otorrinolaringologia completa: tratamento de sinusite, amigdalite, distúrbios auditivos, vertigem e cirurgias do ouvido, nariz e garganta.'),
# MAGIC   ( 95, 'Instituto de Ortopedia de Recife', 'Ortopedia', 'Recife', 'Av. Conde da Boa Vista, 1765', 'Boa Viagem', 'PE', 'Nordeste', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   ( 96, 'Hospital Regional Sul', 'Clínica Geral', 'Recife', 'Av. Boa Viagem, 1812', 'Boa Viagem', 'PE', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 97, 'Centro de Saúde Cocó', 'Urologia', 'Fortaleza', 'Av. Santos Dumont, 1859', 'Praia de Iracema', 'CE', 'Nordeste', 25, 18, '07h-19h', 'Urologia clínica e cirúrgica. Tratamento de cálculo renal, próstata, incontinência urinária e atendimento andrológico.'),
# MAGIC   ( 98, 'Fundação Hospitalar de Fortaleza', 'Clínica Geral', 'Fortaleza', 'Av. Dom Luís, 1906', 'Meireles', 'CE', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   ( 99, 'Centro Médico Praia de Iracema', 'Pediatria', 'Fortaleza', 'Av. Dom Luís, 1953', 'Praia de Iracema', 'CE', 'Nordeste', 50, 30, '24h', 'Pediatria geral, vacinação infantil, acompanhamento de desenvolvimento, urgências pediátricas e consultoria nutricional.'),
# MAGIC   (100, 'Hospital Universitário Praia de Iracema', 'Clínica Geral', 'Fortaleza', 'Av. Dom Luís, 2000', 'Aldeota', 'CE', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (101, 'Hospital Municipal Trindade', 'Clínica Geral', 'Florianópolis', 'Av. Beira-Mar Norte, 2047', 'Agronômica', 'SC', 'Sul', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (102, 'Fundação Hospitalar de Florianópolis', 'Clínica Geral', 'Florianópolis', 'Av. Beira-Mar Norte, 2094', 'Lagoa', 'SC', 'Sul', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (103, 'Fundação Hospitalar de Goiânia', 'Clínica Geral', 'Goiânia', 'Av. T-63, 2141', 'Setor Oeste', 'GO', 'Centro-Oeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (104, 'Centro de Saúde Setor Sul', 'Dermatologia', 'Goiânia', 'Av. 85, 2188', 'Setor Sul', 'GO', 'Centro-Oeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   (105, 'Centro Médico Centro', 'Urologia', 'Manaus', 'Av. Eduardo Ribeiro, 2235', 'Flores', 'AM', 'Norte', 25, 18, '07h-19h', 'Urologia clínica e cirúrgica. Tratamento de cálculo renal, próstata, incontinência urinária e atendimento andrológico.'),
# MAGIC   (106, 'Fundação Beneficente Centro', 'Clínica Geral', 'Manaus', 'Av. Eduardo Ribeiro, 2282', 'Parque Dez', 'AM', 'Norte', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (107, 'Hospital Municipal Central de Campinas', 'Clínica Geral', 'Campinas', 'Av. Brasil, 2329', 'Mansões Santo Antônio', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (108, 'Hospital de Clínicas de Campinas', 'Clínica Geral', 'Campinas', 'Av. Brasil, 2376', 'Mansões Santo Antônio', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (109, 'Hospital Estadual Mansões Santo Antônio', 'Clínica Geral', 'Campinas', 'Av. Norte-Sul, 2423', 'Cambuí', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (110, 'Instituto Regional de Reumatologia', 'Reumatologia', 'Vitória', 'Av. Reta da Penha, 2470', 'Mata da Praia', 'ES', 'Sudeste', 5, 12, '07h-19h', 'Diagnóstico e tratamento de artrite, artrose, lúpus, fibromialgia e outras doenças reumatológicas. Infiltrações disponíveis.'),
# MAGIC   (111, 'Hospital de Clínicas de Vitória', 'Hematologia', 'Vitória', 'Av. Reta da Penha, 2517', 'Jardim Camburi', 'ES', 'Sudeste', 70, 35, '24h', 'Hematologia e oncohematologia: tratamento de anemias, leucemias, linfomas, distúrbios de coagulação e transplante de medula.'),
# MAGIC   (112, 'Instituto de Otorrinolaringologia de Belém', 'Otorrinolaringologia', 'Belém', 'Av. Presidente Vargas, 2564', 'Nazaré', 'PA', 'Norte', 15, 15, '07h-19h', 'Otorrinolaringologia completa: tratamento de sinusite, amigdalite, distúrbios auditivos, vertigem e cirurgias do ouvido, nariz e garganta.'),
# MAGIC   (113, 'Hospital da Caridade', 'Clínica Geral', 'Belém', 'Av. Nazaré, 2611', 'Batista Campos', 'PA', 'Norte', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (114, 'Centro de Saúde Umarizal', 'Ortopedia', 'Belém', 'Av. Nazaré, 2658', 'Nazaré', 'PA', 'Norte', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   (115, 'Hospital Beneficência Santo Antônio', 'Clínica Geral', 'São Luís', 'Av. dos Holandeses, 2705', 'Calhau', 'MA', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (116, 'Centro de Saúde Calhau', 'Dermatologia', 'São Luís', 'Av. dos Holandeses, 2752', 'Calhau', 'MA', 'Nordeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   (117, 'Instituto Regional de Gastroenterologia', 'Gastroenterologia', 'São Luís', 'Av. dos Holandeses, 2799', 'Calhau', 'MA', 'Nordeste', 30, 22, '07h-19h', 'Gastroenterologia clínica com endoscopia, colonoscopia, tratamento de refluxo, gastrite, doenças intestinais e hepáticas.'),
# MAGIC   (118, 'Instituto Regional de Oftalmologia', 'Oftalmologia', 'Maceió', 'Av. Brasil, 2846', 'Jatiúca', 'AL', 'Nordeste', 10, 18, '07h-19h', 'Oftalmologia completa: exames de refração, tratamento de catarata, glaucoma, retina, cirurgias refrativas (LASIK) e atendimento pediátrico.'),
# MAGIC   (119, 'Hospital de Clínicas de Maceió', 'Cardiologia', 'Maceió', 'Av. Brasil, 2893', 'Ponta Verde', 'AL', 'Nordeste', 60, 35, '24h', 'Especializada em cardiologia clínica e intervencionista, com exames de ecocardiograma, holter, teste ergométrico e cateterismo.'),
# MAGIC   (120, 'Hospital Beneficência Dom Bosco', 'Clínica Geral', 'Maceió', 'Av. Brasil, 140', 'Pajuçara', 'AL', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (121, 'Hospital Universitário de Natal', 'Cardiologia', 'Natal', 'Av. Engenheiro Roberto Freire, 187', 'Lagoa Nova', 'RN', 'Nordeste', 60, 35, '24h', 'Especializada em cardiologia clínica e intervencionista, com exames de ecocardiograma, holter, teste ergométrico e cateterismo.'),
# MAGIC   (122, 'Fundação Beneficente Petrópolis', 'Clínica Geral', 'Natal', 'Av. Hermes da Fonseca, 234', 'Lagoa Nova', 'RN', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (123, 'Instituto de Dermatologia de Natal', 'Dermatologia', 'Natal', 'Av. Engenheiro Roberto Freire, 281', 'Tirol', 'RN', 'Nordeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   (124, 'Fundação Hospitalar de João Pessoa', 'Clínica Geral', 'João Pessoa', 'Av. Epitácio Pessoa, 328', 'Cabo Branco', 'PB', 'Nordeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (125, 'Instituto de Cardiologia de João Pessoa', 'Cardiologia', 'João Pessoa', 'Av. Almirante Tamandaré, 375', 'Manaíra', 'PB', 'Nordeste', 60, 35, '24h', 'Especializada em cardiologia clínica e intervencionista, com exames de ecocardiograma, holter, teste ergométrico e cateterismo.'),
# MAGIC   (126, 'Hospital Universitário Centro Norte', 'Clínica Geral', 'Cuiabá', 'Av. Getúlio Vargas, 422', 'Goiabeiras', 'MT', 'Centro-Oeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (127, 'Centro Clínico Madre Tereza', 'Ortopedia', 'Cuiabá', 'Av. Getúlio Vargas, 469', 'Centro Norte', 'MT', 'Centro-Oeste', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   (128, 'Hospital Municipal de Campo Grande', 'Pediatria', 'Campo Grande', 'Av. Afonso Pena, 516', 'Jardim dos Estados', 'MS', 'Centro-Oeste', 50, 30, '24h', 'Pediatria geral, vacinação infantil, acompanhamento de desenvolvimento, urgências pediátricas e consultoria nutricional.'),
# MAGIC   (129, 'Hospital Municipal Central de Campo Grande', 'Clínica Geral', 'Campo Grande', 'Av. Mato Grosso, 563', 'Centro', 'MS', 'Centro-Oeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (130, 'Instituto de Endocrinologia de Aracaju', 'Endocrinologia', 'Aracaju', 'Av. Beira Mar, 610', 'Atalaia', 'SE', 'Nordeste', 5, 15, '07h-19h', 'Tratamento de diabetes, tireoide, obesidade, distúrbios hormonais e metabolismo. Acompanhamento nutricional integrado.'),
# MAGIC   (131, 'Centro de Saúde Jardins', 'Psiquiatria', 'Aracaju', 'Av. Tancredo Neves, 657', 'Jardins', 'SE', 'Nordeste', 40, 22, '07h-19h', 'Psiquiatria adulta e infantil. Tratamento de depressão, ansiedade, transtorno bipolar, TDAH, dependência química e psicoterapia.'),
# MAGIC   (132, 'Instituto de Dermatologia de Teresina', 'Dermatologia', 'Teresina', 'Av. Frei Serafim, 704', 'Centro', 'PI', 'Nordeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   (133, 'Hospital Universitário de Teresina', 'Cardiologia', 'Teresina', 'Av. Frei Serafim, 751', 'Fátima', 'PI', 'Nordeste', 60, 35, '24h', 'Especializada em cardiologia clínica e intervencionista, com exames de ecocardiograma, holter, teste ergométrico e cateterismo.'),
# MAGIC   (134, 'Hospital Beneficência Madre Tereza', 'Clínica Geral', 'São Bernardo do Campo', 'Av. Pereira Barreto, 798', 'Centro', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (135, 'Centro Clínico São José', 'Ortopedia', 'São Bernardo do Campo', 'Av. Kennedy, 845', 'Centro', 'SP', 'Sudeste', 40, 25, '07h-19h', 'Ortopedia geral, traumatologia esportiva e cirurgia da coluna. Atendimento de fraturas, lesões esportivas e dores articulares.'),
# MAGIC   (136, 'Fundação Beneficente Jardim', 'Clínica Geral', 'Santo André', 'Av. Atlântica, 892', 'Vila Assunção', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (137, 'Instituto de Ginecologia de Santo André', 'Ginecologia', 'Santo André', 'Av. Industrial, 939', 'Vila Assunção', 'SP', 'Sudeste', 35, 22, '07h-19h', 'Saúde da mulher em todas as fases: consultas, exames preventivos, gestação de alto risco, planejamento familiar e cirurgias ginecológicas.'),
# MAGIC   (138, 'Hospital Universitário de Ribeirão Preto', 'Clínica Geral', 'Ribeirão Preto', 'Av. Independência, 986', 'Centro', 'SP', 'Sudeste', 80, 50, '24h', 'Atendimento clínico geral, check-ups, consultas preventivas, atendimento de urgência ambulatorial e encaminhamento para especialistas.'),
# MAGIC   (139, 'Instituto de Dermatologia de Sorocaba', 'Dermatologia', 'Sorocaba', 'Av. Antônio Carlos Comitre, 1033', 'Campolim', 'SP', 'Sudeste', 5, 12, '07h-19h', 'Dermatologia clínica e estética. Tratamento de acne, psoríase, melanoma, procedimentos estéticos e cirurgias dermatológicas.'),
# MAGIC   (140, 'Hospital Universitário de Niterói', 'Hematologia', 'Niterói', 'R. Visconde de Sepetiba, 1080', 'Santa Rosa', 'RJ', 'Sudeste', 70, 35, '24h', 'Hematologia e oncohematologia: tratamento de anemias, leucemias, linfomas, distúrbios de coagulação e transplante de medula.');

# COMMAND ----------

# DBTITLE 1,Validar hospitais
# MAGIC %sql
# MAGIC SELECT * FROM hospitais ORDER BY id_hospital LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabela `pacientes` — enriquecida com PII e demografia
# MAGIC
# MAGIC **Objetivo:** Gerar **1500 pacientes sintéticos** (500 com distribuição etária ampla + 1000 com viés de juventude) com dados completos para demos de IA, BI e governança LGPD.
# MAGIC
# MAGIC **Schema enriquecido — 12 colunas:**
# MAGIC
# MAGIC | Categoria | Colunas |
# MAGIC |-----------|---------|
# MAGIC | **Identificação** | `id_paciente`, `nome_paciente` |
# MAGIC | **Demografia** | `data_nascimento`, `idade`, `sexo`, `estado_civil` |
# MAGIC | **PII / LGPD** | `cpf`, `cns`, `rg`, `email`, `telefone` |
# MAGIC | **Comercial** | `convenio` |
# MAGIC
# MAGIC **Estratégia SQL declarativa (geração 100% determinística):**
# MAGIC - `sequence(101, 600)` + `explode()` → 500 IDs sequenciais
# MAGIC - Pool de **30 nomes + 20 sobrenomes** BR → 600 combinações possíveis (recicladas via `pmod` para 1500 pacientes)
# MAGIC - Datas de nascimento variando por ID: IDs 101..600 entre **1940-2015** (uniforme), IDs 601..1600 com **viés de juventude** (70% < 35 anos)
# MAGIC - **CPF, CNS, RG**: formatos oficiais BR mas com dígitos sintéticos (nunca correspondem a pessoas reais)
# MAGIC - **CNS** começa com **700** (prefixo válido para Cartão Nacional de Saúde - SUS)
# MAGIC - **E-mail** derivado de `primeiro_nome.sobrenome{id}@email.com.br` (sem acentos)
# MAGIC - **Telefone** com **14 DDDs brasileiros reais** (11, 21, 31, 41, 51, 61, 71, 81, 85, 62, 92...)
# MAGIC - **Estado civil** e **sexo** com distribuição realista (IBGE)
# MAGIC
# MAGIC > **Casos de uso desbloqueados na demo:**
# MAGIC > - **Demografia em dashboards**: filtros por idade, sexo, estado civil
# MAGIC > - **Governança LGPD**: demonstra masking, column-level security em CPF/CNS/email
# MAGIC > - **AI/BI**: agregações por faixa etária × convênio × hospital
# MAGIC > - **Genie / NL queries**: "pacientes do SUS acima de 60 anos"
# MAGIC
# MAGIC > **Resultado esperado:** Tabela `healthcare_lakehouse.bronze.pacientes` com **1500 registros** × **12 colunas** — 500 da população geral + 1000 com perfil mais jovem (target: 70% < 35 anos).

# COMMAND ----------

# DBTITLE 1,Criar e popular bronze.pacientes
# MAGIC %sql
# MAGIC -- Schema enriquecido: identificação + demografia + PII (LGPD)
# MAGIC CREATE OR REPLACE TABLE pacientes (
# MAGIC   id_paciente     INT     NOT NULL COMMENT 'Chave primária — ID único do paciente',
# MAGIC   nome_paciente   STRING  NOT NULL COMMENT 'Nome completo do paciente',
# MAGIC   data_nascimento DATE    NOT NULL COMMENT 'Data de nascimento — base para idade e segmentação',
# MAGIC   idade           INT     NOT NULL COMMENT 'Idade calculada (atualiza com current_date)',
# MAGIC   sexo            STRING  NOT NULL COMMENT 'Sexo biológico (M/F)',
# MAGIC   estado_civil    STRING  NOT NULL COMMENT 'Solteiro(a) / Casado(a) / Divorciado(a) / Viúvo(a)',
# MAGIC   cpf             STRING  NOT NULL COMMENT 'CPF formatado XXX.XXX.XXX-XX (PII - LGPD)',
# MAGIC   cns             STRING  NOT NULL COMMENT 'Cartão Nacional de Saúde (PII - LGPD)',
# MAGIC   rg              STRING  NOT NULL COMMENT 'RG formatado XX.XXX.XXX-X (PII - LGPD)',
# MAGIC   email           STRING  NOT NULL COMMENT 'E-mail de contato (PII - LGPD)',
# MAGIC   telefone        STRING  NOT NULL COMMENT 'Telefone celular formatado (PII - LGPD)',
# MAGIC   convenio        STRING  NOT NULL COMMENT 'Plano de saúde / convênio'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Cadastro base de pacientes — entidade (LGPD: contém PII — CPF, CNS, RG, email, telefone)';
# MAGIC
# MAGIC -- 1500 pacientes gerados em SQL puro com PII fake mas formatado validamente
# MAGIC -- Todos os campos PII (CPF, CNS, RG, email, telefone) são SINTÉTICOS — não correspondem a pessoas reais
# MAGIC -- Mas seguem padrões oficiais BR (formato, comprimento, prefixos válidos)
# MAGIC INSERT INTO pacientes
# MAGIC WITH
# MAGIC   -- Pool de 30 nomes brasileiros (50% masculinos, 50% femininos — alternados)
# MAGIC   nomes AS (
# MAGIC     SELECT explode(array(
# MAGIC       'João','Maria','José','Ana','Pedro','Paula','Lucas','Mariana',
# MAGIC       'Carlos','Juliana','Rafael','Camila','Bruno','Fernanda','Felipe','Beatriz',
# MAGIC       'Gustavo','Larissa','Tiago','Patrícia','André','Aline','Marcos','Sabrina',
# MAGIC       'Rodrigo','Letícia','Diego','Renata','Eduardo','Cláudia'
# MAGIC     )) AS nome
# MAGIC   ),
# MAGIC   sobrenomes AS (
# MAGIC     SELECT explode(array(
# MAGIC       'Silva','Souza','Oliveira','Pereira','Costa','Rodrigues','Almeida','Lima',
# MAGIC       'Gomes','Ribeiro','Carvalho','Martins','Araújo','Barbosa','Mendes','Cardoso',
# MAGIC       'Nascimento','Rocha','Cavalcanti','Moreira'
# MAGIC     )) AS sobrenome
# MAGIC   ),
# MAGIC   nomes_idx      AS (SELECT nome,      row_number() OVER (ORDER BY nome)      - 1 AS idx FROM nomes),
# MAGIC   sobrenomes_idx AS (SELECT sobrenome, row_number() OVER (ORDER BY sobrenome) - 1 AS idx FROM sobrenomes),
# MAGIC   -- 1500 pacientes ao total:
# MAGIC   --   IDs 101..600  (500 originais) — distribuição etária ampla (1940..~2015)
# MAGIC   --   IDs 601..1600 (1000 novos)    — VIÉS DE JUVENTUDE: 70% < 35 anos
# MAGIC   ids AS (SELECT explode(sequence(101, 1600)) AS id_paciente),
# MAGIC   pacientes_base AS (
# MAGIC     SELECT
# MAGIC       i.id_paciente,
# MAGIC       n.nome      AS primeiro_nome,
# MAGIC       s.sobrenome AS sobrenome,
# MAGIC       CASE
# MAGIC         -- IDs 101..600: fórmula original (distribuição uniforme 1940..~2015)
# MAGIC         WHEN i.id_paciente <= 600 THEN
# MAGIC           date_add('1940-01-01', pmod(i.id_paciente * 73, 27000))
# MAGIC         -- IDs 601..1600: viés de juventude por faixa
# MAGIC         --   bucket < 20 → 0-17 anos (20%)
# MAGIC         --   bucket < 70 → 18-34 anos (50%)  ← 70% < 35 acumulado
# MAGIC         --   bucket < 85 → 35-49 anos (15%)
# MAGIC         --   bucket < 95 → 50-64 anos (10%)
# MAGIC         --   else        → 65+   anos ( 5%)
# MAGIC         ELSE date_sub(current_date(),
# MAGIC           CASE
# MAGIC             WHEN pmod(i.id_paciente * 73, 100) < 20 THEN CAST(pmod(i.id_paciente * 41, 17 * 365) + 365 AS INT)
# MAGIC             WHEN pmod(i.id_paciente * 73, 100) < 70 THEN CAST(18 * 365 + pmod(i.id_paciente * 41, 17 * 365) AS INT)
# MAGIC             WHEN pmod(i.id_paciente * 73, 100) < 85 THEN CAST(35 * 365 + pmod(i.id_paciente * 41, 15 * 365) AS INT)
# MAGIC             WHEN pmod(i.id_paciente * 73, 100) < 95 THEN CAST(50 * 365 + pmod(i.id_paciente * 41, 15 * 365) AS INT)
# MAGIC             ELSE CAST(65 * 365 + pmod(i.id_paciente * 41, 20 * 365) AS INT)
# MAGIC           END
# MAGIC         )
# MAGIC       END AS data_nascimento
# MAGIC     FROM ids i
# MAGIC     LEFT JOIN nomes_idx      n ON n.idx = pmod(i.id_paciente * 31, (SELECT count(*) FROM nomes_idx))
# MAGIC     LEFT JOIN sobrenomes_idx s ON s.idx = pmod(i.id_paciente * 17, (SELECT count(*) FROM sobrenomes_idx))
# MAGIC   )
# MAGIC SELECT
# MAGIC   id_paciente,
# MAGIC   concat(primeiro_nome, ' ', sobrenome) AS nome_paciente,
# MAGIC   -- Demografia
# MAGIC   data_nascimento,
# MAGIC   CAST(floor(datediff(current_date(), data_nascimento) / 365.25) AS INT) AS idade,
# MAGIC   -- Sexo via pmod do ID (50/50)
# MAGIC   CASE WHEN pmod(id_paciente * 11, 2) = 0 THEN 'M' ELSE 'F' END AS sexo,
# MAGIC   -- Estado civil com distribuição realista BR (IBGE)
# MAGIC   CASE
# MAGIC     WHEN pmod(id_paciente * 19, 100) < 35 THEN 'Solteiro(a)'
# MAGIC     WHEN pmod(id_paciente * 19, 100) < 85 THEN 'Casado(a)'
# MAGIC     WHEN pmod(id_paciente * 19, 100) < 95 THEN 'Divorciado(a)'
# MAGIC     ELSE 'Viúvo(a)'
# MAGIC   END AS estado_civil,
# MAGIC   -- PII — CPF formatado XXX.XXX.XXX-XX (11 dígitos sintéticos)
# MAGIC   concat(
# MAGIC     lpad(CAST(pmod(id_paciente * 137, 1000) AS STRING), 3, '0'), '.',
# MAGIC     lpad(CAST(pmod(id_paciente * 211, 1000) AS STRING), 3, '0'), '.',
# MAGIC     lpad(CAST(pmod(id_paciente * 313, 1000) AS STRING), 3, '0'), '-',
# MAGIC     lpad(CAST(pmod(id_paciente * 53,  100)  AS STRING), 2, '0')
# MAGIC   ) AS cpf,
# MAGIC   -- PII — CNS (Cartão Nacional de Saúde) com prefixo 700 (formato oficial SUS)
# MAGIC   concat('700 ',
# MAGIC     lpad(CAST(pmod(id_paciente * 167, 10000) AS STRING), 4, '0'), ' ',
# MAGIC     lpad(CAST(pmod(id_paciente * 251, 10000) AS STRING), 4, '0'), ' ',
# MAGIC     lpad(CAST(pmod(id_paciente * 379, 1000)  AS STRING), 3, '0')
# MAGIC   ) AS cns,
# MAGIC   -- PII — RG formatado XX.XXX.XXX-X
# MAGIC   concat(
# MAGIC     lpad(CAST(pmod(id_paciente * 89,  100)  AS STRING), 2, '0'), '.',
# MAGIC     lpad(CAST(pmod(id_paciente * 127, 1000) AS STRING), 3, '0'), '.',
# MAGIC     lpad(CAST(pmod(id_paciente * 181, 1000) AS STRING), 3, '0'), '-',
# MAGIC     CAST(pmod(id_paciente * 7, 10) AS STRING)
# MAGIC   ) AS rg,
# MAGIC   -- PII — E-mail derivado do nome + ID
# MAGIC   lower(concat(
# MAGIC     translate(primeiro_nome, 'áéíóúâêôãõçÁÉÍÓÚÂÊÔÃÕÇ', 'aeiouaeoaocAEIOUAEOAOC'), '.',
# MAGIC     translate(sobrenome,     'áéíóúâêôãõçÁÉÍÓÚÂÊÔÃÕÇ', 'aeiouaeoaocAEIOUAEOAOC'),
# MAGIC     CAST(id_paciente AS STRING), '@email.com.br'
# MAGIC   )) AS email,
# MAGIC   -- PII — Telefone (DDD)9XXXX-XXXX com DDDs reais brasileiros
# MAGIC   concat('(',
# MAGIC     element_at(array('11','11','11','21','21','31','41','51','61','71','81','85','62','92'),
# MAGIC               pmod(id_paciente * 41, 14) + 1),
# MAGIC     ') 9',
# MAGIC     lpad(CAST(pmod(id_paciente * 233, 10000) AS STRING), 4, '0'), '-',
# MAGIC     lpad(CAST(pmod(id_paciente * 317, 10000) AS STRING), 4, '0')
# MAGIC   ) AS telefone,
# MAGIC   -- Convênio: distribuição realista BR (SUS ~60%)
# MAGIC   CASE
# MAGIC     WHEN pmod(id_paciente * 13, 100) < 60 THEN 'SUS'
# MAGIC     WHEN pmod(id_paciente * 13, 100) < 75 THEN 'Unimed'
# MAGIC     WHEN pmod(id_paciente * 13, 100) < 87 THEN 'Bradesco Saúde'
# MAGIC     WHEN pmod(id_paciente * 13, 100) < 95 THEN 'Hapvida NotreDame Intermédica'
# MAGIC     ELSE 'Amil'
# MAGIC   END AS convenio
# MAGIC FROM pacientes_base;

# COMMAND ----------

# DBTITLE 1,Validar pacientes
# MAGIC %sql
# MAGIC -- Amostra dos 10 primeiros pacientes
# MAGIC SELECT * FROM pacientes ORDER BY id_paciente LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificando a fundação Bronze
# MAGIC
# MAGIC Vamos listar as tabelas criadas no schema `bronze` para confirmar nosso ponto de partida.

# COMMAND ----------

# DBTITLE 1,Listar tabelas do schema bronze
# MAGIC %sql
# MAGIC SHOW TABLES IN bronze;

# COMMAND ----------

# DBTITLE 1,Inspecionar metadata (governança)
# MAGIC %sql
# MAGIC -- Inspecionar metadata da tabela hospitais (lineage, comentários, propriedades)
# MAGIC DESCRIBE TABLE EXTENDED hospitais;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PARTE 2 — Landing Zone (Volume) com Dados Brutos
# MAGIC
# MAGIC ## O conceito de Landing Zone
# MAGIC
# MAGIC A **Landing Zone** é a área onde arquivos brutos (CSVs, JSONs, Parquet)
# MAGIC chegam **antes** de virarem tabelas Delta. É a "porta de entrada" do Lakehouse.
# MAGIC
# MAGIC ### Por que usar Unity Catalog Volumes?
# MAGIC - **Governança nativa**: mesmo controle de acesso das tabelas (GRANT/REVOKE)
# MAGIC - **Lineage**: o UC rastreia quem leu qual arquivo
# MAGIC - **Substitui DBFS**: Volumes são o caminho recomendado para arquivos no UC
# MAGIC - **Integração com Lakeflow Connect / Designer**: pipelines apontam direto para o Volume
# MAGIC
# MAGIC ## O que vamos fazer aqui (hands-on)
# MAGIC
# MAGIC 1. **Criar** o Volume `healthcare_lakehouse.bronze.landing_zone`
# MAGIC 2. **Gerar** o arquivo `consultas/` no Volume (1000 consultas, 100 com paciente nulo)
# MAGIC 3. **Gerar** o arquivo `avaliacoes/` no Volume (500 avaliações com textos BR realistas)
# MAGIC 4. **Inspecionar** os arquivos gerados (amostra + quality check)
# MAGIC
# MAGIC > Usamos `INSERT OVERWRITE DIRECTORY ... USING CSV` — comando SQL nativo do Spark
# MAGIC > que escreve queries direto como arquivos CSV no Volume. Em SQL Warehouse,
# MAGIC > o resultado é uma **pasta** contendo `part-XXXXX.csv` (o padrão Spark distribuído).
# MAGIC > O Lakeflow Designer aponta para a **pasta** e lê o conteúdo automaticamente.

# COMMAND ----------

# DBTITLE 1,Criar Volume landing_zone
# MAGIC %sql
# MAGIC -- Volume gerenciado para arquivos CSV brutos
# MAGIC CREATE VOLUME IF NOT EXISTS healthcare_lakehouse.bronze.landing_zone
# MAGIC   COMMENT 'Landing zone for raw CSV files (Lakeflow Designer source)';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gerando `consultas/` no Volume
# MAGIC
# MAGIC **Lógica de geração (1000 consultas, IDs 1001..2000) — 12 colunas:**
# MAGIC
# MAGIC | Categoria | Campos | Distribuição |
# MAGIC |-----------|--------|-------------|
# MAGIC | **FK** | `id_paciente`, `id_hospital` | 90% pacientes válidos + 10% NULL; hospitais ponderados por cidade |
# MAGIC | **Médico** | `id_medico`, `nome_medico`, `crm` | 100 médicos sintéticos (IDs 5000..5099) |
# MAGIC | **Modalidade** | `modalidade` | 60% presencial, 30% retorno, 10% telemedicina |
# MAGIC | **Status** | `status`, `tempo_espera_minutos` | 85% finalizada, 10% cancelada, 5% em_andamento |
# MAGIC | **Clínico** | `cid_10`, `diagnostico_descricao`, `procedimento_realizado` | CIDs reais por especialidade (preenchidos só em consultas finalizadas) |
# MAGIC
# MAGIC **CIDs por especialidade** (alinhamento com a tabela `hospitais`):
# MAGIC - Cardiologia → I10 (HAS), I25 (isquêmica), I50 (insuficiência)
# MAGIC - Ortopedia → M54 (lombalgia), M17 (gonartrose), S82 (fratura)
# MAGIC - Pediatria → J06 (IVAS), A09 (gastroenterite), Z00 (rotina)
# MAGIC - Endocrinologia → E11 (DM2), E03 (hipotireoidismo), E66 (obesidade)
# MAGIC - ... +30 outros CIDs OMS reais
# MAGIC
# MAGIC Tudo **determinístico** (`pmod` com primos diferentes) — mesma execução, mesmos dados.
# MAGIC
# MAGIC > **Demos desbloqueadas:** análise de produtividade médica (consultas/médico), ratio de telemedicina,
# MAGIC > top CIDs por especialidade, vector search em `diagnostico_descricao`, classificação clínica com `ai_extract`.

# COMMAND ----------

# DBTITLE 1,Gerar consultas.csv no Volume
# MAGIC %sql
# MAGIC -- Materializa 1000 consultas direto como CSV no Volume
# MAGIC INSERT OVERWRITE DIRECTORY '/Volumes/healthcare_lakehouse/bronze/landing_zone/consultas'
# MAGIC   USING CSV
# MAGIC   OPTIONS ('header' 'true')
# MAGIC WITH
# MAGIC   ids AS (SELECT explode(sequence(1001, 2000)) AS id_consulta),
# MAGIC   -- Pesos por hospital alinhados às cidades (SP/RJ pesam mais)
# MAGIC   hospitais_pesos AS (
# MAGIC     SELECT id_hospital, peso,
# MAGIC            sum(peso) OVER (ORDER BY id_hospital) - peso AS limite_inf,
# MAGIC            sum(peso) OVER (ORDER BY id_hospital)        AS limite_sup
# MAGIC     FROM VALUES
# MAGIC       (  1, 25), (  2, 25), (  3, 25), (  4, 25), (  5, 25), (  6, 25), (  7, 25),
# MAGIC       (  8, 25), (  9, 18), ( 10, 18), ( 11, 18), ( 12, 18), ( 13, 18), ( 14, 12),
# MAGIC       ( 15, 12), ( 16, 12), ( 17, 10), ( 18, 10), ( 19, 7), ( 20, 7), ( 21, 6),
# MAGIC       ( 22, 6), ( 23, 3), ( 24, 5), ( 25, 5), ( 26, 4), ( 27, 4), ( 28, 4),
# MAGIC       ( 29, 4), ( 30, 2), ( 31, 2), ( 32, 1), ( 33, 1), ( 34, 1), ( 35, 25),
# MAGIC       ( 36, 25), ( 37, 18), ( 38, 5), ( 39, 4), ( 40, 7), ( 41, 25), ( 42, 25),
# MAGIC       ( 43, 25), ( 44, 25), ( 45, 25), ( 46, 25), ( 47, 25), ( 48, 25), ( 49, 25),
# MAGIC       ( 50, 25), ( 51, 25), ( 52, 25), ( 53, 25), ( 54, 25), ( 55, 25), ( 56, 25),
# MAGIC       ( 57, 25), ( 58, 25), ( 59, 25), ( 60, 25), ( 61, 18), ( 62, 18), ( 63, 18),
# MAGIC       ( 64, 18), ( 65, 18), ( 66, 18), ( 67, 18), ( 68, 18), ( 69, 18), ( 70, 18),
# MAGIC       ( 71, 18), ( 72, 18), ( 73, 12), ( 74, 12), ( 75, 12), ( 76, 12), ( 77, 12),
# MAGIC       ( 78, 10), ( 79, 10), ( 80, 10), ( 81, 10), ( 82, 7), ( 83, 7), ( 84, 7),
# MAGIC       ( 85, 6), ( 86, 6), ( 87, 6), ( 88, 6), ( 89, 5), ( 90, 5), ( 91, 5),
# MAGIC       ( 92, 5), ( 93, 4), ( 94, 4), ( 95, 4), ( 96, 4), ( 97, 4), ( 98, 4),
# MAGIC       ( 99, 4), (100, 4), (101, 3), (102, 3), (103, 2), (104, 2), (105, 2),
# MAGIC       (106, 2), (107, 1), (108, 1), (109, 1), (110, 1), (111, 1), (112, 3),
# MAGIC       (113, 3), (114, 3), (115, 2), (116, 2), (117, 2), (118, 2), (119, 2),
# MAGIC       (120, 2), (121, 2), (122, 2), (123, 2), (124, 2), (125, 2), (126, 2),
# MAGIC       (127, 2), (128, 2), (129, 2), (130, 1), (131, 1), (132, 1), (133, 1),
# MAGIC       (134, 2), (135, 2), (136, 2), (137, 2), (138, 1), (139, 1), (140, 1)
# MAGIC     AS t(id_hospital, peso)
# MAGIC   ),
# MAGIC   total_peso AS (SELECT max(limite_sup) AS total FROM hospitais_pesos),
# MAGIC   -- Mapeamento CID-10 por especialidade (códigos reais OMS)
# MAGIC   cids AS (
# MAGIC     SELECT * FROM VALUES
# MAGIC       ('Cardiologia',          'I10', 'Hipertensão arterial sistêmica'),
# MAGIC       ('Cardiologia',          'I25', 'Doença isquêmica crônica do coração'),
# MAGIC       ('Cardiologia',          'I50', 'Insuficiência cardíaca'),
# MAGIC       ('Ortopedia',            'M54', 'Dorsalgia / dor lombar'),
# MAGIC       ('Ortopedia',            'M17', 'Gonartrose (artrose do joelho)'),
# MAGIC       ('Ortopedia',            'S82', 'Fratura da perna, incluindo tornozelo'),
# MAGIC       ('Pediatria',            'J06', 'Infecção aguda das vias aéreas superiores'),
# MAGIC       ('Pediatria',            'A09', 'Diarreia e gastroenterite de origem infecciosa'),
# MAGIC       ('Pediatria',            'Z00', 'Consulta pediátrica de rotina'),
# MAGIC       ('Ginecologia',          'N94', 'Dor pélvica e dismenorreia'),
# MAGIC       ('Ginecologia',          'O80', 'Parto único espontâneo'),
# MAGIC       ('Ginecologia',          'N76', 'Vaginite e vulvite agudas'),
# MAGIC       ('Dermatologia',         'L70', 'Acne'),
# MAGIC       ('Dermatologia',         'L40', 'Psoríase'),
# MAGIC       ('Dermatologia',         'C44', 'Neoplasia maligna da pele (não melanoma)'),
# MAGIC       ('Oftalmologia',         'H52', 'Erros de refração'),
# MAGIC       ('Oftalmologia',         'H25', 'Catarata senil'),
# MAGIC       ('Urologia',             'N40', 'Hiperplasia da próstata'),
# MAGIC       ('Urologia',             'N20', 'Cálculo renal e ureteral'),
# MAGIC       ('Endocrinologia',       'E11', 'Diabetes mellitus tipo 2'),
# MAGIC       ('Endocrinologia',       'E03', 'Hipotireoidismo'),
# MAGIC       ('Endocrinologia',       'E66', 'Obesidade'),
# MAGIC       ('Pneumologia',          'J45', 'Asma'),
# MAGIC       ('Pneumologia',          'J44', 'DPOC (doença pulmonar obstrutiva crônica)'),
# MAGIC       ('Neurologia',           'G43', 'Enxaqueca'),
# MAGIC       ('Neurologia',           'G40', 'Epilepsia'),
# MAGIC       ('Reumatologia',         'M05', 'Artrite reumatoide'),
# MAGIC       ('Reumatologia',         'M32', 'Lúpus eritematoso sistêmico'),
# MAGIC       ('Psiquiatria',          'F32', 'Episódio depressivo'),
# MAGIC       ('Psiquiatria',          'F41', 'Transtorno de ansiedade generalizada'),
# MAGIC       ('Gastroenterologia',    'K29', 'Gastrite e duodenite'),
# MAGIC       ('Gastroenterologia',    'K21', 'Refluxo gastroesofágico'),
# MAGIC       ('Otorrinolaringologia', 'J31', 'Rinite, faringite e nasofaringite crônicas'),
# MAGIC       ('Otorrinolaringologia', 'H66', 'Otite média'),
# MAGIC       ('Nefrologia',           'N18', 'Doença renal crônica'),
# MAGIC       ('Nefrologia',           'N17', 'Insuficiência renal aguda'),
# MAGIC       ('Hematologia',          'D50', 'Anemia por deficiência de ferro'),
# MAGIC       ('Hematologia',          'C91', 'Leucemia linfoide'),
# MAGIC       ('Oncologia',            'C50', 'Neoplasia maligna da mama'),
# MAGIC       ('Oncologia',            'C18', 'Neoplasia maligna do cólon'),
# MAGIC       ('Clínica Geral',        'Z00', 'Exame geral e investigação de pessoas sem queixa'),
# MAGIC       ('Clínica Geral',        'R51', 'Cefaleia'),
# MAGIC       ('Clínica Geral',        'R10', 'Dor abdominal'),
# MAGIC       ('Clínica Geral',        'B34', 'Doença viral não especificada')
# MAGIC     AS t(especialidade, cid_10, diagnostico_descricao)
# MAGIC   ),
# MAGIC   -- Numera CIDs dentro de cada especialidade
# MAGIC   cids_idx AS (
# MAGIC     SELECT especialidade, cid_10, diagnostico_descricao,
# MAGIC            row_number() OVER (PARTITION BY especialidade ORDER BY cid_10) - 1 AS idx,
# MAGIC            count(*) OVER (PARTITION BY especialidade) AS qtd
# MAGIC     FROM cids
# MAGIC   )
# MAGIC SELECT
# MAGIC   i.id_consulta,
# MAGIC   -- 10% nulos: pmod * 7 % 10 == 0
# MAGIC   CASE WHEN pmod(i.id_consulta * 7, 10) = 0
# MAGIC        THEN CAST(NULL AS INT)
# MAGIC        ELSE 101 + pmod(i.id_consulta * 31, 1500)
# MAGIC   END AS id_paciente,
# MAGIC   -- Hospital via pesos (sorteia 0..total e busca range correspondente)
# MAGIC   cp.id_hospital,
# MAGIC   -- Médico: ID 5000..5099 (100 médicos no total), determinístico
# MAGIC   5000 + pmod(i.id_consulta * 29, 100) AS id_medico,
# MAGIC   -- Nome do médico (Dr./Dra. + nome + sobrenome)
# MAGIC   concat(
# MAGIC     CASE WHEN pmod(i.id_consulta * 29, 2) = 0 THEN 'Dr. ' ELSE 'Dra. ' END,
# MAGIC     element_at(array('Carlos','Ana','Pedro','Lucia','Marcos','Patricia','Roberto','Camila','João','Beatriz','Rafael','Mariana','Eduardo','Juliana','Felipe','Cláudia'),
# MAGIC                pmod(i.id_consulta * 29, 16) + 1),
# MAGIC     ' ',
# MAGIC     element_at(array('Silva','Souza','Oliveira','Costa','Almeida','Rodrigues','Pereira','Lima','Gomes','Carvalho'),
# MAGIC                pmod(i.id_consulta * 47, 10) + 1)
# MAGIC   ) AS nome_medico,
# MAGIC   -- CRM formatado (CRM/UF + 6 dígitos)
# MAGIC   concat('CRM/',
# MAGIC     element_at(array('SP','RJ','MG','RS','PR','BA','PE','CE','DF','SC'), pmod(i.id_consulta * 29, 10) + 1),
# MAGIC     ' ', lpad(CAST(100000 + pmod(i.id_consulta * 379, 800000) AS STRING), 6, '0')
# MAGIC   ) AS crm,
# MAGIC   -- Modalidade: 60% presencial, 30% retorno, 10% telemedicina
# MAGIC   CASE
# MAGIC     WHEN pmod(i.id_consulta * 53, 100) < 60 THEN 'presencial'
# MAGIC     WHEN pmod(i.id_consulta * 53, 100) < 90 THEN 'retorno'
# MAGIC     ELSE 'telemedicina'
# MAGIC   END AS modalidade,
# MAGIC   -- Status com distribuição realista
# MAGIC   CASE
# MAGIC     WHEN pmod(i.id_consulta * 11, 100) < 85 THEN 'finalizada'
# MAGIC     WHEN pmod(i.id_consulta * 11, 100) < 95 THEN 'cancelada'
# MAGIC     ELSE 'em_andamento'
# MAGIC   END AS status,
# MAGIC   -- Tempo de espera: 0 se cancelada, 0..120 min senão
# MAGIC   CASE
# MAGIC     WHEN pmod(i.id_consulta * 11, 100) >= 85
# MAGIC      AND pmod(i.id_consulta * 11, 100) <  95 THEN 0
# MAGIC     ELSE pmod(i.id_consulta * 23, 90) + pmod(i.id_consulta * 19, 30)
# MAGIC   END AS tempo_espera_minutos,
# MAGIC   -- CID-10 e diagnóstico: só preenche para consultas FINALIZADAS, baseado na ESPECIALIDADE do hospital
# MAGIC   CASE WHEN pmod(i.id_consulta * 11, 100) < 85 THEN c.cid_10 ELSE NULL END AS cid_10,
# MAGIC   CASE WHEN pmod(i.id_consulta * 11, 100) < 85 THEN c.diagnostico_descricao ELSE NULL END AS diagnostico_descricao,
# MAGIC   -- Procedimento realizado (baseado em modalidade + status)
# MAGIC   CASE
# MAGIC     WHEN pmod(i.id_consulta * 11, 100) >= 85 AND pmod(i.id_consulta * 11, 100) < 95 THEN NULL
# MAGIC     WHEN pmod(i.id_consulta * 53, 100) >= 60 AND pmod(i.id_consulta * 53, 100) < 90 THEN 'Consulta de retorno'
# MAGIC     WHEN pmod(i.id_consulta * 53, 100) >= 90 THEN 'Consulta por telemedicina'
# MAGIC     WHEN pmod(i.id_consulta * 83, 10) < 6 THEN 'Consulta de rotina'
# MAGIC     WHEN pmod(i.id_consulta * 83, 10) < 8 THEN 'Exame complementar solicitado'
# MAGIC     ELSE 'Procedimento ambulatorial realizado'
# MAGIC   END AS procedimento_realizado
# MAGIC FROM ids i
# MAGIC CROSS JOIN total_peso tp
# MAGIC JOIN hospitais_pesos cp
# MAGIC   ON pmod(i.id_consulta * 17, tp.total) >= cp.limite_inf
# MAGIC  AND pmod(i.id_consulta * 17, tp.total) <  cp.limite_sup
# MAGIC -- JOIN com tabela hospitais (já criada na Parte 1) para puxar a especialidade
# MAGIC JOIN healthcare_lakehouse.bronze.hospitais h ON h.id_hospital = cp.id_hospital
# MAGIC -- JOIN com pool de CIDs filtrado pela especialidade do hospital
# MAGIC LEFT JOIN cids_idx c
# MAGIC   ON c.especialidade = h.especialidade
# MAGIC  AND c.idx = pmod(i.id_consulta * 67, c.qtd);

# COMMAND ----------

# DBTITLE 1,Listar arquivos do diretório consultas/
# MAGIC %sql
# MAGIC -- Cada INSERT OVERWRITE DIRECTORY produz part-XXXXX.csv dentro da pasta
# MAGIC LIST '/Volumes/healthcare_lakehouse/bronze/landing_zone/consultas/';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecionando `consultas/`
# MAGIC
# MAGIC **Estrutura final do CSV:**
# MAGIC ```
# MAGIC id_consulta, id_paciente, id_hospital, status, tempo_espera_minutos
# MAGIC 1001, 132, 5, "finalizada", 102
# MAGIC 1002, 163, 6, "finalizada", 24
# MAGIC ...
# MAGIC 1010, , 8, "finalizada", 56    ← paciente nulo (1 a cada 10)
# MAGIC ```
# MAGIC
# MAGIC **Problemas de qualidade propositais** (para a demo de Data Quality no Lakeflow):
# MAGIC - **100 linhas com `id_paciente` nulo** (10%) — quebra integridade referencial
# MAGIC - **~10% canceladas** — não devem entrar em métricas de atendimento
# MAGIC - **~5% em andamento** — em produção, exigiria filtro especial
# MAGIC
# MAGIC O Lakeflow Designer vai cuidar dessa limpeza visualmente, na transição Bronze → Silver.

# COMMAND ----------

# DBTITLE 1,Amostrar consultas (10 linhas)
# MAGIC %sql
# MAGIC -- read_files lê o diretório inteiro (todos os part-XXXXX.csv dentro)
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   '/Volumes/healthcare_lakehouse/bronze/landing_zone/consultas/',
# MAGIC   format => 'csv',
# MAGIC   header => true
# MAGIC )
# MAGIC ORDER BY id_consulta
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Quality check — nulos e distribuição por status
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                       AS total_consultas,
# MAGIC   COUNT(CASE WHEN id_paciente IS NULL THEN 1 END)                AS paciente_nulo,
# MAGIC   COUNT(CASE WHEN status = 'finalizada' THEN 1 END)              AS finalizadas,
# MAGIC   COUNT(CASE WHEN status = 'cancelada' THEN 1 END)               AS canceladas,
# MAGIC   COUNT(CASE WHEN status = 'em_andamento' THEN 1 END)            AS em_andamento,
# MAGIC   ROUND(AVG(CAST(tempo_espera_minutos AS DOUBLE)), 1)            AS tempo_medio_min
# MAGIC FROM read_files(
# MAGIC   '/Volumes/healthcare_lakehouse/bronze/landing_zone/consultas/',
# MAGIC   format => 'csv', header => true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gerando `avaliacoes/` no Volume
# MAGIC
# MAGIC **Lógica de geração (500 avaliações × 7 colunas) — com SINERGIA ao tempo de espera:**
# MAGIC
# MAGIC | Categoria | Campos | Distribuição |
# MAGIC |-----------|--------|-------------|
# MAGIC | **FK** | `id_consulta`, `id_paciente` | JOIN com `consultas/` apenas onde `status = 'finalizada'` |
# MAGIC | **Texto livre** | `texto_avaliacao` | Template BR com **viés por tempo de espera** (ver tabela abaixo) |
# MAGIC | **Temporal** | `data_avaliacao` | 1-90 dias atrás (sazonalidade) |
# MAGIC | **Canal** | `canal_avaliacao` | App 35% / WhatsApp 30% / SMS 15% / Site 12% / Quiosque 8% |
# MAGIC | **Identificação** | `avaliacao_anonima`, `tempo_para_avaliar_min` | 15% anônimas; tempo segue cauda longa (20% < 2h, 40% 2-24h, 30% 1-5d, 10% 5-7d) |
# MAGIC
# MAGIC **Viés do sentimento por tempo de espera** (regra de negócio crítica):
# MAGIC
# MAGIC | Faixa tempo_espera | % positivo | % negativo | % neutro | Insight de negócio |
# MAGIC |--------------------|-----------:|-----------:|---------:|-------------------|
# MAGIC | **< 20 min**       | 75%        | 10%        | 15%      | Atendimento rápido → satisfação alta |
# MAGIC | **20-30 min**      | 55%        | 25%        | 20%      | Tolerável, mas avaliações mistas |
# MAGIC | **> 30 min**       | **15%**    | **70%**    | 15%      | **Frustração predominante** (regra de negócio!) |
# MAGIC
# MAGIC Cada sentimento puxa de um **pool de templates BR** (25 positivos, 25 negativos, 15 neutros).
# MAGIC
# MAGIC **Por que isso importa para a demo de IA/BI:**
# MAGIC - Dashboards Gold vão mostrar **correlação real** entre operação (tempo) e patient experience
# MAGIC - `ai_analyze_sentiment()` vai validar a hipótese: tempo alto = sentimento baixo
# MAGIC - Análise de canal: qual canal (App/WhatsApp) gera mais feedbacks por hospital?
# MAGIC - Anonimato: avaliações anônimas são mais negativas? (hipótese a ser validada)
# MAGIC - Time-to-feedback: quanto antes o paciente avalia, mais positivo tende a ser?

# COMMAND ----------

# DBTITLE 1,Gerar avaliacoes.csv no Volume
# MAGIC %sql
# MAGIC INSERT OVERWRITE DIRECTORY '/Volumes/healthcare_lakehouse/bronze/landing_zone/avaliacoes'
# MAGIC   USING CSV
# MAGIC   OPTIONS ('header' 'true')
# MAGIC WITH
# MAGIC   -- 25 templates positivos com índice 0-based
# MAGIC   textos_pos AS (
# MAGIC     SELECT texto, row_number() OVER (ORDER BY texto) - 1 AS idx FROM (
# MAGIC       SELECT explode(array(
# MAGIC         'Atendimento muito rápido, médico excelente!',
# MAGIC         'Médica atenciosa e clínica organizada. Recomendo!',
# MAGIC         'Excelente atendimento, fui muito bem recebido na recepção.',
# MAGIC         'Tudo perfeito, do estacionamento ao consultório.',
# MAGIC         'Profissionais qualificados e simpáticos. Voltarei sempre.',
# MAGIC         'Consulta no horário marcado, sem demora. Adorei!',
# MAGIC         'Equipe extremamente competente e gentil.',
# MAGIC         'Médico explicou tudo com paciência, saí bem informado.',
# MAGIC         'Estrutura impecável, atendimento humanizado.',
# MAGIC         'Recomendo a todos, atendimento de primeira!',
# MAGIC         'Muito satisfeito, atendimento rápido e eficiente.',
# MAGIC         'Clínica limpa, organizada e com profissionais ótimos.',
# MAGIC         'Médica especialista muito boa, tirou todas as dúvidas.',
# MAGIC         'Atendimento ágil, pontual e cordial. Nota 10!',
# MAGIC         'Foi rápido e o médico foi muito atencioso comigo.',
# MAGIC         'Saí da consulta com confiança no diagnóstico.',
# MAGIC         'Adorei a recepcionista, super educada e prestativa.',
# MAGIC         'Ambiente acolhedor e equipe maravilhosa.',
# MAGIC         'Médico cuidadoso, marquei retorno sem dúvida.',
# MAGIC         'Atendimento humano e profissional, parabéns!',
# MAGIC         'Excelente experiência do início ao fim.',
# MAGIC         'Pontualidade e cordialidade, muito bom!',
# MAGIC         'Equipe top, recomendo de olhos fechados.',
# MAGIC         'Atendimento exemplar, fui bem cuidado.',
# MAGIC         'Muito boa a consulta, médico paciente e atencioso.'
# MAGIC       )) AS texto
# MAGIC     )
# MAGIC   ),
# MAGIC   -- 25 templates negativos
# MAGIC   textos_neg AS (
# MAGIC     SELECT texto, row_number() OVER (ORDER BY texto) - 1 AS idx FROM (
# MAGIC       SELECT explode(array(
# MAGIC         'A consulta foi boa, mas demorou muito na recepção, um absurdo.',
# MAGIC         'Esperei mais de uma hora para ser atendido, péssimo!',
# MAGIC         'Recepção desorganizada, ninguém sabia informar nada.',
# MAGIC         'Médico apressado, não me deu atenção.',
# MAGIC         'Atendimento ruim, não recomendo esse hospital.',
# MAGIC         'Demora absurda, fiquei esperando duas horas.',
# MAGIC         'Recepcionista mal educada, atendimento horrível.',
# MAGIC         'Sistema fora do ar, tive que esperar muito tempo.',
# MAGIC         'Médica não soube responder minhas perguntas, decepcionante.',
# MAGIC         'Estrutura velha e atendimento lento, não voltarei.',
# MAGIC         'Marquei horário e não fui atendido no tempo previsto.',
# MAGIC         'Consulta corrida, médico mal olhou pra mim.',
# MAGIC         'Péssima experiência, vou procurar outra clínica.',
# MAGIC         'Recepção lotada e desorganizada, demorei demais.',
# MAGIC         'Atendimento frio, médico não fez perguntas direito.',
# MAGIC         'Demora insuportável, atrasou todo o meu dia.',
# MAGIC         'Equipe despreparada, não recomendo.',
# MAGIC         'Fui mal atendido, médico arrogante.',
# MAGIC         'Recepcionista grosseira, atendimento abaixo do esperado.',
# MAGIC         'Esperei muito, médico atendeu rápido demais. Frustrante.',
# MAGIC         'Sistema travou, demorou uma eternidade para liberar.',
# MAGIC         'Pouca paciência do médico, fiquei sem entender o diagnóstico.',
# MAGIC         'Atrasaram minha consulta sem aviso. Horrível.',
# MAGIC         'Atendimento descaso, ninguém estava preocupado.',
# MAGIC         'Não gostei, fila enorme e atendimento ruim.'
# MAGIC       )) AS texto
# MAGIC     )
# MAGIC   ),
# MAGIC   -- 15 templates neutros
# MAGIC   textos_neu AS (
# MAGIC     SELECT texto, row_number() OVER (ORDER BY texto) - 1 AS idx FROM (
# MAGIC       SELECT explode(array(
# MAGIC         'Atendimento dentro do esperado.',
# MAGIC         'Consulta tranquila, sem grandes problemas.',
# MAGIC         'Foi ok, nem bom nem ruim.',
# MAGIC         'Tudo dentro do normal, sem queixas.',
# MAGIC         'Atendimento padrão, comum.',
# MAGIC         'Médico cumpriu o protocolo, sem destaques.',
# MAGIC         'Sem reclamações, mas também sem elogios.',
# MAGIC         'Experiência mediana, atende as necessidades básicas.',
# MAGIC         'Atendimento regular, espera moderada.',
# MAGIC         'Foi tudo bem, dentro do que eu esperava.',
# MAGIC         'Consulta normal, sem novidades.',
# MAGIC         'Atendimento ok, dentro da média.',
# MAGIC         'Tudo certo, mas nada surpreendente.',
# MAGIC         'Sem maiores observações, atendimento regular.',
# MAGIC         'Cumpriu o esperado, atendimento simples.'
# MAGIC       )) AS texto
# MAGIC     )
# MAGIC   ),
# MAGIC   -- Lê consultas finalizadas DO PRÓPRIO CSV gerado anteriormente (já no Volume)
# MAGIC   -- Traz tempo_espera (p/ sinergia sentimento) E id_paciente (FK redundante útil pra joins)
# MAGIC   finalizadas AS (
# MAGIC     SELECT
# MAGIC       CAST(id_consulta AS INT) AS id_consulta,
# MAGIC       CAST(tempo_espera_minutos AS INT) AS tempo_espera,
# MAGIC       CAST(id_paciente AS INT) AS id_paciente,
# MAGIC       row_number() OVER (ORDER BY CAST(id_consulta AS INT)) AS seq
# MAGIC     FROM read_files(
# MAGIC       '/Volumes/healthcare_lakehouse/bronze/landing_zone/consultas/',
# MAGIC       format => 'csv', header => true
# MAGIC     )
# MAGIC     WHERE status = 'finalizada'
# MAGIC   ),
# MAGIC   -- Decora cada consulta finalizada com sentimento ENVIESADO pelo tempo de espera
# MAGIC   --   < 20 min  → 75% positivo / 10% negativo / 15% neutro  (atendimento rápido = satisfação)
# MAGIC   --   20-30 min → 55% positivo / 25% negativo / 20% neutro  (tolerável, mas misto)
# MAGIC   --   > 30 min  → 15% positivo / 70% negativo / 15% neutro  (frustração predominante)
# MAGIC   --   pmod(seq*7, 100) gera um valor 0..99 determinístico para cada consulta
# MAGIC   aval_decorada AS (
# MAGIC     SELECT
# MAGIC       f.id_consulta,
# MAGIC       f.id_paciente,
# MAGIC       f.tempo_espera,
# MAGIC       CASE
# MAGIC         -- Faixa 1: tempo curto (< 20 min) → maioria positivo
# MAGIC         WHEN f.tempo_espera < 20 THEN
# MAGIC           CASE
# MAGIC             WHEN pmod(f.seq * 7, 100) < 75 THEN 'pos'
# MAGIC             WHEN pmod(f.seq * 7, 100) < 85 THEN 'neg'
# MAGIC             ELSE 'neu'
# MAGIC           END
# MAGIC         -- Faixa 2: tempo médio (20-30 min) → misto
# MAGIC         WHEN f.tempo_espera <= 30 THEN
# MAGIC           CASE
# MAGIC             WHEN pmod(f.seq * 7, 100) < 55 THEN 'pos'
# MAGIC             WHEN pmod(f.seq * 7, 100) < 80 THEN 'neg'
# MAGIC             ELSE 'neu'
# MAGIC           END
# MAGIC         -- Faixa 3: tempo alto (> 30 min) → maioria negativo (regra de negócio!)
# MAGIC         ELSE
# MAGIC           CASE
# MAGIC             WHEN pmod(f.seq * 7, 100) < 15 THEN 'pos'
# MAGIC             WHEN pmod(f.seq * 7, 100) < 85 THEN 'neg'
# MAGIC             ELSE 'neu'
# MAGIC           END
# MAGIC       END AS sentimento,
# MAGIC       pmod(f.seq * 13, 25) AS idx_pos,
# MAGIC       pmod(f.seq * 13, 25) AS idx_neg,
# MAGIC       pmod(f.seq * 13, 15) AS idx_neu
# MAGIC     FROM finalizadas f
# MAGIC     WHERE f.seq <= 500     -- pega só as 500 primeiras finalizadas
# MAGIC   )
# MAGIC SELECT
# MAGIC   ad.id_consulta,
# MAGIC   ad.id_paciente,
# MAGIC   -- Texto livre via JOIN com pool correspondente ao sentimento
# MAGIC   COALESCE(p.texto, n.texto, u.texto) AS texto_avaliacao,
# MAGIC   -- Data da avaliação: entre 1 e 90 dias atrás (sazonalidade fake)
# MAGIC   date_sub(current_date(), CAST(pmod(ad.id_consulta * 19, 90) AS INT)) AS data_avaliacao,
# MAGIC   -- Canal de coleta — distribuição realista BR (App lidera, Quiosque minoritário)
# MAGIC   --   App 35% / WhatsApp 30% / SMS 15% / Site 12% / Quiosque 8%
# MAGIC   CASE
# MAGIC     WHEN pmod(ad.id_consulta * 43, 100) < 35 THEN 'App'
# MAGIC     WHEN pmod(ad.id_consulta * 43, 100) < 65 THEN 'WhatsApp'
# MAGIC     WHEN pmod(ad.id_consulta * 43, 100) < 80 THEN 'SMS'
# MAGIC     WHEN pmod(ad.id_consulta * 43, 100) < 92 THEN 'Site'
# MAGIC     ELSE 'Quiosque'
# MAGIC   END AS canal_avaliacao,
# MAGIC   -- 15% das avaliações são anônimas (geralmente as negativas)
# MAGIC   CASE WHEN pmod(ad.id_consulta * 71, 100) < 15 THEN true ELSE false END AS avaliacao_anonima,
# MAGIC   -- Tempo até avaliar (em minutos): cauda longa de 5min a 7 dias (10080 min)
# MAGIC   --   maioria avalia entre 30 min e 2 dias
# MAGIC   CASE
# MAGIC     WHEN pmod(ad.id_consulta * 89, 100) < 20 THEN pmod(ad.id_consulta * 31, 120)   + 5     -- 20%: dentro de 2h (rapidão)
# MAGIC     WHEN pmod(ad.id_consulta * 89, 100) < 60 THEN pmod(ad.id_consulta * 31, 1320)  + 120   -- 40%: 2h-24h
# MAGIC     WHEN pmod(ad.id_consulta * 89, 100) < 90 THEN pmod(ad.id_consulta * 31, 5760)  + 1440  -- 30%: 1-5 dias
# MAGIC     ELSE pmod(ad.id_consulta * 31, 2880) + 7200                                            -- 10%: 5-7 dias (atrasão)
# MAGIC   END AS tempo_para_avaliar_min
# MAGIC FROM aval_decorada ad
# MAGIC LEFT JOIN textos_pos p ON ad.sentimento = 'pos' AND p.idx = ad.idx_pos
# MAGIC LEFT JOIN textos_neg n ON ad.sentimento = 'neg' AND n.idx = ad.idx_neg
# MAGIC LEFT JOIN textos_neu u ON ad.sentimento = 'neu' AND u.idx = ad.idx_neu
# MAGIC ORDER BY ad.id_consulta;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecionando `avaliacoes/`
# MAGIC
# MAGIC **Estrutura final:**
# MAGIC ```
# MAGIC id_consulta, texto_avaliacao
# MAGIC 1001, "Foi rápido e o médico foi muito atencioso comigo."
# MAGIC 1003, "Muito boa a consulta, médico paciente e atencioso."
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC Cada avaliação referencia uma consulta **finalizada** existente (joinable em `id_consulta`).
# MAGIC No Lakeflow Designer, aplicaremos `ai_analyze_sentiment()` para classificar automaticamente
# MAGIC esses textos como positivo / negativo / neutro.

# COMMAND ----------

# DBTITLE 1,Amostrar avaliacoes (mix de sentimentos)
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   '/Volumes/healthcare_lakehouse/bronze/landing_zone/avaliacoes/',
# MAGIC   format => 'csv', header => true
# MAGIC )
# MAGIC ORDER BY id_consulta
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Total de avaliações + estado do Volume
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM read_files(
# MAGIC     '/Volumes/healthcare_lakehouse/bronze/landing_zone/avaliacoes/',
# MAGIC     format => 'csv', header => true)) AS total_avaliacoes,
# MAGIC   (SELECT COUNT(*) FROM read_files(
# MAGIC     '/Volumes/healthcare_lakehouse/bronze/landing_zone/consultas/',
# MAGIC     format => 'csv', header => true))  AS total_consultas;

# COMMAND ----------

# DBTITLE 1,Listar arquivos do landing_zone (estrutura final)
# MAGIC %sql
# MAGIC LIST '/Volumes/healthcare_lakehouse/bronze/landing_zone/';

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Próximos Passos — Lakeflow Designer
# MAGIC
# MAGIC ## O que conseguimos até aqui
# MAGIC
# MAGIC ```
# MAGIC healthcare_lakehouse
# MAGIC ├── bronze
# MAGIC │   ├── hospitais     ← criada (140 registros)
# MAGIC │   ├── pacientes     ← criada (1500 registros)
# MAGIC │   └── landing_zone  ← Volume com consultas.csv + avaliacoes.csv
# MAGIC ├── silver            (vazio — será preenchido pelo Lakeflow)
# MAGIC └── gold              (vazio — será preenchido pelo Lakeflow)
# MAGIC ```
# MAGIC
# MAGIC ## O que vem agora (no Lakeflow Designer)
# MAGIC
# MAGIC Com a fundação pronta, abrimos o **Lakeflow Designer** e construímos visualmente:
# MAGIC
# MAGIC ### Pipeline 1 — Ingestão de Consultas
# MAGIC - Upload do arquivo `consultas.csv`
# MAGIC - Source → CSV file
# MAGIC - Transform → data quality (remover nulos e cancelados)
# MAGIC - Sink → `silver.consultas`
# MAGIC
# MAGIC ### Pipeline 2 — Ingestão de Avaliações
# MAGIC - Upload do arquivo `avaliacoes.csv`
# MAGIC - Source → CSV file
# MAGIC - Sink → `bronze.avaliacoes`
# MAGIC
# MAGIC ### Pipeline 3 — Visão 360 (Joins)
# MAGIC - Join visual: `silver.consultas` + `bronze.pacientes` + `bronze.hospitais`
# MAGIC - Sink → `silver.consultas_360`
# MAGIC
# MAGIC ### Pipeline 4 — IA Generativa
# MAGIC - Adicionar coluna calculada usando `ai_analyze_sentiment(texto_avaliacao)`
# MAGIC - Adicionar coluna calculada usando `ai_extract` para sumarizar e categorizar problemas
# MAGIC - Sink → `silver.consultas_avaliadas`
# MAGIC
# MAGIC ### Pipeline 5 — KPIs Gold
# MAGIC - Agregações por hospital (volume, tempo médio de espera, sentimento médio)
# MAGIC - Sink → `gold.kpis_clinica`
# MAGIC
# MAGIC ## Por que essa abordagem híbrida?
# MAGIC
# MAGIC | Notebook SQL (este) | Lakeflow Designer (próximos passos) |
# MAGIC |--------------------|--------------------------------------|
# MAGIC | Setup governado (catalog, schemas) | Pipelines visuais |
# MAGIC | Cadastros base (estáveis)  | Fatos transacionais |
# MAGIC | SQL versionável | Self-service para analistas |
# MAGIC | Reprodutível em qualquer ambiente | Drag-and-drop, fácil de demonstrar |
# MAGIC
# MAGIC > **Mensagem-chave:** O Lakehouse não é só código — é também uma **plataforma visual**.
# MAGIC > O Lakeflow Designer permite que **engenheiros, analistas e até gestores** construam pipelines
# MAGIC > sem precisar saber Spark, mantendo toda a governança do Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Perguntas-modelo para o Lakeflow Designer (IA Assistant)
# MAGIC
# MAGIC Quando você estiver no **Lakeflow Designer**, o campo de chat do **AI Assistant**
# MAGIC aceita prompts em linguagem natural. Use estas 5 perguntas-modelo
# MAGIC (em ordem crescente de complexidade) para construir suas Silver/Gold visualmente:
# MAGIC
# MAGIC ## Pergunta 1 — KPI por Hospital (Gold simples)
# MAGIC
# MAGIC > Group by `id_hospital`, `nome_hospital`, `cidade`, `especialidade` and calculate
# MAGIC > `total_consultas` as count of `id_consulta`,
# MAGIC > `tempo_espera_medio` as average of `tempo_espera_minutos`,
# MAGIC > `cancelamentos` as sum of consultas where status = 'cancelada',
# MAGIC > and `pacientes_unicos` as count distinct of `id_paciente`
# MAGIC
# MAGIC **Resultado esperado:** Tabela Gold com 1 linha por hospital, ideal para o primeiro dashboard.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pergunta 2 — Patient Experience por Canal de Avaliação
# MAGIC
# MAGIC > Group by `canal_avaliacao`, `cidade` and calculate
# MAGIC > `total_avaliacoes` as count of `id_consulta`,
# MAGIC > `avg_tempo_para_avaliar` as average of `tempo_para_avaliar_min`,
# MAGIC > `taxa_anonimato` as sum of `avaliacao_anonima` divided by total,
# MAGIC > and `tempo_espera_medio` as average of `tempo_espera_minutos`
# MAGIC
# MAGIC **Storytelling:** "Qual canal (App/WhatsApp/SMS) gera mais avaliações — e elas são mais rápidas ou mais anônimas?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pergunta 3 — Análise Epidemiológica por Região + CID
# MAGIC
# MAGIC > Group by `regiao`, `estado`, `especialidade`, `cid_10`, `diagnostico_descricao` and calculate
# MAGIC > `total_consultas` as count of `id_consulta`,
# MAGIC > `pacientes_distintos` as count distinct of `id_paciente`,
# MAGIC > `idade_media` as average of `idade`,
# MAGIC > and `tempo_espera_medio` as average of `tempo_espera_minutos`
# MAGIC
# MAGIC **Storytelling:** "Quais são as doenças mais comuns por região brasileira? Idade média dos pacientes por CID?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pergunta 4 — Produtividade Médica e Telemedicina
# MAGIC
# MAGIC > Group by `id_medico`, `nome_medico`, `crm`, `especialidade` and calculate
# MAGIC > `total_consultas` as count of `id_consulta`,
# MAGIC > `presenciais` as sum of consultas where modalidade = 'presencial',
# MAGIC > `telemedicina` as sum where modalidade = 'telemedicina',
# MAGIC > and `tempo_espera_medio` as average of `tempo_espera_minutos`
# MAGIC
# MAGIC **Storytelling:** "Quais médicos atendem mais? Qual o ratio de telemedicina por especialidade?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pergunta 5 — Demografia × Convênio (Dashboard Executivo)
# MAGIC
# MAGIC > Group by `convenio`, `sexo`, `faixa_etaria`, `regiao` and calculate
# MAGIC > `total_pacientes` as count distinct of `id_paciente`,
# MAGIC > `total_consultas` as count of `id_consulta`,
# MAGIC > `cancelamentos` as sum where status = 'cancelada',
# MAGIC > `tempo_espera_medio` as average of `tempo_espera_minutos`,
# MAGIC > and `taxa_telemedicina` as sum where modalidade = 'telemedicina' divided by total
# MAGIC
# MAGIC **Storytelling:** "SUS vs planos privados — onde a população jovem está sendo melhor atendida?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dica de uso no Lakeflow Designer
# MAGIC
# MAGIC 1. Abra o **Lakeflow Editor** → **New ETL pipeline**
# MAGIC 2. Adicione a fonte: **read_files** apontando para `/Volumes/healthcare_lakehouse/bronze/landing_zone/consultas/`
# MAGIC 3. Adicione o joiner: arraste `bronze.hospitais` e `bronze.pacientes` como source secundárias
# MAGIC 4. No node de **Transform**, cole uma das 5 perguntas acima no campo do **AI Assistant**
# MAGIC 5. O Lakeflow vai gerar o código SQL/Spark automaticamente
# MAGIC 6. Aponte o **Sink** para `silver.consultas_360` ou `gold.kpis_*`
# MAGIC
# MAGIC > **Sugestão de hands-on**: começar pela **Pergunta 1** (simples), depois subir para a **Pergunta 3** ou **Pergunta 5** que combinam dimensões de forma mais rica.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### Setup concluído — siga para o Lakeflow Designer!
# MAGIC
# MAGIC > **Databricks Field Engineering — Healthcare**
