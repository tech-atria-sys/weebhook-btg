import os
import io
import re
import hmac
import uuid
import math
import zipfile
import requests
import pyodbc
import pandas as pd
import threading
from typing import Optional, Tuple
from urllib.parse import urlparse, quote_plus
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from flask import Flask, request, jsonify
from zoneinfo import ZoneInfo

app = Flask(__name__)

# 1. CONFIGURAÇÕES

SERVER_NAME       = os.getenv("SERVER_NAME")
DATABASE_NAME     = os.getenv("DATABASE_NAME")
USERNAME          = os.getenv("USERNAME")
PASSWORD          = os.getenv("PASSWORD")
WEBHOOK_TOKEN     = os.getenv("WEBHOOK_TOKEN")
BTG_CLIENT_ID     = os.getenv("BTG_CLIENT_ID")
BTG_CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET")

URL_REPORT_NNM      = os.getenv("PARTNER_REPORT_URL_NNM")
URL_REPORT_BASE     = os.getenv("PARTNER_REPORT_URL_BASEBTG")
URL_REPORT_CUSTODIA = os.getenv("PARTNER_REPORT_URL_CUSTODIA")

SHAREPOINT_LINKS = [
    ("RODRIGO DE MELLO D’ELIA",         "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQBVuGicHybdRrC4d1MtFO8vAbY4Kw4m4_8gNo8EKu3BN4I?download=1"),
    ("CAIC ZEM GOMES",                   "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQByqpoVZXN3TYdcUlFmYqE9Af8AsDkk6umaNM26wLfQlo4?download=1"),
    ("FERNANDO DOMINGUES DA SILVA",      "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQAFP5cFxU9QRL_OWClG-BJdAT3Wvk18_VoypIF3CxIpQYY?download=1"),
    ("SAADALLAH JOSE ASSAD",             "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQDjXAOoHHxgQYo_aeQQdXz4AXzuH0TotDouMPG_NNH4H-4?download=1"),
    ("PAULO ROBERTO FARIA SILVA",        "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQA6mQn9Z8lwTL7HfLdw1UzSAbCoa8qOg03wNZvbJyDoIaw?download=1"),
    ("MARCOS SOARES PEREIRA FILHO",      "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQAtEitVDNlnT4zWgSIonhfzAQlTKUlOX3xzRRYfgcoHrZE?download=1"),
    ("RENAN BENTO DA SILVA",             "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQCr2UivmbvdQ4EIlpS8fcTfAVmuuqLXeLhqroDonRcYXDQ?download=1"),
    ("ROSANA APARECIDA PAVANI DA SILVA", "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQDmCPZulmfjTYtC94MGxHh1ARzYBjyg9gWNL9v2U--M0CQ?download=1"),
    ("RAFAEL PASOLD MEDEIROS",           "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQBVpoNJYZ8NSY9rnRotpPSPAXlir-PVsE7SItom19ZhijI?download=1"),
    ("FELIPE AUGUSTO CARDOSO",           "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQCy_TXmqwgLSL2M6iwrcsp0AWNG5plDMI-xPRY363bX2dA?download=1"),
    ("GUILHERME DE LUCCA BERTELONI",     "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQCZlPDCr4lsTpfTGlsnjvQiAT0lnyonDCFpCKlASYnfES4?download=1"),
    ("IZADORA VILLELA FREITAS",          "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQD4lZoreFnUQYKgxi0ibLONAV1CwfMLplcscrKNd6ZJPis?download=1"),
    ("VITOR OLIVEIRA DOS REIS",          "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQAmZYnDo_EeToIhGV7QRgYJAb6YYoMOMaWaAEadD6WOS4I?download=1"),
]

SCHEMA_DEFAULT = "dbo"
TZ_BRASILIA    = ZoneInfo("America/Sao_Paulo")

DOMINIOS_PERMITIDOS = {
    "invest-reports.s3.amazonaws.com",
    "invest-reports-prd.s3.sa-east-1.amazonaws.com",
    "api.btgpactual.com",
    "api.ipify.org"
}

app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

# Janela curta da tabela de fatos NNM (dias relativos ao max do CSV)
DIAS_FATO_NNM = 2

# 2. CONSTANTES DE NEGÓCIO — centralizadas para facilitar manutenção

# Mapeamento de colunas do CSV do BTG → padrão interno
COLUNAS_RENAME_BASE_BTG = {
    "nr_conta":        "Conta",
    "nome_completo":   "Nome",
    "nm_officer":      "Assessor",
    "faixa_cliente":   "Faixa Cliente",
    "pl_total":        "PL Total",
    "vl_pl_declarado": "PL Declarado",
    "dt_vinculo":      "Data Vínculo",
    "dt_abertura":     "Data de Abertura",
}

# Faixas consolidadas em "Ate 300k"
FAIXAS_ATE_300K = {"Ate 50K", "Entre 50k e 100k", "Entre 100k e 300k"}

# Correções de assessor conhecidas — aplicadas em todos os pontos da app
CORRECOES_ASSESSOR = {
    "RODRIGO DE MELLO D?ELIA": "RODRIGO DE MELLO D’ELIA",
    "RODRIGO DE MELLO DELIA":  "RODRIGO DE MELLO D’ELIA",
    "MURILO LUIZ SILVA GINO":  "IZADORA VILLELA FREITAS",
}

# Correções manuais por conta específica
CORRECOES_CONTA_ASSESSOR = {
    "590732": "JOSE AUGUSTO ALVES DE PAULA FILHO",
    "299305": "JOSE AUGUSTO ALVES DE PAULA FILHO",
}

# Contas de controle interno — ignoradas no entradas/saídas
CONTAS_IGNORAR = {"1983816", "1106619"}

# Colunas salvas no snapshot diário
COLUNAS_SNAPSHOT = [
    "Conta", "Nome", "Assessor", "PL Total", "PL Declarado",
    "Faixa Cliente", "Data Vínculo", "Data de Abertura",
    "tipo_cliente", "profissao", "dt_nascimento",
    "perfil_investidor", "endereco_cidade", "endereco_estado",
    "pl_conta_corrente", "pl_fundos", "pl_renda_fixa",
    "pl_renda_variavel", "pl_previdencia", "pl_derivativos",
    "pl_valores_transito", "cge_officer", "cge_partner",
    "nm_partner", "email", "email_assessor",
]

# Colunas salvas no pl_historico_diario
COLUNAS_PL_HISTORICO = [
    "Conta", "Assessor", "PL Total", "PL Declarado",
    "Faixa Cliente", "Data Vínculo",
    "pl_conta_corrente", "pl_fundos", "pl_renda_fixa",
    "pl_renda_variavel", "pl_previdencia", "pl_derivativos",
]

# 3. INFRAESTRUTURA

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"UID={quote_plus(USERNAME or '')};"
    f"PWD={quote_plus(PASSWORD or '')};"
    f"TrustServerCertificate=yes"
)


def now_brasilia() -> datetime:
    return datetime.utcnow() - timedelta(hours=3)


def get_engine():
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={CONN_STR}",
        fast_executemany=True
    )


def registrar_log(atividade: str, status: str, linhas: int = 0, mensagem: str = ""):
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO dbo.logs_atividades
                    (atividade, status, linhas_processadas, mensagem_detalhe, data_hora)
                VALUES
                    (:atv, :st, :ln, :msg, :dt)
            """), {
                "atv": atividade,
                "st":  status,
                "ln":  linhas,
                "msg": str(mensagem)[:500],
                "dt":  now_brasilia()
            })
    except Exception as e:
        print(f"[AVISO] Falha ao gravar log no banco: {e}")


def salvar_df_otimizado(
    df: pd.DataFrame,
    nome_tabela: str,
    col_pk: Optional[str] = None,
    if_exists: str = "append",
    schema: str = "dbo"
):
    """
    Persiste DataFrame no SQL Server com chunksize seguro para o limite
    de 2.100 parâmetros do pyodbc.
    """
    if df.empty:
        return

    num_colunas    = len(df.columns)
    limit_params   = math.floor(2090 / num_colunas) if num_colunas > 0 else 1000
    safe_chunksize = max(1, min(limit_params, 1000))

    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(
            name=nome_tabela,
            con=conn,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=safe_chunksize,
            method="multi"
        )

        if col_pk and if_exists == "replace":
            try:
                conn.execute(text(
                    f'ALTER TABLE {schema}."{nome_tabela}" '
                    f'ALTER COLUMN "{col_pk}" VARCHAR(450) NOT NULL'
                ))
                conn.execute(text(
                    f'ALTER TABLE {schema}."{nome_tabela}" '
                    f'ADD PRIMARY KEY ("{col_pk}")'
                ))
            except Exception as e:
                print(f"[AVISO] PK em {nome_tabela}: {e}")


def get_btg_token() -> Optional[str]:
    url = (
        "https://api.btgpactual.com/iaas-auth/api/v1/"
        "authorization/oauth2/accesstoken"
    )
    headers = {
        "x-id-partner-request": str(uuid.uuid4()),
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }
    try:
        r = requests.post(
            url,
            data={"grant_type": "client_credentials"},
            headers=headers,
            auth=(BTG_CLIENT_ID, BTG_CLIENT_SECRET)
        )
        return r.headers.get("access_token") if r.status_code == 200 else None
    except Exception as e:
        print(f"[ERRO] get_btg_token: {e}")
        return None


def extrair_conta_do_nome(nome_arquivo: str) -> Optional[str]:
    match = re.search(r"(\d+)", nome_arquivo)
    return match.group(1) if match else None


def validar_token(req) -> bool:
    token_recebido = (
        req.headers.get("X-Webhook-Token")
        or req.headers.get("X-Api-Key")
        or ""
    )
    token_esperado = WEBHOOK_TOKEN or ""
    if not token_esperado:
        return False
    return hmac.compare_digest(token_recebido, token_esperado)


def validar_url_download(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme != "https":
            return False
        if parsed.netloc not in DOMINIOS_PERMITIDOS:
            return False
        return True
    except Exception:
        return False


def parse_datas_csv(df: pd.DataFrame, coluna: str) -> Tuple[datetime, datetime]:
    df[coluna] = pd.to_datetime(df[coluna], errors="coerce")
    data_min   = df[coluna].min()
    data_max   = df[coluna].max()

    if pd.isnull(data_min) or pd.isnull(data_max):
        raise ValueError(
            f"Coluna '{coluna}' sem datas válidas — verifique o CSV recebido."
        )
    return data_min, data_max


def erro_interno(atividade: str, e: Exception, conta: str = "") -> tuple:
    detalhe = f"Conta: {conta} | {str(e)}" if conta else str(e)
    print(f"[ERRO CRÍTICO {atividade}] {detalhe}")
    registrar_log(atividade, "Erro", 0, detalhe)
    return jsonify({"erro": "Erro interno — consulte os logs"}), 500


def aplicar_correcoes_assessor(df: pd.DataFrame, coluna: str = "Assessor") -> pd.DataFrame:
    """
    Aplica CORRECOES_ASSESSOR e CORRECOES_CONTA_ASSESSOR de forma centralizada.
    Pode ser chamada em qualquer ponto que manipule assessores.
    """
    if coluna in df.columns:
        df[coluna] = df[coluna].astype(str).str.upper().str.strip()
        df[coluna].replace(CORRECOES_ASSESSOR, inplace=True)

    if "Conta" in df.columns:
        for conta, assessor in CORRECOES_CONTA_ASSESSOR.items():
            df.loc[df["Conta"] == conta, coluna] = assessor

    return df

# 4. FUNÇÕES AUXILIARES DO BASEBTG

def _atualizar_tipo_clientes(base: pd.DataFrame, engine):
    """
    Deriva tipo_clientes diretamente da base_btg.
    Replace a cada atualização — sempre reflete o estado atual.
    Contas sem tipo_cliente recebem 'Offshore' por padrão.
    """
    try:
        if "tipo_cliente" not in base.columns:
            print("[AVISO] tipo_cliente ausente na base — tipo_clientes não atualizado")
            return

        tipo = base[["Conta", "tipo_cliente"]].copy()
        tipo.rename(columns={"tipo_cliente": "Tipo"}, inplace=True)
        tipo["Tipo"].fillna("Offshore", inplace=True)

        salvar_df_otimizado(
            tipo, "tipo_clientes",
            col_pk="Conta", if_exists="replace"
        )
        print(f"[tipo_clientes] {len(tipo)} registros atualizados", flush=True)

    except Exception as e:
        print(f"[AVISO] Falha ao atualizar tipo_clientes: {e}", flush=True)

# 5. FUNÇÕES DE PROCESSOS ASSÍNCRONOS

def _executar_calculo_saidas():
    """
    Detecta contas que constam no histórico de captação mas saíram da base_btg
    e registra um lançamento de débito com o último PL conhecido.
    Deve rodar APÓS webhook/basebtg e webhook/nnm.
    Idempotente: não reinsere contas que já têm lançamento de saída.
    """
    atividade = "CALCULO_SAIDAS"
    try:
        engine = get_engine()

        with engine.connect() as conn:
            contas_historico = pd.read_sql(
                "SELECT DISTINCT nr_conta FROM dbo.relatorios_nnm_gerencial", conn
            )
            contas_historico["nr_conta"] = contas_historico["nr_conta"].astype(str).str.strip()

            contas_ativas = pd.read_sql("SELECT Conta FROM dbo.base_btg", conn)
            contas_ativas["Conta"] = contas_ativas["Conta"].astype(str).str.strip()

            inativas = contas_historico[
                ~contas_historico["nr_conta"].isin(contas_ativas["Conta"])
            ]["nr_conta"].tolist()

            if not inativas:
                registrar_log(atividade, "Sucesso", 0, "Nenhuma saída detectada")
                return

            saidas_existentes = pd.read_sql(
                "SELECT DISTINCT nr_conta FROM dbo.relatorios_nnm_gerencial "
                "WHERE tipo_captacao = 'Saída de conta'",
                conn
            )
            saidas_existentes["nr_conta"] = saidas_existentes["nr_conta"].astype(str)

            novas_saidas = [
                c for c in inativas
                if c not in saidas_existentes["nr_conta"].tolist()
            ]

            if not novas_saidas:
                registrar_log(atividade, "Sucesso", 0, "Nenhuma saída nova a registrar")
                return

            placeholders = ", ".join([f"'{c}'" for c in novas_saidas])
            pl_historico = pd.read_sql(f"""
                SELECT Conta AS nr_conta, [PL Total], Data
                FROM dbo.pl_historico_diario
                WHERE Conta IN ({placeholders})
            """, conn)

        pl_historico["nr_conta"] = pl_historico["nr_conta"].astype(str)
        pl_historico["Data"]     = pd.to_datetime(pl_historico["Data"])

        ultimo_pl = (
            pl_historico
            .sort_values("Data")
            .groupby("nr_conta")
            .last()
            .reset_index()
        )

        debitos = ultimo_pl.copy()
        debitos["captacao"]      = debitos["PL Total"] * -1
        debitos["data_captacao"] = debitos["Data"].dt.date
        debitos["tipo_captacao"] = "Saída de conta"
        debitos["mercado"]       = "Saída de conta"
        debitos["descricao"]     = "Saída de conta"
        debitos["data_upload"]   = now_brasilia()

        colunas_saida = [
            "nr_conta", "data_captacao", "captacao",
            "tipo_captacao", "mercado", "descricao", "data_upload"
        ]
        debitos = debitos[[c for c in colunas_saida if c in debitos.columns]]

        salvar_df_otimizado(debitos, "relatorios_nnm_gerencial", if_exists="append")

        msg = f"{len(debitos)} saídas registradas"
        registrar_log(atividade, "Sucesso", len(debitos), msg)
        print(f"[SUCESSO CALCULO_SAIDAS] {msg}")

    except Exception as e:
        registrar_log(atividade, "Erro", 0, str(e))
        print(f"[ERRO CRÍTICO CALCULO_SAIDAS] {e}")


def _executar_entradas_saidas():
    """
    Compara o snapshot de hoje com o de ontem na base_btg_snapshot_diario
    para detectar entradas e saídas de clientes.
    Deve rodar APÓS webhook/basebtg (que gera o snapshot).
    Idempotente: deleta e recria os registros do dia atual antes de inserir.
    """
    atividade = "ENTRADAS_SAIDAS"
    try:
        engine = get_engine()

        with engine.connect() as conn:
            datas_query = pd.read_sql("""
                SELECT DISTINCT CONVERT(DATE, Data) AS Data
                FROM dbo.base_btg_snapshot_diario
                ORDER BY Data DESC
                OFFSET 0 ROWS FETCH NEXT 2 ROWS ONLY
            """, conn)

        if len(datas_query) < 2:
            registrar_log(atividade, "Aviso", 0,
                          "Snapshot insuficiente — menos de 2 dias disponíveis")
            return

        data_hoje  = datas_query["Data"].iloc[0]
        data_ontem = datas_query["Data"].iloc[1]

        print(f"[ENTRADAS_SAIDAS] Comparando {data_ontem} → {data_hoje}")

        with engine.connect() as conn:
            snap_hoje  = pd.read_sql(f"""
                SELECT * FROM dbo.base_btg_snapshot_diario
                WHERE CONVERT(DATE, Data) = '{data_hoje}'
            """, conn)

            snap_ontem = pd.read_sql(f"""
                SELECT * FROM dbo.base_btg_snapshot_diario
                WHERE CONVERT(DATE, Data) = '{data_ontem}'
            """, conn)

        for df in [snap_hoje, snap_ontem]:
            df["Conta"] = df["Conta"].astype(str).str.strip()

        # Contas que existiam ontem e não existem hoje = SAÍRAM
        contas_sairam = snap_ontem[
            ~snap_ontem["Conta"].isin(snap_hoje["Conta"])
        ].copy()
        contas_sairam["Situação"] = "Saiu"

        # Contas que não existiam ontem e existem hoje = ENTRARAM
        contas_entraram = snap_hoje[
            ~snap_hoje["Conta"].isin(snap_ontem["Conta"])
        ].copy()
        contas_entraram["Situação"] = "Entrou"
        contas_entraram.drop(columns=["Faixa Cliente"], errors="ignore", inplace=True)

        movimentacoes = pd.concat([contas_sairam, contas_entraram], ignore_index=True)

        if movimentacoes.empty:
            registrar_log(atividade, "Sucesso", 0,
                          f"Nenhuma movimentação em {data_hoje}")
            return

        movimentacoes["Mês de entrada/saída"] = pd.to_datetime(data_hoje)

        # Remove contas de controle interno
        movimentacoes = movimentacoes[
            ~movimentacoes["Conta"].isin(CONTAS_IGNORAR)
        ]

        # Correções de assessor centralizadas
        movimentacoes = aplicar_correcoes_assessor(movimentacoes)

        # Nome já está no snapshot — não precisa de merge externo
        movimentacoes.drop_duplicates(subset=["Conta"], keep="last", inplace=True)

        # Idempotência: remove registros do dia atual antes de reinserir
        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.Entradas_e_saidas_consolidado
                WHERE CONVERT(DATE, [Mês de entrada/saída]) = :hoje
            """), {"hoje": str(data_hoje)})

        colunas_alvo = [
            "Conta", "Nome", "Assessor", "PL Total", "PL Declarado",
            "Faixa Cliente", "Data Vínculo", "Situação", "Mês de entrada/saída"
        ]
        movimentacoes = movimentacoes[
            [c for c in colunas_alvo if c in movimentacoes.columns]
        ]

        salvar_df_otimizado(
            movimentacoes, "Entradas_e_saidas_consolidado", if_exists="append"
        )

        # ── MIGRACOES_BTG: somente contas que ENTRARAM ────────────────────────
        entradas = movimentacoes[movimentacoes["Situação"] == "Entrou"].copy()
        qtd_migracoes = 0

        if not entradas.empty:
            entradas_mig = entradas[
                ["Mês de entrada/saída", "Conta", "Assessor", "PL Total"]
            ].copy()
            entradas_mig.rename(columns={
                "Mês de entrada/saída": "DATA",
                "Conta":    "CONTA",
                "PL Total": "CAPTAÇÃO"
            }, inplace=True)
            entradas_mig["TIPO DE CAPTACAO"] = "Migração BTG"
            entradas_mig["MERCADO"]          = "Migração BTG"
            entradas_mig["DESCRIÇÃO"]        = "Migração BTG"
            entradas_mig["Considerar"]       = "SIM"
            entradas_mig.dropna(subset=["CAPTAÇÃO"], inplace=True)

            # Não insere contas que já existem em migracoes_btg
            with engine.connect() as conn:
                existentes = pd.read_sql(
                    "SELECT DISTINCT CONTA FROM dbo.migracoes_btg", conn
                )
            existentes["CONTA"] = existentes["CONTA"].astype(str)
            entradas_mig = entradas_mig[
                ~entradas_mig["CONTA"].isin(existentes["CONTA"])
            ]

            if not entradas_mig.empty:
                salvar_df_otimizado(
                    entradas_mig, "migracoes_btg", if_exists="append"
                )
                qtd_migracoes = len(entradas_mig)

        msg = (
            f"Saíram: {len(contas_sairam)} | "
            f"Entraram: {len(contas_entraram)} | "
            f"Migrações novas: {qtd_migracoes}"
        )
        print(f"[SUCESSO ENTRADAS_SAIDAS] {msg}")
        registrar_log(atividade, "Sucesso", len(movimentacoes), msg)

    except Exception as e:
        registrar_log(atividade, "Erro", 0, str(e))
        print(f"[ERRO CRÍTICO ENTRADAS_SAIDAS] {e}")


def _load_previa_assessor(advisor_name: str, link: str) -> pd.DataFrame:
    """Baixa e parseia a aba 'Meta' do Excel de um assessor."""
    try:
        response = requests.get(link, params={"downloadformat": "excel"}, timeout=15)
        response.raise_for_status()

        df = pd.read_excel(io.BytesIO(response.content), sheet_name="Meta")

        if len(df.columns) < 5:
            print(f"   [AVISO] Colunas insuficientes para {advisor_name}.")
            return pd.DataFrame()

        df.rename(columns={
            df.columns[0]: "Categoria - Acompanhamento Next",
            df.columns[1]: "META - ROA",
            df.columns[2]: "REALIZADO - ROA",
            df.columns[4]: "REALIZADO - VOLUME",
        }, inplace=True)

        df["META - VOLUME"] = 0.0

        if "Categoria - Acompanhamento Next" in df.columns:
            df = df[df["Categoria - Acompanhamento Next"] != "TOTAL"]

        df["Assessor"] = advisor_name

        colunas_ordem = [
            "Categoria - Acompanhamento Next", "META - VOLUME", "REALIZADO - VOLUME",
            "META - ROA", "REALIZADO - ROA", "Assessor",
        ]
        return df[[c for c in colunas_ordem if c in df.columns]]

    except Exception as e:
        print(f"   [ERRO] {advisor_name}: {e}")
        return pd.DataFrame()


def _executar_previa_receita():
    """
    Lógica completa de atualização da Prévia Receita.
    Roda em thread separada para não bloquear o endpoint.
    """
    atividade = "PREVIA_RECEITA"
    try:
        primeiro_dia_mes = pd.Timestamp(
            datetime.now(TZ_BRASILIA).replace(
                day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=None
            )
        )

        engine = get_engine()
        try:
            hist = pd.read_sql(
                f"SELECT * FROM {SCHEMA_DEFAULT}.previa_receita_nova", engine
            )
            if not hist.empty:
                hist["Data"] = pd.to_datetime(hist["Data"], errors="coerce")
                hist = hist[hist["Data"] != primeiro_dia_mes]
        except Exception:
            hist = pd.DataFrame()

        lista_dfs = []
        for nome, link in SHAREPOINT_LINKS:
            df_temp = _load_previa_assessor(nome, link)
            if not df_temp.empty:
                lista_dfs.append(df_temp)
                print(f"   [OK] {nome}")

        if not lista_dfs:
            registrar_log(atividade, "Erro", 0, "Nenhum dado coletado dos assessores.")
            return

        previa_receita = pd.concat(lista_dfs, ignore_index=True)
        previa_receita["Data"]            = primeiro_dia_mes
        previa_receita["Hora Atualizado"] = datetime.now(TZ_BRASILIA).replace(tzinfo=None)

        previa_final = pd.concat([hist, previa_receita], axis=0, ignore_index=True)
        previa_final.loc[previa_final["META - VOLUME"] == "-", "META - VOLUME"] = 0
        previa_final.fillna(0, inplace=True)

        salvar_df_otimizado(
            previa_final, "previa_receita_nova",
            if_exists="replace", schema=SCHEMA_DEFAULT
        )

        cols_agg = [
            c for c in ["Assessor", "Data", "META - ROA", "REALIZADO - ROA"]
            if c in previa_receita.columns
        ]
        previa_agg = (
            previa_receita[cols_agg]
            .groupby(["Assessor", "Data"])
            .sum()
            .reset_index()
        )
        previa_agg["Data"] = pd.to_datetime(previa_agg["Data"])

        try:
            hist_agg = pd.read_sql(
                f"SELECT * FROM {SCHEMA_DEFAULT}.previa_receita_assessor_historico",
                engine
            )
            if not hist_agg.empty:
                hist_agg["Data"] = pd.to_datetime(hist_agg["Data"], errors="coerce")
                hist_agg = hist_agg[hist_agg["Data"] != primeiro_dia_mes]
            previa_final_agg = pd.concat(
                [hist_agg, previa_agg], axis=0, ignore_index=True
            )
        except Exception:
            previa_final_agg = previa_agg

        salvar_df_otimizado(
            previa_final_agg, "previa_receita_assessor_historico",
            if_exists="replace", schema=SCHEMA_DEFAULT
        )

        msg = (
            f"Detalhado: {len(previa_final)} linhas | "
            f"Agregado: {len(previa_final_agg)} linhas"
        )
        print(f"[SUCESSO PREVIA_RECEITA] {msg}")
        registrar_log(atividade, "Sucesso", len(previa_final), msg)

    except Exception as e:
        registrar_log(atividade, "Erro", 0, str(e))
        print(f"[ERRO CRÍTICO PREVIA_RECEITA] {e}")

# 6. ROTAS DE GATILHO

def _trigger_generico(url_relatorio: str, nome_log: str):
    """Lógica comum a todos os triggers de relatório BTG."""
    access_token = get_btg_token()
    if not access_token:
        registrar_log(nome_log, "Erro", 0, "Falha na geração do token BTG")
        return jsonify({"erro": "Falha ao autenticar"}), 502

    headers = {
        "x-id-partner-request": str(uuid.uuid4()),
        "access_token": access_token,
        "Content-Type": "application/json"
    }
    try:
        r = requests.get(url_relatorio, headers=headers)
        if r.status_code == 202:
            registrar_log(nome_log, "Sucesso", 0, "Solicitação aceita pelo BTG")
            return jsonify({"status": "Solicitado", "http_code": 202}), 202
        return jsonify({"erro_btg": r.text}), r.status_code
    except Exception as e:
        return erro_interno(nome_log, e)


@app.route("/trigger/nnm", methods=["GET"])
def trigger_nnm():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403
    return _trigger_generico(URL_REPORT_NNM, "TRIGGER_NNM")


@app.route("/trigger/basebtg", methods=["GET"])
def trigger_basebtg():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403
    return _trigger_generico(URL_REPORT_BASE, "TRIGGER_BASE")


@app.route("/trigger/custodia", methods=["GET"])
def trigger_custodia():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403
    return _trigger_generico(URL_REPORT_CUSTODIA, "TRIGGER_CUSTODIA")


@app.route("/trigger/previa-receita", methods=["GET"])
def trigger_previa_receita():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    thread = threading.Thread(target=_executar_previa_receita, daemon=True)
    thread.start()
    return jsonify({
        "status": "iniciado",
        "mensagem": "Atualização da Prévia Receita em andamento"
    }), 202


@app.route("/trigger/calcular-entradas-saidas", methods=["GET"])
def trigger_calcular_entradas_saidas():
    """
    Compara snapshots consecutivos da base_btg para detectar entradas e saídas.
    Deve ser chamado após o webhook/basebtg ter processado o snapshot do dia.
    """
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    thread = threading.Thread(target=_executar_entradas_saidas, daemon=True)
    thread.start()
    return jsonify({"status": "iniciado"}), 202


@app.route("/trigger/calcular-saidas", methods=["GET"])
def trigger_calcular_saidas():
    """
    Detecta contas inativas e registra débitos em relatorios_nnm_gerencial.
    Deve ser chamado após calcular-entradas-saidas.
    """
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    thread = threading.Thread(target=_executar_calculo_saidas, daemon=True)
    thread.start()
    return jsonify({"status": "iniciado"}), 202


@app.route("/trigger/carteiras-recomendadas", methods=["GET"])
def trigger_carteiras_recomendadas():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    access_token = get_btg_token()
    if not access_token:
        registrar_log("CARTEIRAS_RECOM", "Erro", 0, "Falha na geração do token BTG")
        return jsonify({"erro": "Falha ao autenticar no BTG"}), 502

    headers = {
        "x-id-partner-request": str(uuid.uuid4()),
        "access_token": access_token,
        "Content-Type": "application/json"
    }

    try:
        url = (
            "https://api.btgpactual.com/iaas-recommended-equities/api/v1/"
            "recommended-equities-allocation"
        )
        r = requests.get(url, headers=headers)

        if r.status_code != 200:
            erro_msg = f"Erro BTG: {r.status_code}"
            registrar_log("CARTEIRAS_RECOM", "Erro", 0, f"{erro_msg} - {r.text}")
            return jsonify({"erro": erro_msg}), r.status_code

        dados = r.json()
        if not dados:
            return jsonify({"status": "Sucesso", "mensagem": "Nenhuma carteira retornada"}), 200

        linhas = []
        for carteira in dados:
            base_linha = {
                "tipo_carteira":           carteira.get("typeInitial"),
                "descricao":               carteira.get("description"),
                "nome_carteira":           carteira.get("name"),
                "link_pdf":                carteira.get("fileName"),
                "rentabilidade_anterior":  carteira.get("previousProfitability"),
                "rentabilidade_acumulada": carteira.get("accumulatedProfitability"),
                "inicio_validade":         pd.to_datetime(carteira.get("validityStart"), errors="coerce"),
                "fim_validade":            pd.to_datetime(carteira.get("validityEnd"),   errors="coerce"),
                "data_extracao":           now_brasilia()
            }
            ativos = carteira.get("assets", [])
            if ativos:
                for item in ativos:
                    ativo = item.get("asset", {})
                    setor = ativo.get("sector", {})
                    linha = base_linha.copy()
                    linha.update({
                        "ticker":  ativo.get("ticker"),
                        "empresa": ativo.get("company"),
                        "setor":   setor.get("name"),
                        "peso":    item.get("weight")
                    })
                    linhas.append(linha)
            else:
                linhas.append(base_linha)

        df_carteiras = pd.DataFrame(linhas)
        salvar_df_otimizado(df_carteiras, "carteiras_recomendadas_btg", if_exists="replace")

        registrar_log("CARTEIRAS_RECOM", "Sucesso", len(df_carteiras),
                      "Carteiras recomendadas importadas com sucesso")
        return jsonify({"status": "Sucesso", "linhas_salvas": len(df_carteiras)}), 200

    except Exception as e:
        return erro_interno("CARTEIRAS_RECOM", e)

# 7. WEBHOOKS

@app.route("/webhook/basebtg", methods=["POST"])
def webhook_base_btg():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    try:
        dados = request.json
        url_download = dados.get("response", {}).get("url") or dados.get("url")
        if not url_download:
            return jsonify({"erro": "URL não encontrada"}), 400

        if not validar_url_download(url_download):
            registrar_log("BASE_BTG", "Erro", 0,
                          f"URL bloqueada por política SSRF: {url_download}")
            return jsonify({"erro": "URL não autorizada"}), 400

        # ── 1. DOWNLOAD E PARSE ───────────────────────────────────────────────
        r = requests.get(url_download)
        r.raise_for_status()

        base = pd.read_csv(io.BytesIO(r.content), sep=";", encoding="utf-8")

        # ── 2. BACKUP RAW ─────────────────────────────────────────────────────
        df_raw = base.copy()
        df_raw["data_recebimento_webhook"] = now_brasilia()
        salvar_df_otimizado(df_raw, "backup_base_btg_raw", if_exists="append")

        # ── 3. RENAME ─────────────────────────────────────────────────────────
        renomear_presentes = {
            k: v for k, v in COLUNAS_RENAME_BASE_BTG.items()
            if k in base.columns
        }
        ausentes = [k for k in COLUNAS_RENAME_BASE_BTG if k not in base.columns]
        if ausentes:
            print(f"[AVISO BASE_BTG] Colunas não encontradas no CSV: {ausentes}",
                  flush=True)

        base.rename(columns=renomear_presentes, inplace=True)

        # ── 4. VALIDAÇÃO CRÍTICA ──────────────────────────────────────────────
        colunas_criticas = ["Conta", "Assessor", "PL Total"]
        faltando = [c for c in colunas_criticas if c not in base.columns]
        if faltando:
            msg = f"Colunas críticas ausentes após rename: {faltando}"
            registrar_log("BASE_BTG", "Erro", 0, msg)
            return jsonify({"erro": msg}), 400

        # ── 5. TIPAGEM ────────────────────────────────────────────────────────
        base["Conta"]    = base["Conta"].astype(str).str.strip()
        base["Assessor"] = base["Assessor"].astype(str).str.upper().str.strip()

        for col_data in [
            "Data Vínculo", "Data de Abertura", "dt_nascimento",
            "dt_primeiro_investimento", "dt_ultimo_aporte", "dt_vinculo_escritorio"
        ]:
            if col_data in base.columns:
                base[col_data] = pd.to_datetime(base[col_data], errors="coerce")

        # ── 6. REGRAS DE NEGÓCIO ──────────────────────────────────────────────
        if "Faixa Cliente" in base.columns:
            base.loc[
                base["Faixa Cliente"].isin(FAIXAS_ATE_300K),
                "Faixa Cliente"
            ] = "Ate 300k"

        base = aplicar_correcoes_assessor(base)

        # ── 7. MERGE COM OFFSHORE ─────────────────────────────────────────────
        engine = get_engine()
        try:
            with engine.connect() as conn:
                offshore = pd.read_sql(
                    "SELECT Conta, Nome, Assessor, [PL Total] FROM dbo.pl_offshore",
                    conn
                )
            offshore["Conta"] = offshore["Conta"].astype(str).str.strip()

            # Offshore entra no topo — keep='first' no drop_duplicates preserva offshore
            base = pd.concat([offshore, base], axis=0, ignore_index=True)
            print(f"[BASE_BTG] Offshore mesclado: {len(offshore)} contas", flush=True)

        except Exception as e:
            print(f"[AVISO BASE_BTG] Offshore não carregado (tabela ausente?): {e}",
                  flush=True)

        # ── 8. DEDUPLICAÇÃO ───────────────────────────────────────────────────
        base.drop_duplicates(subset="Conta", keep="first", inplace=True)

        # ── 9. SALVA BASE_BTG ─────────────────────────────────────────────────
        salvar_df_otimizado(
            base, "base_btg",
            col_pk="Conta", if_exists="replace"
        )

        # ── 10. SNAPSHOT DIÁRIO ───────────────────────────────────────────────
        hoje = now_brasilia().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        str_hoje = hoje.strftime("%Y-%m-%d")

        df_snapshot = base[
            [c for c in COLUNAS_SNAPSHOT if c in base.columns]
        ].copy()
        df_snapshot["Data"] = hoje
        df_snapshot["Mês"]  = hoje.strftime("%Y/%m")

        # Idempotência: remove snapshot do dia atual antes de reinserir
        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.base_btg_snapshot_diario
                WHERE CONVERT(DATE, Data) = :hoje
            """), {"hoje": str_hoje})

        salvar_df_otimizado(
            df_snapshot, "base_btg_snapshot_diario", if_exists="append"
        )

        # ── 11. PL HISTÓRICO DIÁRIO ───────────────────────────────────────────
        df_pl_hist = base[
            [c for c in COLUNAS_PL_HISTORICO if c in base.columns]
        ].copy()
        df_pl_hist["Data"] = hoje
        df_pl_hist["Mês"]  = hoje.strftime("%Y/%m")

        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.pl_historico_diario
                WHERE CONVERT(DATE, Data) = :hoje
            """), {"hoje": str_hoje})

        salvar_df_otimizado(
            df_pl_hist, "pl_historico_diario", if_exists="append"
        )

        # ── 12. TABELAS DERIVADAS ─────────────────────────────────────────────
        _atualizar_tipo_clientes(base, engine)

        # ── 13. LOG E RESPOSTA ────────────────────────────────────────────────
        msg = (
            f"base_btg: {len(base)} contas | "
            f"snapshot: {len(df_snapshot)} linhas | "
            f"pl_historico: {len(df_pl_hist)} linhas"
        )
        print(f"[SUCESSO BASE_BTG] {msg}", flush=True)
        registrar_log("BASE_BTG", "Sucesso", len(base), msg)

        return jsonify({
            "status":       "Sucesso",
            "base_btg":     len(base),
            "snapshot":     len(df_snapshot),
            "pl_historico": len(df_pl_hist),
        }), 200

    except Exception as e:
        return erro_interno("BASE_BTG", e)


@app.route("/webhook/nnm", methods=["POST"])
def webhook_nnm():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    try:
        url_download = dados.get("response", {}).get("url") or dados.get("url")
        if not url_download:
            return jsonify({"status": "Recebido sem URL"}), 200

        if not validar_url_download(url_download):
            registrar_log("NNM", "Erro", 0,
                          f"URL bloqueada por política SSRF: {url_download}")
            return jsonify({"erro": "URL não autorizada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()

        df = pd.read_csv(io.StringIO(r.content.decode("utf-8")), sep=";")
        df.rename(columns={"dt_captacao": "data_captacao"}, inplace=True)

        # Remove lançamentos do tipo RS (estorno de saldo — não representa captação)
        if "tipo_lancamento" in df.columns:
            df = df[df["tipo_lancamento"] != "RS"].copy()

        df.dropna(subset=["data_captacao"], inplace=True)

        if df.empty:
            return jsonify({"status": "Sem dados válidos após filtros"}), 200

        # Datas como DATE nativo — formatação fica na view/apresentação
        df["data_captacao"] = pd.to_datetime(
            df["data_captacao"], errors="coerce"
        ).dt.date

        data_min_csv = pd.to_datetime(df["data_captacao"].min())
        data_max_csv = pd.to_datetime(df["data_captacao"].max())
        data_corte_fato = data_max_csv - timedelta(days=DIAS_FATO_NNM)

        str_min   = data_min_csv.strftime("%Y-%m-%d")
        str_max   = data_max_csv.strftime("%Y-%m-%d")
        str_corte = data_corte_fato.strftime("%Y-%m-%d")

        engine = get_engine()

        # ── BACKUP RAW ────────────────────────────────────────────────────────
        df_raw = df.copy()
        df_raw["data_recebimento_webhook"] = now_brasilia()

        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.backup_nnm_raw
                WHERE data_captacao BETWEEN :data_min AND :data_max
            """), {"data_min": str_min, "data_max": str_max})

        salvar_df_otimizado(df_raw, "backup_nnm_raw", if_exists="append")

        # ── TABELA FATO ───────────────────────────────────────────────────────
        colunas_fato = [
            "nr_conta", "data_captacao", "ativo", "mercado", "cge_officer",
            "tipo_lancamento", "descricao", "qtd", "captacao",
            "is_officer_nnm", "is_partner_nnm", "is_channel_nnm", "is_bu_nnm",
            "submercado", "submercado_detalhado"
        ]

        df_fato = df[df["data_captacao"] > data_corte_fato.date()].copy()
        df_fato = df_fato[[c for c in colunas_fato if c in df_fato.columns]].copy()

        for col in ["is_officer_nnm", "is_partner_nnm", "is_channel_nnm", "is_bu_nnm"]:
            if col in df_fato.columns:
                df_fato[col] = (
                    df_fato[col]
                    .map({"t": 1, "f": 0, "True": 1, "False": 0})
                    .fillna(0)
                    .astype(int)
                )

        # ── ENRIQUECIMENTO ────────────────────────────────────────────────────
        # Tipo de captação fixo para entradas via BTG webhook
        df_fato["tipo_captacao"] = "Padrão"

        with engine.connect() as conn:

            # Assessor: cge_officer → times_nova_empresa
            try:
                times_df = pd.read_sql(
                    "SELECT Assessor, [CGE OFFICER] FROM dbo.times_nova_empresa", conn
                )
                times_df["CGE OFFICER"] = times_df["CGE OFFICER"].astype(str).str.strip()
                df_fato["cge_officer"]  = df_fato["cge_officer"].astype(str).str.strip()

                df_fato = df_fato.merge(
                    times_df, left_on="cge_officer", right_on="CGE OFFICER", how="left"
                )
                df_fato.drop(columns=["CGE OFFICER"], inplace=True)

            except Exception as e:
                print(f"[AVISO NNM] Falha ao mapear times_nova_empresa: {e}")
                df_fato["Assessor"] = None

            # Nome e Situação: base_btg
            # Nome já vem diretamente da base_btg — sem necessidade de Excel externo
            try:
                base_ref = pd.read_sql(
                    "SELECT Conta, Nome FROM dbo.base_btg", conn
                )
                base_ref["Conta"] = base_ref["Conta"].astype(str).str.strip()
                df_fato["nr_conta_str"] = df_fato["nr_conta"].astype(str).str.strip()

                df_fato = df_fato.merge(
                    base_ref, left_on="nr_conta_str", right_on="Conta", how="left"
                )
                df_fato["Situação"] = df_fato["Conta"].apply(
                    lambda x: "Ativo" if pd.notnull(x) else "Inativo"
                )
                df_fato.drop(columns=["Conta", "nr_conta_str"], inplace=True)

            except Exception as e:
                print(f"[AVISO NNM] Falha ao mapear base_btg: {e}")
                df_fato["Nome"]    = None
                df_fato["Situação"] = "Desconhecida"

        df_fato["data_upload"] = now_brasilia()

        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.relatorios_nnm_gerencial
                WHERE data_captacao > :corte
            """), {"corte": str_corte})

        salvar_df_otimizado(df_fato, "relatorios_nnm_gerencial", if_exists="append")

        msg = (
            f"Raw: {str_min}→{str_max} ({len(df_raw)} linhas) | "
            f"Fato: {str_corte}→{str_max} ({len(df_fato)} linhas)"
        )
        print(f"[SUCESSO NNM] {msg}")
        registrar_log("NNM", "Sucesso", len(df_fato), msg)

        return jsonify({
            "status": "Sucesso",
            "raw":  {"de": str_min,   "ate": str_max,   "linhas": len(df_raw)},
            "fato": {"de": str_corte, "ate": str_max,   "linhas": len(df_fato)}
        }), 200

    except ValueError as e:
        registrar_log("NNM", "Erro", 0, str(e))
        return jsonify({"erro": str(e)}), 400

    except Exception as e:
        return erro_interno("NNM", e)


@app.route("/webhook/performance", methods=["POST"])
def webhook_performance():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    if not dados:
        return jsonify({"erro": "Payload vazio"}), 400

    conta_id = "Desconhecida"
    try:
        res          = dados.get("response") or dados.get("partnerResponse") or {}
        res          = res if isinstance(res, dict) else {}
        url_download = res.get("url")
        data_ref     = res.get("endDate")
        req_id       = dados.get("idPartnerRequest", "N/A")

        conta_raw = (
            res.get("accountNumber")
            or dados.get("cge")
            or dados.get("accountNumber")
        )
        conta_id = (
            str(conta_raw)
            if (conta_raw and str(conta_raw).lower() != "null")
            else "Desconhecida"
        )

        if not url_download:
            print(f"[AVISO PERFORMANCE] URL não encontrada. Conta: {conta_id} | ID: {req_id}")
            return jsonify({"erro": "URL não encontrada", "conta": conta_id}), 400

        if not validar_url_download(url_download):
            registrar_log("PERFORMANCE", "Erro", 0,
                          f"URL bloqueada por política SSRF: {url_download}")
            return jsonify({"erro": "URL não autorizada"}), 400

        r = requests.get(url_download, stream=True)
        r.raise_for_status()

        arquivos_salvos = 0
        conn_db = pyodbc.connect(CONN_STR)
        cursor  = conn_db.cursor()

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            for nome_arquivo in z.namelist():
                if not nome_arquivo.lower().endswith(".pdf"):
                    continue

                conta_final = (
                    conta_id if conta_id != "Desconhecida"
                    else extrair_conta_do_nome(nome_arquivo)
                )
                pdf_bytes = z.read(nome_arquivo)

                cursor.execute("""
                    MERGE dbo.relatorios_performance_atual AS Target
                    USING (SELECT ? AS ContaVal) AS Source
                        ON Target.conta = Source.ContaVal
                    WHEN MATCHED THEN
                        UPDATE SET
                            arquivo_pdf     = ?,
                            nome_arquivo    = ?,
                            data_referencia = ?,
                            data_upload     = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (conta, arquivo_pdf, nome_arquivo, data_referencia, data_upload)
                        VALUES (?, ?, ?, ?, GETDATE());
                """, (conta_final, pdf_bytes, nome_arquivo, data_ref,
                      conta_final, pdf_bytes, nome_arquivo, data_ref))
                arquivos_salvos += 1

        conn_db.commit()
        conn_db.close()

        print(
            f"[SUCESSO PERFORMANCE] Conta: {conta_id} | "
            f"Salvos: {arquivos_salvos} | Ref: {data_ref} | ID: {req_id}"
        )
        return jsonify({"status": "Processado", "conta": conta_id}), 200

    except Exception as e:
        return erro_interno("PERFORMANCE", e, conta=conta_id)


@app.route("/webhook/custodia", methods=["POST"])
def webhook_custodia():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    try:
        url_download = dados.get("response", {}).get("url")
        if not url_download:
            return jsonify({"status": "Recebido sem URL"}), 200

        if not validar_url_download(url_download):
            registrar_log("CUSTODIA", "Erro", 0,
                          f"URL bloqueada por política SSRF: {url_download}")
            return jsonify({"erro": "URL não autorizada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            nome_csv = z.namelist()[0]
            with z.open(nome_csv) as f:
                df = pd.read_csv(f, sep=",", encoding="latin1", low_memory=False)

        df_raw = df.copy()
        df_raw["data_recebimento_webhook"] = now_brasilia()
        salvar_df_otimizado(df_raw, "backup_custodia_raw", if_exists="replace")

        df_final = df.copy()
        for col in ["referenceDate", "dataInicio", "fixingDate", "dataKnockIn"]:
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(
                    df_final[col], format="%d/%m/%Y", errors="coerce"
                )

        df_final["data_upload"] = now_brasilia()
        salvar_df_otimizado(df_final, "relatorios_custodia", if_exists="replace")

        msg = f"Importação Custódia concluída. Linhas: {len(df_final)}"
        print(f"[SUCESSO CUSTODIA] {msg}", flush=True)
        registrar_log("CUSTODIA", "Sucesso", len(df_final), msg)
        return jsonify({"status": "Sucesso", "linhas": len(df_final)}), 200

    except Exception as e:
        return erro_interno("CUSTODIA", e)


@app.route("/webhook/offshore", methods=["POST"])
def webhook_offshore():
    """
    Recebe o arquivo AuC Offshore.xlsx via multipart/form-data.
    Atualiza nnm_offshore (para a view de captação) e auc_offshore
    (para mapeamento de assessor em outros processos).
    """
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    if "file" not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado"}), 400

    arquivo = request.files["file"]
    try:
        df_offshore = pd.read_excel(arquivo, sheet_name="NNM Offshore")

        renomear = {
            "Data NNM": "data_captacao",
            "Conta":    "nr_conta",
            "NNM BRL":  "captacao"
        }
        df_offshore.rename(columns=renomear, inplace=True)
        df_offshore.drop(columns=["NNM USD"], errors="ignore", inplace=True)

        df_offshore["nr_conta"]      = df_offshore["nr_conta"].astype(str).str.strip()
        df_offshore["data_captacao"] = pd.to_datetime(
            df_offshore["data_captacao"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
        df_offshore["descricao"]     = "OFFSHORE"
        df_offshore["tipo_captacao"] = "Offshore"
        df_offshore["mercado"]       = "Offshore"
        df_offshore["data_upload"]   = now_brasilia()

        df_offshore.dropna(subset=["data_captacao", "nr_conta"], inplace=True)

        # Assessor via auc_offshore
        engine = get_engine()
        try:
            with engine.connect() as conn:
                auc = pd.read_sql(
                    "SELECT Conta, Assessor FROM dbo.auc_offshore", conn
                )
            auc["Conta"] = auc["Conta"].astype(str).str.strip()
            df_offshore = df_offshore.merge(
                auc, left_on="nr_conta", right_on="Conta", how="left"
            )
            df_offshore.drop(columns=["Conta"], errors="ignore", inplace=True)
        except Exception as e:
            print(f"[AVISO OFFSHORE] auc_offshore não carregada: {e}")

        salvar_df_otimizado(df_offshore, "nnm_offshore", if_exists="replace")

        registrar_log("OFFSHORE_NNM", "Sucesso", len(df_offshore),
                      f"{len(df_offshore)} linhas offshore importadas")
        return jsonify({"status": "Sucesso", "linhas": len(df_offshore)}), 200

    except Exception as e:
        return erro_interno("OFFSHORE_NNM", e)

# 8. ADMIN — uploads manuais pontuais

@app.route("/admin/times", methods=["POST"])
def upload_times():
    """
    Atualiza times_nova_empresa a partir do arquivo times_atria.xlsx.
    Deve ser chamado sempre que houver mudança no time de assessores.
    """
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    if "file" not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado"}), 400

    try:
        arquivo = request.files["file"]
        df = pd.read_excel(arquivo)

        colunas_esperadas = ["Assessor", "CGE OFFICER"]
        faltando = [c for c in colunas_esperadas if c not in df.columns]
        if faltando:
            return jsonify({"erro": f"Colunas esperadas ausentes: {faltando}"}), 400

        df["Assessor"] = df["Assessor"].astype(str).str.upper().str.strip()

        salvar_df_otimizado(
            df, "times_nova_empresa",
            col_pk="Assessor", if_exists="replace"
        )

        registrar_log("UPLOAD_TIMES", "Sucesso", len(df),
                      f"{len(df)} assessores carregados")
        return jsonify({"status": "Sucesso", "linhas": len(df)}), 200

    except Exception as e:
        return erro_interno("UPLOAD_TIMES", e)


@app.route("/admin/offshore", methods=["POST"])
def upload_offshore():
    """
    Atualiza pl_offshore e auc_offshore a partir do arquivo AuC Offshore.xlsx.
    pl_offshore alimenta o merge do webhook/basebtg.
    auc_offshore alimenta o mapeamento de assessor no webhook/offshore.
    """
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    if "file" not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado"}), 400

    try:
        arquivo = request.files["file"]
        df = pd.read_excel(arquivo, sheet_name="AuC Offshore")

        colunas_necessarias = ["Nome", "Conta", "AUC BRL", "Assessor"]
        faltando = [c for c in colunas_necessarias if c not in df.columns]
        if faltando:
            return jsonify({"erro": f"Colunas ausentes: {faltando}"}), 400

        df["Conta"]    = df["Conta"].astype(str).str.strip()
        df["Assessor"] = df["Assessor"].astype(str).str.upper().str.strip()

        # pl_offshore — usado no merge do webhook/basebtg
        pl_offshore = df[["Nome", "Conta", "AUC BRL", "Assessor"]].copy()
        pl_offshore.rename(columns={"AUC BRL": "PL Total"}, inplace=True)
        salvar_df_otimizado(
            pl_offshore, "pl_offshore",
            col_pk="Conta", if_exists="replace"
        )

        # auc_offshore — mapeamento de assessor para contas offshore
        auc_offshore = df[["Conta", "Assessor"]].copy()
        salvar_df_otimizado(
            auc_offshore, "auc_offshore",
            col_pk="Conta", if_exists="replace"
        )

        registrar_log("UPLOAD_OFFSHORE", "Sucesso", len(df),
                      f"{len(df)} contas offshore carregadas")
        return jsonify({"status": "Sucesso", "linhas": len(df)}), 200

    except Exception as e:
        return erro_interno("UPLOAD_OFFSHORE", e)

# 9. UTILITÁRIOS

@app.route("/meu-ip", methods=["GET"])
def get_ip():
    try:
        return jsonify({"ip_render": requests.get("https://api.ipify.org").text})
    except Exception:
        return jsonify({"erro": "Falha ao obter IP"}), 500

# 10. ENTRYPOINT

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)