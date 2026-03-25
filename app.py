import os
import io
import re
import hmac
import uuid
import math
import time
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
URL_POSICAO_PARTNER = "https://api.btgpactual.com/api/v1/position/partner"
URL_POSICAO_REFRESH = "https://api.btgpactual.com/api/v1/position/refresh"
URL_SALDO_CC        = "https://api.btgpactual.com/api-account-balance/api/v1/account-balance/list"

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
DIAS_FATO_NNM = 4
JANELA_CAPTACAO_DIAS = 4

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
# Mapeamento: nome da API (base) → nome da coluna no banco
COLUNAS_PL_HISTORICO = [
    "Conta", "Assessor", "PL Total", "PL Declarado",
    "Faixa Cliente", "Data Vínculo",
    "pl_conta_corrente", "pl_fundos", "pl_renda_fixa",
    "pl_renda_variavel", "pl_previdencia", "pl_derivativos",
]
RENAME_PL_HISTORICO = {
    "pl_conta_corrente": "Conta Corrente",
    "pl_fundos":         "Fundos",
    "pl_renda_fixa":     "Renda Fixa",
    "pl_renda_variavel":  "Renda Variável",
    "pl_previdencia":    "Previdência",
    "pl_derivativos":    "Derivativos",
}

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
    import traceback
    tb = traceback.format_exc()
    detalhe = f"Conta: {conta} | {str(e)}\n{tb}" if conta else f"{str(e)}\n{tb}"
    print(f"[ERRO CRÍTICO {atividade}] {detalhe}")
    registrar_log(atividade, "Erro", 0, detalhe[:2000])
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
                "SELECT DISTINCT CONTA AS nr_conta FROM dbo.captacao_historico", conn
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
                "SELECT DISTINCT CONTA AS nr_conta FROM dbo.captacao_historico "
                "WHERE [TIPO DE CAPTACAO] = 'Saída de conta'",
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
                ORDER BY Data
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
        debitos["CAPTAÇÃO"]         = debitos["PL Total"] * -1
        debitos["DATA"]             = debitos["Data"]
        debitos["TIPO DE CAPTACAO"] = "Saída de conta"
        debitos["MERCADO"]          = "Saída de conta"
        debitos["Situacao"]         = "Inativo"

        # Assessor atual
        with engine.connect() as conn:
            base_ref = pd.read_sql("SELECT Conta, Nome, Assessor FROM dbo.base_btg", conn)
        base_ref["Conta"] = base_ref["Conta"].astype(str).str.strip()

        debitos["nr_conta"] = debitos["nr_conta"].astype(str).str.strip()
        debitos = debitos.merge(
            base_ref.rename(columns={"Conta": "nr_conta", "Assessor": "Assessor_ref"}),
            on="nr_conta", how="left"
        )
        debitos["Assessor"] = debitos["Assessor_ref"].fillna("")
        debitos["CONTA"]    = debitos["nr_conta"]

        colunas_saida = ["DATA", "CONTA", "CAPTAÇÃO", "Assessor", "TIPO DE CAPTACAO", "MERCADO", "Situacao", "Nome"]
        debitos = debitos[[c for c in colunas_saida if c in debitos.columns]]

        if debitos.empty:
            registrar_log(atividade, "Sucesso", 0, "Nenhuma saída com PL histórico encontrada")
            return

        with engine.begin() as conn:
            placeholders = ", ".join([f"'{c}'" for c in debitos["CONTA"].tolist()])
            conn.execute(text(f"""
                DELETE FROM dbo.captacao_historico
                WHERE CONTA IN ({placeholders})
                AND [TIPO DE CAPTACAO] = 'Saída de conta'
            """))

        salvar_df_otimizado(debitos, "captacao_historico", if_exists="append")

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

        if movimentacoes.empty:
            registrar_log(atividade, "Sucesso", 0,
                          f"Nenhuma conta nova ou removida em {data_hoje}")
            print(f"[ENTRADAS_SAIDAS] Nenhuma movimentacao em {data_hoje}")
            return

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
    atividade = "PREVIA_RECEITA"
    try:
        primeiro_dia_mes = pd.Timestamp(
            datetime.now(TZ_BRASILIA).replace(
                day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=None
            )
        )

        engine = get_engine()

        # --- Carrega histórico e salva META - ROA do mês atual ---
        try:
            hist = pd.read_sql(
                f"SELECT * FROM {SCHEMA_DEFAULT}.previa_receita_nova", engine
            )
            if not hist.empty:
                hist["Data"] = pd.to_datetime(hist["Data"], errors="coerce")

                # Salva META - ROA antes de remover o mês atual
                metas_mes_atual = hist[hist["Data"] == primeiro_dia_mes][
                    ["Assessor", "Categoria - Acompanhamento Next", "META - ROA"]
                ].copy()

                hist = hist[hist["Data"] != primeiro_dia_mes]
            else:
                metas_mes_atual = pd.DataFrame()
        except Exception:
            hist = pd.DataFrame()
            metas_mes_atual = pd.DataFrame()

        # --- Coleta dos assessores ---
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

        # --- Restaura META - ROA do mês atual ---
        if not metas_mes_atual.empty:
            previa_receita = previa_receita.merge(
                metas_mes_atual.rename(columns={"META - ROA": "META - ROA_salva"}),
                on=["Assessor", "Categoria - Acompanhamento Next"],
                how="left"
            )
            mask = previa_receita["META - ROA_salva"].notna()
            previa_receita.loc[mask, "META - ROA"] = previa_receita.loc[mask, "META - ROA_salva"]
            previa_receita.drop(columns=["META - ROA_salva"], inplace=True)
            print(f"   -> META - ROA restaurada para {mask.sum()} linhas.")

        previa_final = pd.concat([hist, previa_receita], axis=0, ignore_index=True)

        # Zera nulos só em colunas numéricas — não destrói datas
        previa_final.loc[previa_final["META - VOLUME"] == "-", "META - VOLUME"] = 0
        colunas_num = previa_final.select_dtypes(include="number").columns
        previa_final[colunas_num] = previa_final[colunas_num].fillna(0)

        # Cast correto para SQL Server não salvar como string
        previa_final["Data"] = pd.to_datetime(previa_final["Data"]).astype("datetime64[ms]")
        previa_final["Hora Atualizado"] = pd.to_datetime(
            previa_final["Hora Atualizado"], errors="coerce"
        ).astype("datetime64[ms]")

        salvar_df_otimizado(
            previa_final, "previa_receita_nova",
            if_exists="replace", schema=SCHEMA_DEFAULT
        )

        # --- Agregado por assessor ---
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

                # Salva e restaura META - ROA agregada do mês atual
                metas_agg_mes_atual = hist_agg[
                    hist_agg["Data"] == primeiro_dia_mes
                ][["Assessor", "META - ROA"]].copy().rename(
                    columns={"META - ROA": "META - ROA_salva"}
                )

                hist_agg = hist_agg[hist_agg["Data"] != primeiro_dia_mes]

                if not metas_agg_mes_atual.empty:
                    previa_agg = previa_agg.merge(
                        metas_agg_mes_atual, on="Assessor", how="left"
                    )
                    mask_agg = previa_agg["META - ROA_salva"].notna()
                    previa_agg.loc[mask_agg, "META - ROA"] = (
                        previa_agg.loc[mask_agg, "META - ROA_salva"]
                    )
                    previa_agg.drop(columns=["META - ROA_salva"], inplace=True)

            previa_final_agg = pd.concat(
                [hist_agg, previa_agg], axis=0, ignore_index=True
            )
        except Exception:
            previa_final_agg = previa_agg

        # Cast correto para o agregado também
        previa_final_agg["Data"] = pd.to_datetime(
            previa_final_agg["Data"]
        ).astype("datetime64[ms]")

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

def _executar_posicao():
    """
    Busca posições de todas as contas via API BTG (síncrono via /partner).
    Fluxo: refresh → sleep 90s → partner → download ZIP → transforma → grava posicao.
    """
    atividade = "POSICAO"
    try:
        token = get_btg_token()
        if not token:
            registrar_log(atividade, "Erro", 0, "Falha ao obter token BTG")
            return

        headers_btg = {
            "x-id-partner-request": str(uuid.uuid4()),
            "access_token": token,
            "Content-Type": "application/json"
        }

        # 1. Dispara atualização do cache no BTG (async — fire & forget)
        r_refresh = requests.get(URL_POSICAO_REFRESH, headers=headers_btg, timeout=30)
        print(f"[POSICAO] Refresh status: {r_refresh.status_code}", flush=True)

        # 2. Aguarda geração do arquivo (BTG leva ~60-90s)
        time.sleep(90)

        # 3. Busca URL do ZIP (síncrono — lê do cache atualizado)
        headers_btg["x-id-partner-request"] = str(uuid.uuid4())
        r_partner = requests.get(URL_POSICAO_PARTNER, headers=headers_btg, timeout=30)
        r_partner.raise_for_status()
        dados = r_partner.json()
        url_zip = (dados.get("response") or {}).get("url") or dados.get("url")

        if not url_zip:
            registrar_log(atividade, "Erro", 0, f"URL do ZIP não retornada: {dados}")
            return

        if not validar_url_download(url_zip):
            registrar_log(atividade, "Erro", 0, f"URL nao autorizada: {url_zip}")
            return

        # 4. Baixa e extrai CSV do ZIP
        r_zip = requests.get(url_zip, timeout=120)
        r_zip.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(r_zip.content)) as z:
            conteudo = z.read(z.namelist()[0])

        df = None
        for encoding in ["utf-8", "latin1", "cp1252"]:
            try:
                df = pd.read_csv(io.BytesIO(conteudo), sep=None, engine="python", encoding=encoding)
                break
            except Exception:
                continue

        if df is None or df.empty:
            registrar_log(atividade, "Erro", 0, "CSV vazio ou nao parseavel")
            return

        # 5. Mapeamento de colunas API → schema da tabela posicao
        # TODO: preencher após rodar GET /trigger/inspecionar-posicao e inspecionar colunas reais
        RENAME_POSICAO = {
            # "coluna_api": "Coluna DB",
        }
        df.rename(columns=RENAME_POSICAO, inplace=True)
        df.drop(columns=["ESCRITÓRIO"], errors="ignore", inplace=True)

        # 6. Converte VENCIMENTO para datetime se existir
        if "VENCIMENTO" in df.columns:
            df["VENCIMENTO"] = pd.to_datetime(df["VENCIMENTO"], errors="coerce")

        # 7. Merge com base_btg para adicionar Assessor
        engine = get_engine()
        with engine.connect() as conn:
            base_ref = pd.read_sql("SELECT Conta, Assessor FROM dbo.base_btg", conn)
        base_ref["Conta"] = base_ref["Conta"].astype(str)
        if "Conta" in df.columns:
            df["Conta"] = df["Conta"].astype(str)
            df = df.merge(base_ref, on="Conta", how="left")

        # 8. Adiciona Setor e Subsetor via setores.xlsx
        try:
            setores = pd.read_excel(r"C:\Scripts\setores_ativos\setores.xlsx")
            for col_match in ["Ativo", "Emissor"]:
                if col_match in df.columns and col_match in setores.columns:
                    df = df.merge(
                        setores[[col_match, "Setor", "Subsetor"]].drop_duplicates(col_match),
                        on=col_match, how="left"
                    )
                    break
        except Exception as e_set:
            print(f"[POSICAO] Aviso: nao foi possivel carregar setores — {e_set}")

        # 9. Grava no banco (REPLACE total — snapshot D0)
        salvar_df_otimizado(df, "posicao", if_exists="replace")

        msg = f"{len(df)} posicoes gravadas"
        print(f"[SUCESSO POSICAO] {msg}", flush=True)
        registrar_log(atividade, "Sucesso", len(df), msg)

    except Exception as e:
        registrar_log(atividade, "Erro", 0, str(e))
        print(f"[ERRO CRITICO POSICAO] {e}")


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


@app.route("/trigger/inspecionar-posicao", methods=["GET"])
def trigger_inspecionar_posicao():
    """
    Endpoint de inspeção: baixa o ZIP de posição do BTG e retorna
    as colunas e uma amostra das primeiras linhas do CSV.
    Usar apenas para mapear a estrutura antes de implementar o ETL.
    """
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    try:
        token = get_btg_token()

        # Solicita atualização do cache antes de baixar
        r_refresh = requests.get(
            URL_POSICAO_REFRESH,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30
        )
        print(f"[POSICAO] Refresh status: {r_refresh.status_code}", flush=True)

        # Obtém URL do ZIP
        r = requests.get(
            URL_POSICAO_PARTNER,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30
        )
        r.raise_for_status()
        dados = r.json()
        url_zip = dados.get("url")

        if not url_zip:
            return jsonify({"erro": "URL do ZIP não retornada", "resposta_btg": dados}), 400

        if not validar_url_download(url_zip):
            return jsonify({"erro": "URL não autorizada"}), 400

        # Baixa e abre o ZIP
        r_zip = requests.get(url_zip, timeout=60)
        r_zip.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(r_zip.content)) as z:
            arquivos = z.namelist()
            resultado = {}
            for nome_arquivo in arquivos:
                with z.open(nome_arquivo) as f:
                    # Tenta diferentes encodings e separadores
                    conteudo = f.read()
                    for encoding in ["utf-8", "latin1", "cp1252"]:
                        try:
                            df = pd.read_csv(
                                io.BytesIO(conteudo),
                                sep=None, engine="python",
                                encoding=encoding,
                                nrows=3
                            )
                            resultado[nome_arquivo] = {
                                "encoding": encoding,
                                "colunas": list(df.columns),
                                "amostra": df.head(2).to_dict(orient="records")
                            }
                            break
                        except Exception:
                            continue
                    else:
                        resultado[nome_arquivo] = {"erro": "Não foi possível parsear o arquivo"}

        return jsonify({
            "arquivos_no_zip": arquivos,
            "conteudo": resultado,
            "metadata_btg": {k: v for k, v in dados.items() if k != "url"}
        }), 200

    except Exception as e:
        return erro_interno("INSPECIONAR_POSICAO", e)


@app.route("/trigger/saldo-cc", methods=["GET"])
def trigger_saldo_cc():
    """
    Busca saldo de todas as contas em tempo real via API BTG e grava saldo_conta_corrente.
    Chamada sincrona — retorna resultado direto.
    """
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    atividade = "SALDO_CC"
    try:
        token = get_btg_token()
        if not token:
            registrar_log(atividade, "Erro", 0, "Falha ao obter token BTG")
            return jsonify({"erro": "Falha ao autenticar"}), 502

        headers_btg = {
            "x-id-partner-request": str(uuid.uuid4()),
            "access_token": token,
            "Content-Type": "application/json"
        }

        r = requests.get(URL_SALDO_CC, headers=headers_btg, timeout=30)
        r.raise_for_status()

        accounts = r.json().get("accounts", [])
        if not accounts:
            registrar_log(atividade, "Sucesso", 0, "Nenhuma conta retornada pela API")
            return jsonify({"status": "ok", "linhas": 0}), 200

        df = pd.DataFrame(accounts)
        df.rename(columns={"account": "Conta", "balance": "SALDO"}, inplace=True)
        df["Conta"] = df["Conta"].astype(str).str.strip()
        df["SALDO"] = pd.to_numeric(df["SALDO"], errors="coerce")

        # Adiciona Assessor via base_btg
        engine = get_engine()
        with engine.connect() as conn:
            base_ref = pd.read_sql("SELECT Conta, Assessor FROM dbo.base_btg", conn)
        base_ref["Conta"] = base_ref["Conta"].astype(str)
        df = df.merge(base_ref, on="Conta", how="left")

        # Mantém apenas colunas da tabela destino
        df = df[["Conta", "SALDO", "Assessor"]]

        salvar_df_otimizado(df, "saldo_conta_corrente", if_exists="replace", col_pk="Conta")

        msg = f"{len(df)} contas gravadas"
        registrar_log(atividade, "Sucesso", len(df), msg)
        return jsonify({"status": "ok", "linhas": len(df)}), 200

    except Exception as e:
        return erro_interno(atividade, e)


@app.route("/trigger/posicao", methods=["GET"])
def trigger_posicao():
    """Atualiza tabela posicao via API BTG (refresh → 90s → partner → REPLACE)."""
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    thread = threading.Thread(target=_executar_posicao, daemon=True)
    thread.start()
    return jsonify({"status": "iniciado", "info": "refresh → 90s → download → posicao"}), 202


@app.route("/trigger/previa-receita", methods=["GET"])
def trigger_previa_receita():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    thread = threading.Thread(target=_executar_previa_receita, daemon=True)
    thread.start()
    return jsonify({
        "status": "iniciado",
        "mensagem": "Atualizacao da Previa Receita em andamento"
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
        df_pl_hist.rename(columns=RENAME_PL_HISTORICO, inplace=True)
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

        # ── 12b. PL BASE (histórico mensal) ───────────────────────────────────
        pl_base_linhas = 0
        try:
            primeiro_dia_mes = hoje.strftime("%Y-%m-01")

            # Onshore: extrai Assessor, CONTA, PL do base atual
            pl_hoje = base[["Assessor", "Conta", "PL Total"]].copy()
            pl_hoje.rename(columns={"Conta": "CONTA", "PL Total": "PL"}, inplace=True)
            pl_hoje["Mês"] = hoje.strftime("%Y-%m-%d")
            pl_hoje["PL"] = pl_hoje["PL"].fillna(0)
            pl_hoje["Assessor"] = pl_hoje["Assessor"].astype(str).str.upper()

            # Histórico anterior (preserva meses fechados)
            with engine.connect() as conn:
                pl_base_hist = pd.read_sql("SELECT * FROM dbo.[PL Base]", conn)
            pl_base_hist = pl_base_hist[pl_base_hist["Mês"] < primeiro_dia_mes]

            pl_onshore = pd.concat([pl_base_hist, pl_hoje], axis=0, ignore_index=True)

            # Offshore
            with engine.connect() as conn:
                pl_offshore_hist = pd.read_sql(
                    "SELECT * FROM dbo.offshore_adicionar_pl_mes_vigente", conn
                )
                pl_offshore = pd.read_sql(
                    "SELECT Conta, [PL Total], Assessor FROM dbo.pl_offshore", conn
                )

            pl_offshore_hist = pl_offshore_hist[
                pl_offshore_hist["Mês"] < primeiro_dia_mes
            ]
            pl_offshore_hist["Mês"] = pd.to_datetime(
                pl_offshore_hist["Mês"]
            ).dt.strftime("%Y-%m-%d")

            pl_offshore["Mês"] = hoje.strftime("%Y-%m-%d")
            pl_offshore.rename(
                columns={"Conta": "CONTA", "PL Total": "PL"}, inplace=True
            )

            offshore_mes_vigente = pd.concat(
                [pl_offshore, pl_offshore_hist], axis=0, ignore_index=True
            )
            salvar_df_otimizado(
                offshore_mes_vigente, "offshore_adicionar_pl_mes_vigente",
                if_exists="replace"
            )

            # Concat final onshore + offshore
            pl_final = pd.concat([pl_onshore, offshore_mes_vigente], axis=0, ignore_index=True)

            # Correções de assessor
            correcoes_pl = {
                "RODRIGO DE MELLO DELIA":    "RODRIGO DE MELLO D'ELIA",
                "RODRIGO DE MELLO D?ELIA":   "RODRIGO DE MELLO D'ELIA",
                "ROSANA PAVANI":             "ROSANA APARECIDA PAVANI DA SILVA",
                "FERNANDO DOMINGUES":        "FERNANDO DOMINGUES DA SILVA",
                "MURILO LUIZ SILVA GINO":    "IZADORA VILLELA FREITAS",
            }
            pl_final["Assessor"] = pl_final["Assessor"].replace(correcoes_pl)

            pl_final["CONTA"] = pl_final["CONTA"].astype(str)
            pl_final["Mês"] = pd.to_datetime(pl_final["Mês"])
            pl_final.drop_duplicates(subset=["CONTA", "Mês"], keep="first", inplace=True)

            salvar_df_otimizado(pl_final, "PL Base", if_exists="replace")
            pl_base_linhas = len(pl_final)
            print(f"[BASE_BTG] PL Base atualizado: {pl_base_linhas} linhas", flush=True)

        except Exception as e:
            print(f"[AVISO BASE_BTG] PL Base não atualizado: {e}", flush=True)

        # ── 13. LOG E RESPOSTA ────────────────────────────────────────────────
        msg = (
            f"base_btg: {len(base)} contas | "
            f"snapshot: {len(df_snapshot)} linhas | "
            f"pl_historico: {len(df_pl_hist)} linhas | "
            f"pl_base: {pl_base_linhas} linhas"
        )
        print(f"[SUCESSO BASE_BTG] {msg}", flush=True)
        registrar_log("BASE_BTG", "Sucesso", len(base), msg)

        # ── 14. ENCADEAMENTO AUTOMÁTICO ───────────────────────────────────────
        # Dispara entradas/saídas em thread após base estar atualizada.
        # Retorna 200 imediatamente — o processo derivado roda em background.
        thread = threading.Thread(target=_executar_entradas_saidas, daemon=True)
        thread.start()

        return jsonify({
            "status":       "Sucesso",
            "base_btg":     len(base),
            "snapshot":     len(df_snapshot),
            "pl_historico": len(df_pl_hist),
            "pl_base":      pl_base_linhas,
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

        df["data_captacao"] = pd.to_datetime(df["data_captacao"], errors="coerce")
        df.dropna(subset=["data_captacao"], inplace=True)

        data_max_csv = df["data_captacao"].max()
        str_max      = data_max_csv.strftime("%Y-%m-%d")
        str_min      = df["data_captacao"].min().strftime("%Y-%m-%d")

        engine = get_engine()

        # ── 1. BACKUP RAW (janela 10 dias) ────────────────────────────────────
        df_raw = df.copy()
        df_raw["data_recebimento_webhook"] = now_brasilia()
        str_corte_backup = (data_max_csv - timedelta(days=10)).strftime("%Y-%m-%d")

        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.backup_nnm_raw
                WHERE data_captacao > :corte
            """), {"corte": str_corte_backup})

        salvar_df_otimizado(df_raw, "backup_nnm_raw", if_exists="append")

        # ── 2. MONTA NNM PADRÃO ───────────────────────────────────────────────
        # Corte = D-2 em relação ao max do CSV para garantir que dias parciais
        # (CSV enviado às 15h) sejam reprocessados por completo nas execuções seguintes.
        str_hoje   = now_brasilia().strftime("%Y-%m-%d")
        data_corte = (data_max_csv - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        str_corte  = data_corte.strftime("%Y-%m-%d")

        df_nnm = df[df["data_captacao"].dt.date >= data_corte.date()].copy()

        # Renomeia para padrão captacao_historico
        df_nnm.rename(columns={
            "nr_conta":      "CONTA",
            "data_captacao": "DATA",
            "captacao":      "CAPTAÇÃO",
            "mercado":       "MERCADO",
        }, inplace=True)

        df_nnm["CONTA"]           = df_nnm["CONTA"].astype(str).str.strip()
        df_nnm["TIPO DE CAPTACAO"] = "Padrão"

        # Assessor via cge_officer → times_nova_empresa
        with engine.connect() as conn:
            times_df  = pd.read_sql(
                "SELECT Assessor, [CGE OFFICER] FROM dbo.times_nova_empresa", conn
            )
            times_df.columns = [c.strip() for c in times_df.columns]
            if "assessor" in times_df.columns and "Assessor" not in times_df.columns:
                times_df.rename(columns={"assessor": "Assessor"}, inplace=True)
            base_ref  = pd.read_sql("SELECT Conta, Nome, Assessor FROM dbo.base_btg", conn)
            migracoes = pd.read_sql(
                text("SELECT CONTA, DATA, [CAPTAÇÃO], Assessor FROM dbo.migracoes_btg "
                     "WHERE DATA >= :corte"),
                conn, params={"corte": str_corte}
            )
            offshore  = pd.read_sql(
                text("SELECT nr_conta AS CONTA, data_captacao AS DATA, captacao AS [CAPTAÇÃO], Assessor "
                     "FROM dbo.nnm_offshore "
                     "WHERE data_captacao >= :corte"),
                conn, params={"corte": str_corte}
            )
            entradas_saidas = pd.read_sql(
                "SELECT Conta AS CONTA, [Mês de entrada/saída] FROM dbo.Entradas_e_saidas_consolidado",
                conn
            )

        times_df["CGE OFFICER"] = times_df["CGE OFFICER"].astype(str).str.strip()

        if "cge_officer" in df_nnm.columns:
            df_nnm["cge_officer"] = df_nnm["cge_officer"].astype(str).str.strip()
            df_nnm = df_nnm.merge(
                times_df, left_on="cge_officer", right_on="CGE OFFICER", how="left"
            )
            df_nnm.drop(columns=["CGE OFFICER"], errors="ignore", inplace=True)
        else:
            df_nnm["Assessor"] = None

        if "Assessor" not in df_nnm.columns:
            df_nnm["Assessor"] = None

        colunas_nnm = ["DATA", "CONTA", "CAPTAÇÃO", "Assessor", "TIPO DE CAPTACAO", "MERCADO"]
        df_nnm = df_nnm[[c for c in colunas_nnm if c in df_nnm.columns]].copy()
        for c in colunas_nnm:
            if c not in df_nnm.columns:
                df_nnm[c] = None

        # ── 3. OFFSHORE D-0 ───────────────────────────────────────────────────
        if not offshore.empty:
            offshore["DATA"]            = pd.to_datetime(offshore["DATA"], errors="coerce")
            offshore["CONTA"]           = offshore["CONTA"].astype(str).str.strip()
            offshore["TIPO DE CAPTACAO"] = "Offshore"
            offshore["MERCADO"]         = "Offshore"
            if "Assessor" not in offshore.columns:
                offshore["Assessor"] = None
            offshore = offshore[["DATA", "CONTA", "CAPTAÇÃO", "Assessor", "TIPO DE CAPTACAO", "MERCADO"]]

        # ── 4. MIGRAÇÕES BTG D-0 ──────────────────────────────────────────────
        CONTAS_HARDCODED = {"590732", "299305", "5173757", "5152837", "5149832", "5917705", "15296593"}
        if not migracoes.empty:
            migracoes["CONTA"]            = migracoes["CONTA"].astype(str).str.strip()
            migracoes["DATA"]             = pd.to_datetime(migracoes["DATA"], errors="coerce")
            migracoes["TIPO DE CAPTACAO"] = "Migração BTG"
            migracoes["MERCADO"]          = "Migração BTG"
            if "Assessor" not in migracoes.columns:
                migracoes["Assessor"] = None
            migracoes = migracoes[["DATA", "CONTA", "CAPTAÇÃO", "Assessor", "TIPO DE CAPTACAO", "MERCADO"]]

        # ── 5. CONCAT NNM + OFFSHORE + MIGRAÇÕES ─────────────────────────────
        partes = [df_nnm]
        if not offshore.empty:
            partes.append(offshore)
        if not migracoes.empty:
            partes.append(migracoes)

        captacao_hoje = pd.concat(partes, axis=0, ignore_index=True)
        captacao_hoje["CONTA"]   = captacao_hoje["CONTA"].astype(str).str.strip()
        captacao_hoje["CAPTAÇÃO"] = pd.to_numeric(captacao_hoje["CAPTAÇÃO"], errors="coerce").fillna(0)

        # ── 6. SITUAÇÃO ATIVO/INATIVO ─────────────────────────────────────────
        base_ref["Conta"] = base_ref["Conta"].astype(str).str.strip()
        contas_ativas_set = set(base_ref["Conta"])

        captacao_hoje["Situacao"] = captacao_hoje["CONTA"].apply(
            lambda x: "Ativo" if x in contas_ativas_set else "Inativo"
        )

        # ── 7. DÉBITOS DE SAÍDA (contas inativas) ─────────────────────────────
        contas_inativas = captacao_hoje[captacao_hoje["Situacao"] == "Inativo"] \
            .drop_duplicates(subset="CONTA")

        # Busca PL apenas das contas inativas — evita carregar tabela inteira
        pl_hist = pd.DataFrame()
        if not contas_inativas.empty:
            lista_inativas = "', '".join(contas_inativas["CONTA"].tolist())
            with engine.connect() as conn:
                pl_hist = pd.read_sql(
                    text(f"SELECT Conta AS CONTA, [PL Total], Data "
                         f"FROM dbo.pl_historico_diario "
                         f"WHERE Conta IN ('{lista_inativas}')"),
                    conn
                )
            pl_hist["CONTA"] = pl_hist["CONTA"].astype(str).str.strip()
            pl_hist["Data"]  = pd.to_datetime(pl_hist["Data"], errors="coerce")

        debitos = []
        for _, row in contas_inativas.iterrows():
            conta = row["CONTA"]
            pl_conta = pl_hist[pl_hist["CONTA"] == conta].sort_values("Data")
            if pl_conta.empty:
                continue
            ultimo = pl_conta.iloc[-1]
            debitos.append({
                "CONTA":            conta,
                "CAPTAÇÃO":         float(ultimo["PL Total"]) * -1,
                "Assessor":         row["Assessor"],
                "Situacao":         "Inativo",
                "TIPO DE CAPTACAO": "Saída de conta",
                "MERCADO":          "Saída de conta",
                "_data_pl":         ultimo["Data"],
            })

        if debitos:
            df_debitos = pd.DataFrame(debitos)

            # Usa data oficial de saída de Entradas_e_saidas_consolidado
            entradas_saidas["CONTA"] = entradas_saidas["CONTA"].astype(str).str.strip()
            entradas_saidas = entradas_saidas.drop_duplicates("CONTA", keep="last")
            df_debitos = df_debitos.merge(entradas_saidas, on="CONTA", how="left")
            df_debitos["Mês de entrada/saída"] = pd.to_datetime(
                df_debitos["Mês de entrada/saída"], errors="coerce"
            )
            df_debitos["DATA"] = df_debitos["Mês de entrada/saída"].fillna(df_debitos["_data_pl"])
            df_debitos.drop(columns=["_data_pl", "Mês de entrada/saída"], inplace=True)
            df_debitos = df_debitos[df_debitos["DATA"].notna()]
            df_debitos = df_debitos[df_debitos["DATA"].dt.date >= data_corte.date()]

            captacao_hoje = pd.concat([captacao_hoje, df_debitos], axis=0, ignore_index=True)

        # ── 8. ATUALIZA ASSESSOR ATUAL ────────────────────────────────────────
        assessor_atual = base_ref[["Conta", "Assessor"]].rename(
            columns={"Conta": "CONTA", "Assessor": "Assessor_atual"}
        )
        captacao_hoje = captacao_hoje.merge(assessor_atual, on="CONTA", how="left")
        captacao_hoje["Assessor"] = captacao_hoje["Assessor_atual"].fillna(captacao_hoje["Assessor"])
        captacao_hoje.drop(columns=["Assessor_atual"], inplace=True)
        captacao_hoje["Assessor"] = captacao_hoje["Assessor"].astype(str).str.upper()
        captacao_hoje = aplicar_correcoes_assessor(captacao_hoje)

        # ── 9. ADICIONA NOME ──────────────────────────────────────────────────
        nomes = base_ref[["Conta", "Nome"]].rename(columns={"Conta": "CONTA"})
        nomes.drop_duplicates("CONTA", inplace=True)
        if "Nome" in captacao_hoje.columns:
            captacao_hoje.drop(columns=["Nome"], inplace=True)
        captacao_hoje = captacao_hoje.merge(nomes, on="CONTA", how="left")

        captacao_hoje["DATA"] = pd.to_datetime(captacao_hoje["DATA"], errors="coerce")

        # ── 10. SALVA EM captacao_historico (deleta D-1 e reinsere) ─────────────
        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.captacao_historico
                WHERE CONVERT(DATE, DATA) >= :corte
            """), {"corte": str_corte})

        salvar_df_otimizado(captacao_hoje, "captacao_historico", if_exists="append")

        msg = (
            f"Raw backup: {str_min}→{str_max} ({len(df_raw)} linhas) | "
            f"captacao_historico {str_corte} ate {str_max}: {len(captacao_hoje)} linhas"
        )
        print(f"[SUCESSO NNM] {msg}")
        registrar_log("NNM", "Sucesso", len(captacao_hoje), msg)

        thread = threading.Thread(target=_executar_calculo_saidas, daemon=True)
        thread.start()

        return jsonify({
            "status":              "Sucesso",
            "backup_raw":          {"de": str_min, "ate": str_max, "linhas": len(df_raw)},
            "captacao_historico":  len(captacao_hoje),
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