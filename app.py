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
from typing import Optional, Tuple
from urllib.parse import urlparse, quote_plus
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from flask import Flask, request, jsonify
from zoneinfo import ZoneInfo
import sys
import threading  

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
    ("RODRIGO DE MELLO D'ELIA",         "https://netorg18892072-my.sharepoint.com/:x:/g/personal/joao_aquino_atriacm_com_br/IQBVuGicHybdRrC4d1MtFO8vAbY4Kw4m4_8gNo8EKu3BN4I?download=1"),
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
 
SCHEMA_DEFAULT  = "dbo"
TZ_BRASILIA     = ZoneInfo("America/Sao_Paulo")

# Domínios autorizados para download de arquivos 
DOMINIOS_PERMITIDOS = {
    "invest-reports.s3.amazonaws.com",
    "invest-reports-prd.s3.sa-east-1.amazonaws.com",  
    "api.btgpactual.com",
    "api.ipify.org"
}

# Tamanho máximo de payload aceito nos webhooks (50 MB)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

# Janela curta da tabela de fatos (dias relativos ao max do CSV)
DIAS_FATO_NNM = 2

# Timezone de Brasília — UTC-3
def now_brasilia() -> datetime:
    return datetime.utcnow() - timedelta(hours=3)

# CONN_STR com valores quoted para evitar quebra por caracteres especiais
CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"UID={quote_plus(USERNAME or '')};"
    f"PWD={quote_plus(PASSWORD or '')};"
    f"TrustServerCertificate=yes"
)


# 2. FUNÇÕES AUXILIARES

def get_engine():
    """Cria engine SQLAlchemy. Centralizado para facilitar manutenção."""
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
    """Obtém access token OAuth2 do BTG."""
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
    """
    Aceita token via:
    - X-Webhook-Token: usado pelos gatilhos do GitHub Actions
    - X-Api-Key: usado pelo BTG nos callbacks de webhook
    """
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
    """
    Previne SSRF validando que a URL pertence a um domínio autorizado
    e usa HTTPS. Bloqueia IPs internos, metadata endpoints e domínios
    não listados em DOMINIOS_PERMITIDOS.
    """
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
    """
    Retorna (data_min, data_max) como datetime.
    Lança ValueError se as datas vierem nulas (CSV malformado).
    A coluna é mantida como datetime64 — formatação fica na camada de apresentação.
    """
    df[coluna] = pd.to_datetime(df[coluna], errors="coerce")
    data_min = df[coluna].min()
    data_max = df[coluna].max()

    if pd.isnull(data_min) or pd.isnull(data_max):
        raise ValueError(f"Coluna '{coluna}' sem datas válidas — verifique o CSV recebido.")

    return data_min, data_max


def erro_interno(atividade: str, e: Exception, conta: str = "") -> tuple:
    """
    Loga o erro completo internamente mas retorna mensagem genérica
    para o cliente, evitando vazamento de detalhes da infraestrutura.
    """
    detalhe = f"Conta: {conta} | {str(e)}" if conta else str(e)
    print(f"[ERRO CRÍTICO {atividade}] {detalhe}")
    registrar_log(atividade, "Erro", 0, detalhe)
    return jsonify({"erro": "Erro interno — consulte os logs"}), 500

def _load_previa_assessor(advisor_name: str, link: str) -> pd.DataFrame:
    """Baixa e parseia a aba 'Meta' do Excel de um assessor."""
    try:
        response = requests.get(link, params={"downloadformat": "excel"}, timeout=15)
        response.raise_for_status()
 
        df = pd.read_excel(BytesIO(response.content), sheet_name="Meta")
 
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
 
        # --- Carrega histórico existente ---
        engine = get_engine()
        try:
            hist = pd.read_sql(f"SELECT * FROM {SCHEMA_DEFAULT}.previa_receita_nova", engine)
            if not hist.empty:
                hist["Data"] = pd.to_datetime(hist["Data"], errors="coerce")
                hist = hist[hist["Data"] != primeiro_dia_mes]
        except Exception:
            hist = pd.DataFrame()
 
        # --- Coleta dados de cada assessor ---
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
 
        # --- Consolida com histórico ---
        previa_final = pd.concat([hist, previa_receita], axis=0, ignore_index=True)
        previa_final.loc[previa_final["META - VOLUME"] == "-", "META - VOLUME"] = 0
        previa_final.fillna(0, inplace=True)
 
        salvar_df_otimizado(previa_final, "previa_receita_nova", if_exists="replace", schema=SCHEMA_DEFAULT)
 
        # --- Histórico agregado por assessor ---
        cols_agg = [c for c in ["Assessor", "Data", "META - ROA", "REALIZADO - ROA"] if c in previa_receita.columns]
        previa_agg = previa_receita[cols_agg].groupby(["Assessor", "Data"]).sum().reset_index()
        previa_agg["Data"] = pd.to_datetime(previa_agg["Data"])
 
        try:
            hist_agg = pd.read_sql(f"SELECT * FROM {SCHEMA_DEFAULT}.previa_receita_assessor_historico", engine)
            if not hist_agg.empty:
                hist_agg["Data"] = pd.to_datetime(hist_agg["Data"], errors="coerce")
                hist_agg = hist_agg[hist_agg["Data"] != primeiro_dia_mes]
            previa_final_agg = pd.concat([hist_agg, previa_agg], axis=0, ignore_index=True)
        except Exception:
            previa_final_agg = previa_agg
 
        salvar_df_otimizado(previa_final_agg, "previa_receita_assessor_historico", if_exists="replace", schema=SCHEMA_DEFAULT)
 
        msg = f"Detalhado: {len(previa_final)} linhas | Agregado: {len(previa_final_agg)} linhas"
        print(f"[SUCESSO PREVIA_RECEITA] {msg}")
        registrar_log(atividade, "Sucesso", len(previa_final), msg)
 
    except Exception as e:
        registrar_log(atividade, "Erro", 0, str(e))
        print(f"[ERRO CRÍTICO PREVIA_RECEITA] {e}")

# 3. ROTAS DE GATILHO (disparam geração de relatório no BTG)

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

@app.route("/trigger/previa-receita", methods=["GET"])
def trigger_previa_receita():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403
 
    thread = threading.Thread(target=_executar_previa_receita, daemon=True)
    thread.start()
 
    return jsonify({"status": "iniciado", "mensagem": "Atualização da Prévia Receita em andamento"}), 202

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
            base = {
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
                    linha = base.copy()
                    linha.update({
                        "ticker":  ativo.get("ticker"),
                        "empresa": ativo.get("company"),
                        "setor":   setor.get("name"),
                        "peso":    item.get("weight")
                    })
                    linhas.append(linha)
            else:
                linhas.append(base)

        df_carteiras = pd.DataFrame(linhas)
        salvar_df_otimizado(df_carteiras, "carteiras_recomendadas_btg", if_exists="replace")

        registrar_log("CARTEIRAS_RECOM", "Sucesso", len(df_carteiras),
                      "Carteiras recomendadas importadas com sucesso")
        return jsonify({"status": "Sucesso", "linhas_salvas": len(df_carteiras)}), 200

    except Exception as e:
        return erro_interno("CARTEIRAS_RECOM", e)


# 4. WEBHOOKS (recebem o arquivo gerado pelo BTG e persistem no banco)

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
            registrar_log("NNM", "Erro", 0, f"URL bloqueada por política SSRF: {url_download}")
            return jsonify({"erro": "URL não autorizada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()

        df = pd.read_csv(io.StringIO(r.content.decode("utf-8")), sep=";")
        df.rename(columns={"dt_captacao": "data_captacao"}, inplace=True)

        # Âncoras data-driven — parse com datetime64 para comparações
        data_min_csv, data_max_csv = parse_datas_csv(df, "data_captacao")
        data_corte_fato = data_max_csv - timedelta(days=DIAS_FATO_NNM)

        # Converte para date puro APÓS o parse — garante DATE no banco
        df["data_captacao"] = pd.to_datetime(df["data_captacao"], errors="coerce").dt.date

        str_min_csv    = data_min_csv.strftime("%Y/%m/%d")
        str_max_csv    = data_max_csv.strftime("%Y/%m/%d")
        str_corte_fato = data_corte_fato.strftime("%Y/%m/%d")

        print(
            f"[DEBUG NNM] CSV: {str_min_csv} → {str_max_csv} | "
            f"Janela fato: {str_corte_fato} → {str_max_csv}"
        )

        engine = get_engine()

        df_raw = df.copy()
        df_raw["data_recebimento_webhook"] = now_brasilia()

        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.backup_nnm_raw
                WHERE data_captacao BETWEEN :data_min AND :data_max
            """), {"data_min": str_min_csv, "data_max": str_max_csv})

        salvar_df_otimizado(df_raw, "backup_nnm_raw", if_exists="append")

        colunas_tabela = [
            "nr_conta", "data_captacao", "ativo", "mercado", "cge_officer",
            "tipo_lancamento", "descricao", "qtd", "captacao",
            "is_officer_nnm", "is_partner_nnm", "is_channel_nnm", "is_bu_nnm",
            "submercado", "submercado_detalhado"
        ]
        df_fato = df[df["data_captacao"] > data_corte_fato.date()].copy()
        df_fato = df_fato[[c for c in colunas_tabela if c in df_fato.columns]].copy()

        for col in ["is_officer_nnm", "is_partner_nnm", "is_channel_nnm", "is_bu_nnm"]:
            if col in df_fato.columns:
                df_fato[col] = (
                    df_fato[col]
                    .map({"t": 1, "f": 0, "True": 1, "False": 0})
                    .fillna(0)
                    .astype(int)
                )

        df_fato["data_upload"] = now_brasilia()

        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.relatorios_nnm_gerencial
                WHERE data_captacao > :corte
            """), {"corte": str_corte_fato})

        salvar_df_otimizado(df_fato, "relatorios_nnm_gerencial", if_exists="append")

        msg = (
            f"Backup: {str_min_csv} → {str_max_csv} ({len(df_raw)} linhas) | "
            f"Fato: {str_corte_fato} → {str_max_csv} ({len(df_fato)} linhas)"
        )
        print(f"[SUCESSO NNM] {msg}")
        registrar_log("NNM", "Sucesso", len(df_fato), msg)

        return jsonify({
            "status": "Sucesso",
            "backup": {"de": str_min_csv, "ate": str_max_csv, "linhas": len(df_raw)},
            "fato":   {"de": str_corte_fato, "ate": str_max_csv, "linhas": len(df_fato)}
        }), 200

    except ValueError as e:
        registrar_log("NNM", "Erro", 0, str(e))
        return jsonify({"erro": str(e)}), 400

    except Exception as e:
        return erro_interno("NNM", e)


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
            registrar_log("BASE_BTG", "Erro", 0, f"URL bloqueada por política SSRF: {url_download}")
            return jsonify({"erro": "URL não autorizada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()

        base = pd.read_csv(io.BytesIO(r.content), sep=";", encoding="utf-8")

        # Backup raw antes de qualquer transformação
        df_raw = base.copy()
        df_raw["data_recebimento_webhook"] = now_brasilia()
        salvar_df_otimizado(df_raw, "backup_base_btg_raw", if_exists="append")

        # Rename defensivo — só aplica colunas que existirem no CSV
        renomear = {
            "nm_officer":   "Assessor",
            "nr_conta":      "Conta",
            "pl_total":      "PL Total",
            "nome_completo": "Nome",
            "faixa_cliente": "Faixa Cliente"
        }
        colunas_presentes = {k: v for k, v in renomear.items() if k in base.columns}
        colunas_ausentes  = [k for k in renomear if k not in base.columns]

        if colunas_ausentes:
            print(f"[AVISO BASE_BTG] Colunas esperadas não encontradas: {colunas_ausentes}", flush=True)

        base.rename(columns=colunas_presentes, inplace=True)

        # Valida colunas críticas após rename
        colunas_criticas = ["Assessor", "Conta", "PL Total"]
        faltando = [c for c in colunas_criticas if c not in base.columns]
        if faltando:
            msg = f"Colunas críticas ausentes após rename: {faltando}"
            registrar_log("BASE_BTG", "Erro", 0, msg)
            return jsonify({"erro": msg}), 400

        base["Conta"]    = base["Conta"].astype(str)
        base["Assessor"] = base["Assessor"].str.upper()

        faixas_ate_300 = ["Ate 50K", "Entre 50k e 100k", "Entre 100k e 300k"]
        base.loc[base["Faixa Cliente"].isin(faixas_ate_300), "Faixa Cliente"] = "Ate 300k"

        # Merge com PL offshore
        engine = get_engine()
        try:
            with engine.connect() as conn:
                offshore = pd.read_sql("SELECT * FROM dbo.pl_offshore", conn)
            offshore["Conta"] = offshore["Conta"].astype(str)
            base = pd.concat([offshore, base], axis=0, ignore_index=True)
        except Exception as e:
            print(f"[AVISO BASE_BTG] Offshore não carregado: {e}", flush=True)

        # Correções de assessores
        base.loc[base["Assessor"] == "MURILO LUIZ SILVA GINO", "Assessor"] = "IZADORA VILLELA FREITAS"
        nomes_rodrigo = ["RODRIGO DE MELLO D?ELIA", "RODRIGO DE MELLO DELIA"]
        base.loc[base["Assessor"].isin(nomes_rodrigo), "Assessor"] = "RODRIGO DE MELLO D’ELIA"

        base.drop_duplicates(subset="Conta", keep="first", inplace=True)

        salvar_df_otimizado(base, "base_btg", col_pk="Conta", if_exists="replace")

        # Histórico diário de PL
        hoje = now_brasilia().replace(hour=0, minute=0, second=0, microsecond=0)
        df_hist = base[["Conta", "Assessor", "PL Total"]].copy()
        df_hist["Data"] = hoje
        df_hist["Mês"]  = hoje.strftime("%Y/%m")
        salvar_df_otimizado(df_hist, "pl_historico_diario", if_exists="append")

        msg = f"Base e Histórico atualizados. Total: {len(base)}"
        print(f"[SUCESSO BASE_BTG] {msg}")
        registrar_log("BASE_BTG", "Sucesso", len(base), msg)
        return jsonify({"status": "Sucesso", "total": len(base)}), 200

    except Exception as e:
        return erro_interno("BASE_BTG", e)


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

        conta_raw = res.get("accountNumber") or dados.get("cge") or dados.get("accountNumber")
        conta_id  = str(conta_raw) if (conta_raw and str(conta_raw).lower() != "null") else "Desconhecida"

        if not url_download:
            print(f"[AVISO PERFORMANCE] URL não encontrada. Conta: {conta_id} | ID: {req_id}")
            return jsonify({"erro": "URL não encontrada", "conta": conta_id}), 400

        if not validar_url_download(url_download):
            registrar_log("PERFORMANCE", "Erro", 0, f"URL bloqueada por política SSRF: {url_download}")
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

        print(f"[SUCESSO PERFORMANCE] Conta: {conta_id} | Salvos: {arquivos_salvos} | Ref: {data_ref} | ID: {req_id}")
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
            registrar_log("CUSTODIA", "Erro", 0, f"URL bloqueada por política SSRF: {url_download}")
            return jsonify({"erro": "URL não autorizada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            nome_csv = z.namelist()[0]
            with z.open(nome_csv) as f:
                df = pd.read_csv(f, sep=",", encoding="latin1", low_memory=False)

        # Backup raw (replace — custódia é snapshot diário completo)
        df_raw = df.copy()
        df_raw["data_recebimento_webhook"] = now_brasilia()
        salvar_df_otimizado(df_raw, "backup_custodia_raw", if_exists="replace")

        # Datas mantidas como DATE para queries corretas no banco
        df_final = df.copy()
        for col in ["referenceDate", "dataInicio", "fixingDate", "dataKnockIn"]:
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(df_final[col], format="%d/%m/%Y", errors="coerce")

        df_final["data_upload"] = now_brasilia()
        salvar_df_otimizado(df_final, "relatorios_custodia", if_exists="replace")

        msg = f"Importação Custódia concluída. Linhas: {len(df_final)}"
        print(f"[SUCESSO CUSTODIA] {msg}", flush=True)
        registrar_log("CUSTODIA", "Sucesso", len(df_final), msg)
        return jsonify({"status": "Sucesso", "linhas": len(df_final)}), 200

    except Exception as e:
        return erro_interno("CUSTODIA", e)




# 5. UTILITÁRIOS

@app.route("/meu-ip", methods=["GET"])
def get_ip():
    try:
        return jsonify({"ip_render": requests.get("https://api.ipify.org").text})
    except Exception:
        return jsonify({"erro": "Falha ao obter IP"}), 500


# 6. ENTRYPOINT

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)