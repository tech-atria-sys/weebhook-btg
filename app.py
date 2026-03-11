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

# Domínios autorizados para download (proteção SSRF)
DOMINIOS_PERMITIDOS = {
    "invest-reports.s3.amazonaws.com",
    "invest-reports-prd.s3.sa-east-1.amazonaws.com",
    "api.btgpactual.com",
    "api.ipify.org"
}

app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

# Janela móvel do NNM: quantos dias antes do max do CSV são reprocessados.
# O histórico anterior a esse corte é SEMPRE preservado.
DIAS_JANELA_NNM = 2


def now_brasilia() -> datetime:
    return datetime.utcnow() - timedelta(hours=3)


CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"UID={quote_plus(USERNAME or '')};"
    f"PWD={quote_plus(PASSWORD or '')};"
    f"TrustServerCertificate=yes"
)


# 2. FUNÇÕES AUXILIARES GERAIS

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
                VALUES (:atv, :st, :ln, :msg, :dt)
            """), {
                "atv": atividade, "st": status,
                "ln": linhas, "msg": str(mensagem)[:500],
                "dt": now_brasilia()
            })
    except Exception as e:
        print(f"[AVISO] Falha ao gravar log: {e}")


def salvar_df_otimizado(
    df: pd.DataFrame,
    nome_tabela: str,
    col_pk: Optional[str] = None,
    if_exists: str = "append",
    schema: str = "dbo"
):
    """Persiste DataFrame respeitando o limite de 2.100 parâmetros do pyodbc."""
    if df.empty:
        return
    n_cols         = len(df.columns)
    safe_chunksize = max(1, min(math.floor(2090 / n_cols) if n_cols else 1000, 1000))
    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(
            name=nome_tabela, con=conn, schema=schema,
            if_exists=if_exists, index=False,
            chunksize=safe_chunksize, method="multi"
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
            url, data={"grant_type": "client_credentials"},
            headers=headers, auth=(BTG_CLIENT_ID, BTG_CLIENT_SECRET)
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
        return parsed.scheme == "https" and parsed.netloc in DOMINIOS_PERMITIDOS
    except Exception:
        return False


def parse_datas_csv(df: pd.DataFrame, coluna: str) -> Tuple[datetime, datetime]:
    df[coluna] = pd.to_datetime(df[coluna], errors="coerce")
    data_min, data_max = df[coluna].min(), df[coluna].max()
    if pd.isnull(data_min) or pd.isnull(data_max):
        raise ValueError(f"Coluna '{coluna}' sem datas válidas — verifique o CSV.")
    return data_min, data_max


def erro_interno(atividade: str, e: Exception, conta: str = "") -> tuple:
    detalhe = f"Conta: {conta} | {e}" if conta else str(e)
    print(f"[ERRO CRÍTICO {atividade}] {detalhe}")
    registrar_log(atividade, "Erro", 0, detalhe)
    return jsonify({"erro": "Erro interno — consulte os logs"}), 500


def _ler_tabela(engine, tabela: str, schema: str = "dbo") -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(f'SELECT * FROM {schema}."{tabela}"', conn)
    except Exception as e:
        print(f"[AVISO] Não foi possível ler {schema}.{tabela}: {e}")
        return pd.DataFrame()


def _normalizar_assessor(serie: pd.Series) -> pd.Series:
    """Upper + correções de nomes conhecidos."""
    serie = serie.astype(str).str.upper().str.strip()
    return serie.replace({
        "MURILO LUIZ SILVA GINO":  "IZADORA VILLELA FREITAS",
        "RODRIGO DE MELLO D?ELIA": "RODRIGO DE MELLO D'ELIA",
        "RODRIGO DE MELLO DELIA":  "RODRIGO DE MELLO D'ELIA",
    })


# 3. NÚCLEO — PROCESSAMENTO NNM → CAPTACAO_HISTORICO
#
# captacao_historico é a ÚNICA tabela de fatos de captação.
# O CSV do BTG chega, é enriquecido aqui e gravado diretamente nela.
# O backup_nnm_raw preserva o arquivo bruto intocado.
# O histórico anterior à janela nunca é tocado.

def processar_csv_nnm_para_historico(df_csv: pd.DataFrame, data_corte: datetime) -> dict:
    """
    Recebe o DataFrame do CSV (ou do backup_nnm_raw para reprocessamento) e:
      1. Mapeia colunas CSV → schema de captacao_historico
      2. Filtra a janela ]data_corte, max_csv]
      3. Concatena migrações BTG da mesma janela
      4. Resolve assessor atual via base_btg e marca Ativo/Inativo
      5. Calcula débitos de contas inativas (PL negativo como saída)
      6. Enriquece com nomes_clientes
      7. DELETE cirúrgico apenas na janela + INSERT
    """
    ATIVIDADE = "CAPTACAO_HISTORICO"
    engine    = get_engine()
    str_corte = data_corte.strftime("%Y-%m-%d")

    # ── 3.1  Mapeamento de colunas CSV → schema do histórico ─────────────────
    renomear_csv = {
        "nr_conta":        "CONTA",
        "data_captacao":   "DATA",
        "captacao":        "CAPTACAO",
        "mercado":         "MERCADO",
        "descricao":       "DESCRICAO",
        "cge_officer":     "Assessor",
        "tipo_lancamento": "TIPO",
    }
    df = df_csv.rename(columns={k: v for k, v in renomear_csv.items() if k in df_csv.columns})

    # ── 3.2  Normaliza tipos ──────────────────────────────────────────────────
    df["CONTA"]    = df["CONTA"].astype(str).str.strip()
    df["DATA"]     = pd.to_datetime(df["DATA"], errors="coerce")
    df["CAPTACAO"] = pd.to_numeric(df["CAPTACAO"], errors="coerce")
    df["Assessor"] = _normalizar_assessor(df.get("Assessor", pd.Series(dtype=str)))

    # Remove estornos internos (tipo RS), igual ao script manual
    if "TIPO" in df.columns:
        df = df[df["TIPO"] != "RS"]

    df.dropna(subset=["DATA"], inplace=True)
    df = df[df["DATA"] > data_corte].copy()
    df["TIPO DE CAPTACAO"] = "Padrão"

    # Schema final — colunas obrigatórias
    colunas_base = ["DATA", "CONTA", "CAPTACAO", "Assessor", "TIPO DE CAPTACAO", "MERCADO"]
    for col in colunas_base:
        if col not in df.columns:
            df[col] = None
    df = df[colunas_base].copy()

    linhas_nnm = len(df)
    print(f"[{ATIVIDADE}] Lançamentos NNM na janela (>{str_corte}): {linhas_nnm}")

    if linhas_nnm == 0:
        msg = f"Nenhum lançamento NNM após {str_corte}. Nada a fazer."
        registrar_log(ATIVIDADE, "Sucesso", 0, msg)
        return {"linhas_nnm": 0, "linhas_migracoes": 0,
                "linhas_debitos": 0, "total_inserido": 0}

    # ── 3.3  Migrações BTG da mesma janela ───────────────────────────────────
    df_migracoes     = _ler_tabela(engine, "migracoes_btg")
    linhas_migracoes = 0
    if not df_migracoes.empty:
        df_migracoes["CONTA"] = df_migracoes["CONTA"].astype(str).str.strip()
        df_migracoes["DATA"]  = pd.to_datetime(df_migracoes["DATA"], errors="coerce")
        df_migracoes          = df_migracoes[df_migracoes["DATA"] > data_corte]
        if not df_migracoes.empty:
            df_migracoes["TIPO DE CAPTACAO"] = "Migração BTG"
            df_migracoes["MERCADO"]          = "Migração BTG"
            df = pd.concat(
                [df, df_migracoes[colunas_base]],
                axis=0, ignore_index=True
            )
            linhas_migracoes = len(df_migracoes)
            print(f"[{ATIVIDADE}] Migrações BTG na janela: {linhas_migracoes}")

    # ── 3.4  Assessor atual + Situação via base_btg ───────────────────────────
    # O assessor gravado sempre reflete o responsável atual pela conta,
    # igual ao comportamento do script manual original.
    df_base       = _ler_tabela(engine, "base_btg")
    contas_ativas: set = set()

    if not df_base.empty:
        col_conta    = "Conta"    if "Conta"    in df_base.columns else df_base.columns[0]
        col_assessor = "Assessor" if "Assessor" in df_base.columns else None

        if col_assessor:
            mapa = (
                df_base[[col_conta, col_assessor]]
                .rename(columns={col_conta: "CONTA", col_assessor: "Assessor_atual"})
                .copy()
            )
            mapa["CONTA"]          = mapa["CONTA"].astype(str).str.strip()
            mapa["Assessor_atual"] = _normalizar_assessor(mapa["Assessor_atual"])
            contas_ativas          = set(mapa["CONTA"].tolist())

            df = df.merge(mapa, on="CONTA", how="left")
            mask = df["Assessor_atual"].notna()
            df.loc[mask, "Assessor"] = df.loc[mask, "Assessor_atual"]
            df.drop(columns=["Assessor_atual"], inplace=True)

    df["Situacao"] = df["CONTA"].apply(
        lambda c: "Ativo" if c in contas_ativas else "Inativo"
    )

    # ── 3.5  Débitos — saída de contas inativas ───────────────────────────────
    contas_inativas  = df[df["Situacao"] == "Inativo"]["CONTA"].unique()
    linhas_debitos   = 0
    df_debitos       = pd.DataFrame()

    if len(contas_inativas) > 0:
        contas_tuple = tuple(contas_inativas)

        # Último PL de cada conta inativa
        try:
            with engine.connect() as conn:
                pl_hist = pd.read_sql(
                    text("""
                        SELECT "Conta" AS CONTA, "PL Total", "Data"
                        FROM dbo.pl_historico_diario
                        WHERE "Conta" IN :contas
                    """),
                    conn, params={"contas": contas_tuple}
                )
            pl_hist["CONTA"] = pl_hist["CONTA"].astype(str)
        except Exception as e:
            print(f"[AVISO {ATIVIDADE}] pl_historico_diario indisponível: {e}")
            pl_hist = pd.DataFrame()

        # Data real da saída via Entradas_e_saidas_consolidado
        try:
            with engine.connect() as conn:
                es = pd.read_sql(
                    text("""
                        SELECT "Conta" AS CONTA,
                               MAX("Mês de entrada/saída") AS data_saida
                        FROM dbo."Entradas_e_saidas_consolidado"
                        WHERE "Conta" IN :contas
                        GROUP BY "Conta"
                    """),
                    conn, params={"contas": contas_tuple}
                )
            es["CONTA"] = es["CONTA"].astype(str)
        except Exception as e:
            print(f"[AVISO {ATIVIDADE}] Entradas_e_saidas indisponível: {e}")
            es = pd.DataFrame()

        debitos_list = []
        for conta in contas_inativas:
            assessor_db = (
                df[df["CONTA"] == conta]["Assessor"].iloc[0]
                if conta in df["CONTA"].values else "INDEFINIDO"
            )

            # PL negativo = valor da saída
            if not pl_hist.empty and conta in pl_hist["CONTA"].values:
                pl_rows        = pl_hist[pl_hist["CONTA"] == conta].sort_values("Data")
                captacao_saida = float(pl_rows.iloc[-1]["PL Total"]) * -1
                data_pl_max    = pd.to_datetime(pl_rows["Data"].max())
            else:
                captacao_saida = 0.0
                data_pl_max    = now_brasilia()

            # Data do débito: Entradas_e_saidas tem prioridade
            if not es.empty and conta in es["CONTA"].values:
                data_debito = pd.to_datetime(
                    es[es["CONTA"] == conta]["data_saida"].values[0], errors="coerce"
                )
                if pd.isnull(data_debito):
                    data_debito = data_pl_max
            else:
                data_debito = data_pl_max

            # Só insere se a data da saída cair dentro da janela atual
            if pd.to_datetime(data_debito) > data_corte:
                debitos_list.append({
                    "DATA":             pd.to_datetime(data_debito),
                    "CONTA":            conta,
                    "CAPTACAO":         captacao_saida,
                    "Assessor":         assessor_db,
                    "TIPO DE CAPTACAO": "Saída de conta",
                    "MERCADO":          "Saída de conta",
                    "Situacao":         "Inativo",
                })

        if debitos_list:
            df_debitos     = pd.DataFrame(debitos_list).drop_duplicates(subset="CONTA")
            df             = pd.concat([df, df_debitos], axis=0, ignore_index=True)
            linhas_debitos = len(df_debitos)
            print(f"[{ATIVIDADE}] Débitos (saídas) na janela: {linhas_debitos}")

    # ── 3.6  Nomes dos clientes ───────────────────────────────────────────────
    df_nomes = _ler_tabela(engine, "nomes_clientes")
    if not df_nomes.empty:
        col_n = "Conta" if "Conta" in df_nomes.columns else df_nomes.columns[0]
        df_nomes = df_nomes.rename(columns={col_n: "CONTA"})
        df_nomes["CONTA"] = df_nomes["CONTA"].astype(str).str.strip()
        if "Nome" in df.columns:
            df.drop(columns=["Nome"], inplace=True)
        df = df.merge(df_nomes[["CONTA", "Nome"]], on="CONTA", how="left")

    # ── 3.7  Limpeza final ────────────────────────────────────────────────────
    df["DATA"]     = pd.to_datetime(df["DATA"])
    df["CONTA"]    = df["CONTA"].astype(str).str.strip()
    df["CAPTACAO"] = pd.to_numeric(df["CAPTACAO"], errors="coerce").fillna(0)
    df["Assessor"] = _normalizar_assessor(df["Assessor"])

    total_inserido = len(df)
    if total_inserido == 0:
        msg = f"Processamento concluído — nenhuma linha gerada para >{str_corte}."
        registrar_log(ATIVIDADE, "Sucesso", 0, msg)
        return {"linhas_nnm": linhas_nnm, "linhas_migracoes": linhas_migracoes,
                "linhas_debitos": 0, "total_inserido": 0}

    # ── 3.8  DELETE cirúrgico + INSERT ────────────────────────────────────────
    # DELETE cobre apenas o intervalo exato do batch — nada fora é tocado.
    data_min_batch = df["DATA"].min().strftime("%Y-%m-%d")
    data_max_batch = df["DATA"].max().strftime("%Y-%m-%d")

    with engine.begin() as conn:
        # Lançamentos normais e migrações da janela
        r1 = conn.execute(text("""
            DELETE FROM dbo.captacao_historico
            WHERE DATA BETWEEN :d_min AND :d_max
              AND "TIPO DE CAPTACAO" != 'Saída de conta'
        """), {"d_min": data_min_batch, "d_max": data_max_batch})
        print(f"[{ATIVIDADE}] DELETE lançamentos: {r1.rowcount} linhas "
              f"({data_min_batch} → {data_max_batch})")

        # Débitos: deleta por conta (não por data) para evitar duplicatas
        if not df_debitos.empty:
            contas_debito = tuple(df_debitos["CONTA"].tolist())
            r2 = conn.execute(text("""
                DELETE FROM dbo.captacao_historico
                WHERE "TIPO DE CAPTACAO" = 'Saída de conta'
                  AND CONTA IN :contas
            """), {"contas": contas_debito})
            print(f"[{ATIVIDADE}] DELETE débitos: {r2.rowcount} linhas")

    salvar_df_otimizado(df, "captacao_historico", if_exists="append")

    msg = (
        f"Janela: {data_min_batch} → {data_max_batch} | "
        f"NNM: {linhas_nnm} | Migrações: {linhas_migracoes} | "
        f"Débitos: {linhas_debitos} | Total inserido: {total_inserido}"
    )
    print(f"[SUCESSO {ATIVIDADE}] {msg}")
    registrar_log(ATIVIDADE, "Sucesso", total_inserido, msg)

    return {
        "janela":           {"de": data_min_batch, "ate": data_max_batch},
        "linhas_nnm":       linhas_nnm,
        "linhas_migracoes": linhas_migracoes,
        "linhas_debitos":   linhas_debitos,
        "total_inserido":   total_inserido,
    }


# 4. ROTAS DE GATILHO

def _trigger_generico(url_relatorio: str, nome_log: str):
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
                "inicio_validade": pd.to_datetime(carteira.get("validityStart"), errors="coerce"),
                "fim_validade":    pd.to_datetime(carteira.get("validityEnd"),   errors="coerce"),
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
                      "Carteiras recomendadas importadas")
        return jsonify({"status": "Sucesso", "linhas_salvas": len(df_carteiras)}), 200

    except Exception as e:
        return erro_interno("CARTEIRAS_RECOM", e)


# 5. WEBHOOKS

@app.route("/webhook/nnm", methods=["POST"])
def webhook_nnm():
    """
    Recebe o CSV de NNM do BTG.
    - Salva backup raw intocado em backup_nnm_raw
    - Processa e grava diretamente em captacao_historico (tabela de fatos definitiva)
    A janela reprocessada é determinada pelo próprio CSV (data_max - DIAS_JANELA_NNM).
    O histórico anterior à janela nunca é alterado.
    """
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    try:
        url_download = dados.get("response", {}).get("url") or dados.get("url")
        if not url_download:
            return jsonify({"status": "Recebido sem URL"}), 200

        if not validar_url_download(url_download):
            registrar_log("NNM", "Erro", 0, f"URL bloqueada (SSRF): {url_download}")
            return jsonify({"erro": "URL não autorizada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()

        df_csv = pd.read_csv(io.StringIO(r.content.decode("utf-8")), sep=";")
        df_csv.rename(columns={"dt_captacao": "data_captacao"}, inplace=True)

        # Âncoras data-driven — janela determinada pelo próprio CSV
        data_min_csv, data_max_csv = parse_datas_csv(df_csv, "data_captacao")
        data_corte = data_max_csv - timedelta(days=DIAS_JANELA_NNM)

        str_min   = data_min_csv.strftime("%Y-%m-%d")
        str_max   = data_max_csv.strftime("%Y-%m-%d")
        str_corte = data_corte.strftime("%Y-%m-%d")

        print(f"[NNM] CSV: {str_min} → {str_max} | Janela reprocessada: >{str_corte}")

        # ── Backup raw — arquivo bruto preservado sem qualquer transformação ──
        df_raw = df_csv.copy()
        df_raw["data_recebimento_webhook"] = now_brasilia()

        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.backup_nnm_raw
                WHERE data_captacao BETWEEN :d_min AND :d_max
            """), {"d_min": str_min, "d_max": str_max})

        salvar_df_otimizado(df_raw, "backup_nnm_raw", if_exists="append")
        print(f"[NNM] Backup raw salvo: {len(df_raw)} linhas")

        # ── Processa janela diretamente na captacao_historico ─────────────────
        df_janela = df_csv[
            pd.to_datetime(df_csv["data_captacao"], errors="coerce") > data_corte
        ].copy()

        resultado = processar_csv_nnm_para_historico(df_janela, data_corte)

        registrar_log("NNM", "Sucesso", resultado["total_inserido"],
                      f"Raw: {str_min}→{str_max} ({len(df_raw)} linhas) | "
                      f"Histórico: {resultado}")

        return jsonify({
            "status": "Sucesso",
            "backup_raw": {"de": str_min, "ate": str_max, "linhas": len(df_raw)},
            "captacao_historico": resultado,
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
        dados        = request.json
        url_download = dados.get("response", {}).get("url") or dados.get("url")
        if not url_download:
            return jsonify({"erro": "URL não encontrada"}), 400

        if not validar_url_download(url_download):
            registrar_log("BASE_BTG", "Erro", 0, f"URL bloqueada (SSRF): {url_download}")
            return jsonify({"erro": "URL não autorizada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()

        base   = pd.read_csv(io.BytesIO(r.content), sep=";", encoding="utf-8")
        df_raw = base.copy()
        df_raw["data_recebimento_webhook"] = now_brasilia()
        salvar_df_otimizado(df_raw, "backup_base_btg_raw", if_exists="append")

        renomear = {
            "nm_officer":    "Assessor",
            "nr_conta":      "Conta",
            "pl_total":      "PL Total",
            "nome_completo": "Nome",
            "faixa_cliente": "Faixa Cliente"
        }
        colunas_presentes = {k: v for k, v in renomear.items() if k in base.columns}
        colunas_ausentes  = [k for k in renomear if k not in base.columns]
        if colunas_ausentes:
            print(f"[AVISO BASE_BTG] Colunas não encontradas: {colunas_ausentes}")

        base.rename(columns=colunas_presentes, inplace=True)

        faltando = [c for c in ["Assessor", "Conta", "PL Total"] if c not in base.columns]
        if faltando:
            msg = f"Colunas críticas ausentes: {faltando}"
            registrar_log("BASE_BTG", "Erro", 0, msg)
            return jsonify({"erro": msg}), 400

        base["Conta"]    = base["Conta"].astype(str)
        base["Assessor"] = base["Assessor"].str.upper()

        faixas_ate_300 = ["Ate 50K", "Entre 50k e 100k", "Entre 100k e 300k"]
        base.loc[base["Faixa Cliente"].isin(faixas_ate_300), "Faixa Cliente"] = "Ate 300k"

        engine = get_engine()
        try:
            with engine.connect() as conn:
                offshore = pd.read_sql("SELECT * FROM dbo.pl_offshore", conn)
            offshore["Conta"] = offshore["Conta"].astype(str)
            base = pd.concat([offshore, base], axis=0, ignore_index=True)
        except Exception as e:
            print(f"[AVISO BASE_BTG] Offshore não carregado: {e}")

        base.loc[base["Assessor"] == "MURILO LUIZ SILVA GINO", "Assessor"] = "IZADORA VILLELA FREITAS"
        nomes_rodrigo = ["RODRIGO DE MELLO D?ELIA", "RODRIGO DE MELLO DELIA"]
        base.loc[base["Assessor"].isin(nomes_rodrigo), "Assessor"] = "RODRIGO DE MELLO D'ELIA"
        base.drop_duplicates(subset="Conta", keep="first", inplace=True)

        salvar_df_otimizado(base, "base_btg", col_pk="Conta", if_exists="replace")

        hoje    = now_brasilia().replace(hour=0, minute=0, second=0, microsecond=0)
        df_hist = base[["Conta", "Assessor", "PL Total"]].copy()
        df_hist["Data"] = hoje
        df_hist["Mês"]  = hoje.strftime("%Y-%m")
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

    dados    = request.json
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
            return jsonify({"erro": "URL não encontrada", "conta": conta_id}), 400

        if not validar_url_download(url_download):
            registrar_log("PERFORMANCE", "Erro", 0, f"URL bloqueada (SSRF): {url_download}")
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
                    USING (SELECT ? AS ContaVal) AS Source ON Target.conta = Source.ContaVal
                    WHEN MATCHED THEN UPDATE SET
                        arquivo_pdf=?, nome_arquivo=?, data_referencia=?, data_upload=GETDATE()
                    WHEN NOT MATCHED THEN INSERT
                        (conta, arquivo_pdf, nome_arquivo, data_referencia, data_upload)
                        VALUES (?, ?, ?, ?, GETDATE());
                """, (conta_final, pdf_bytes, nome_arquivo, data_ref,
                      conta_final, pdf_bytes, nome_arquivo, data_ref))
                arquivos_salvos += 1

        conn_db.commit()
        conn_db.close()

        print(f"[SUCESSO PERFORMANCE] Conta: {conta_id} | PDFs: {arquivos_salvos} | Ref: {data_ref}")
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
            registrar_log("CUSTODIA", "Erro", 0, f"URL bloqueada (SSRF): {url_download}")
            return jsonify({"erro": "URL não autorizada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f, sep=",", encoding="latin1", low_memory=False)

        df_raw = df.copy()
        df_raw["data_recebimento_webhook"] = now_brasilia()
        salvar_df_otimizado(df_raw, "backup_custodia_raw", if_exists="replace")

        df_final = df.copy()
        for col in ["referenceDate", "dataInicio", "fixingDate", "dataKnockIn"]:
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(df_final[col], format="%d/%m/%Y", errors="coerce")

        df_final["data_upload"] = now_brasilia()
        salvar_df_otimizado(df_final, "relatorios_custodia", if_exists="replace")

        msg = f"Custódia concluída. Linhas: {len(df_final)}"
        print(f"[SUCESSO CUSTODIA] {msg}")
        registrar_log("CUSTODIA", "Sucesso", len(df_final), msg)
        return jsonify({"status": "Sucesso", "linhas": len(df_final)}), 200

    except Exception as e:
        return erro_interno("CUSTODIA", e)


# 6. UTILITÁRIOS

@app.route("/meu-ip", methods=["GET"])
def get_ip():
    try:
        return jsonify({"ip_render": requests.get("https://api.ipify.org").text})
    except Exception:
        return jsonify({"erro": "Falha ao obter IP"}), 500


@app.route("/admin/reprocessar-captacao", methods=["GET"])
def reprocessar_captacao():
    """
    Reprocessa a captacao_historico a partir de uma data arbitrária,
    relendo do backup_nnm_raw já no banco — sem depender do BTG.

    Uso:  GET /admin/reprocessar-captacao?desde=2025-01-01
    Auth: mesmo token X-Webhook-Token dos webhooks.
    """
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    desde_str = request.args.get("desde")
    if not desde_str:
        return jsonify({"erro": "Parâmetro 'desde' obrigatório (ex: ?desde=2025-01-01)"}), 400

    try:
        data_corte = datetime.strptime(desde_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"erro": "Formato inválido — use YYYY-MM-DD"}), 400

    try:
        engine = get_engine()
        with engine.connect() as conn:
            df_raw = pd.read_sql(
                text("SELECT * FROM dbo.backup_nnm_raw WHERE data_captacao > :corte"),
                conn, params={"corte": desde_str}
            )

        if df_raw.empty:
            return jsonify({
                "status":   "Nada a fazer",
                "mensagem": f"Nenhum registro em backup_nnm_raw após {desde_str}"
            }), 200

        # Normaliza nome da coluna de data (pode variar dependendo de quando foi salva)
        if "dt_captacao" in df_raw.columns:
            df_raw.rename(columns={"dt_captacao": "data_captacao"}, inplace=True)

        resultado = processar_csv_nnm_para_historico(df_raw, data_corte)
        return jsonify({"status": "Sucesso", "resultado": resultado}), 200

    except Exception as e:
        return erro_interno("REPROCESSAR_CAPTACAO", e)


# 7. ENTRYPOINT

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)