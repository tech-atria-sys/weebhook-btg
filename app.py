import os
import io
import re
import uuid
import math
import zipfile
import requests
import pyodbc
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from flask import Flask, request, jsonify

app = Flask(__name__)

# 1. CONFIGURAÇÕES

SERVER_NAME     = os.getenv("SERVER_NAME")
DATABASE_NAME   = os.getenv("DATABASE_NAME")
USERNAME        = os.getenv("USERNAME")
PASSWORD        = os.getenv("PASSWORD")
WEBHOOK_TOKEN   = os.getenv("WEBHOOK_TOKEN")
BTG_CLIENT_ID   = os.getenv("BTG_CLIENT_ID")
BTG_CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET")

URL_REPORT_NNM      = os.getenv("PARTNER_REPORT_URL_NNM")
URL_REPORT_BASE     = os.getenv("PARTNER_REPORT_URL_BASEBTG")
URL_REPORT_CUSTODIA = os.getenv("PARTNER_REPORT_URL_CUSTODIA")

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    f"TrustServerCertificate=yes"
)

# Janela curta da tabela de fatos (dias relativos ao max do CSV)
DIAS_FATO_NNM = 3


# 2. FUNÇÕES AUXILIARES

def get_engine():
    """Cria engine SQLAlchemy. Centralizado para facilitar manutenção."""
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={CONN_STR}",
        fast_executemany=True
    )


def registrar_log(atividade: str, status: str, linhas: int = 0, mensagem: str = ""):
    """Grava registro de auditoria na tabela de logs."""
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO dbo.logs_atividades
                    (atividade, status, linhas_processadas, mensagem_detalhe)
                VALUES
                    (:atv, :st, :ln, :msg)
            """), {
                "atv": atividade,
                "st":  status,
                "ln":  linhas,
                "msg": str(mensagem)[:500]
            })
    except Exception as e:
        print(f"[AVISO] Falha ao gravar log no banco: {e}")


def salvar_df_otimizado(
    df: pd.DataFrame,
    nome_tabela: str,
    col_pk: str = None,
    if_exists: str = "append",
    schema: str = "dbo"
):
    """
    Persiste DataFrame no SQL Server com chunksize seguro para o limite
    de 2.100 parâmetros do pyodbc.
    """
    if df.empty:
        return

    num_colunas = len(df.columns)
    limit_params = math.floor(2090 / num_colunas) if num_colunas > 0 else 1000
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


def btg_headers() -> dict:
    """Monta headers padrão para chamadas à API BTG."""
    return {
        "x-id-partner-request": str(uuid.uuid4()),
        "access_token": get_btg_token() or "",
        "Content-Type": "application/json"
    }


def extrair_conta_do_nome(nome_arquivo: str) -> Optional[str]:
    match = re.search(r"(\d+)", nome_arquivo)
    return match.group(1) if match else None


def validar_token(req) -> bool:
    return req.args.get("token") == WEBHOOK_TOKEN


def parse_datas_csv(df: pd.DataFrame, coluna: str):
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
        return jsonify({"erro": str(e)}), 500


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
            erro_msg = f"Erro BTG: {r.status_code} - {r.text}"
            registrar_log("CARTEIRAS_RECOM", "Erro", 0, erro_msg)
            return jsonify({"erro": erro_msg}), r.status_code

        dados = r.json()
        if not dados:
            return jsonify({"status": "Sucesso", "mensagem": "Nenhuma carteira retornada"}), 200

        linhas = []
        for carteira in dados:
            base = {
                "tipo_carteira":            carteira.get("typeInitial"),
                "descricao":                carteira.get("description"),
                "nome_carteira":            carteira.get("name"),
                "link_pdf":                 carteira.get("fileName"),
                "rentabilidade_anterior":   carteira.get("previousProfitability"),
                "rentabilidade_acumulada":  carteira.get("accumulatedProfitability"),
                "inicio_validade":          pd.to_datetime(carteira.get("validityStart"), errors="coerce"),
                "fim_validade":             pd.to_datetime(carteira.get("validityEnd"),   errors="coerce"),
                "data_extracao":            datetime.now()
            }
            ativos = carteira.get("assets", [])
            if ativos:
                for item in ativos:
                    ativo  = item.get("asset", {})
                    setor  = ativo.get("sector", {})
                    linha  = base.copy()
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
        registrar_log("CARTEIRAS_RECOM", "Erro", 0, str(e))
        return jsonify({"erro": str(e)}), 500


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

        r = requests.get(url_download)
        r.raise_for_status()

        df = pd.read_csv(io.StringIO(r.content.decode("utf-8")), sep=";")
        df.rename(columns={"dt_captacao": "data_captacao"}, inplace=True)

        # --- Âncoras data-driven (não dependem do relógio do servidor) ---
        # data_captacao é mantida como DATE no banco; formatação fica no BI
        data_min_csv, data_max_csv = parse_datas_csv(df, "data_captacao")
        data_corte_fato = data_max_csv - timedelta(days=DIAS_FATO_NNM)

        str_min_csv    = data_min_csv.strftime("%Y-%m-%d")
        str_max_csv    = data_max_csv.strftime("%Y-%m-%d")
        str_corte_fato = data_corte_fato.strftime("%Y-%m-%d")

        print(
            f"[DEBUG NNM] CSV: {str_min_csv} → {str_max_csv} | "
            f"Janela fato: {str_corte_fato} → {str_max_csv}"
        )

        engine = get_engine()


        # CAMADA RAW — janela completa do CSV
        # Substitui apenas o intervalo que chegou; edições fora desse
        # intervalo (períodos antigos corrigidos manualmente) ficam intactas.

        df_raw = df.copy()
        df_raw["data_recebimento_webhook"] = datetime.now()

        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM dbo.backup_nnm_raw
                WHERE data_captacao BETWEEN :data_min AND :data_max
            """), {"data_min": str_min_csv, "data_max": str_max_csv})

        salvar_df_otimizado(df_raw, "backup_nnm_raw", if_exists="append")


        # CAMADA DE FATOS — janela curta ancorada no max do CSV
        # Preserva correções manuais em datas anteriores ao corte.

        colunas_tabela = [
            "nr_conta", "data_captacao", "ativo", "mercado", "cge_officer",
            "tipo_lancamento", "descricao", "qtd", "captacao",
            "is_officer_nnm", "is_partner_nnm", "is_channel_nnm", "is_bu_nnm",
            "submercado", "submercado_detalhado"
        ]
        df_fato = df[df["data_captacao"] > data_corte_fato].copy()
        df_fato = df_fato[[c for c in colunas_tabela if c in df_fato.columns]].copy()

        # Booleanos: converte t/f do Postgres para 1/0
        for col in ["is_officer_nnm", "is_partner_nnm", "is_channel_nnm", "is_bu_nnm"]:
            if col in df_fato.columns:
                df_fato[col] = (
                    df_fato[col]
                    .map({"t": 1, "f": 0, "True": 1, "False": 0})
                    .fillna(0)
                    .astype(int)
                )

        df_fato["data_upload"] = datetime.now()

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
        # Datas nulas no CSV — aborta sem deletar nada no banco
        registrar_log("NNM", "Erro", 0, str(e))
        return jsonify({"erro": str(e)}), 400

    except Exception as e:
        print(f"[ERRO CRÍTICO NNM] {e}")
        registrar_log("NNM", "Erro", 0, str(e))
        return jsonify({"erro": str(e)}), 500


@app.route("/webhook/basebtg", methods=["POST"])
def webhook_base_btg():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    try:
        dados = request.json
        url_download = dados.get("response", {}).get("url") or dados.get("url")
        if not url_download:
            return jsonify({"erro": "URL não encontrada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()

        base = pd.read_csv(io.BytesIO(r.content), sep=";", encoding="utf-8")

        # Backup raw
        df_raw = base.copy()
        df_raw["data_recebimento_webhook"] = datetime.now()
        salvar_df_otimizado(df_raw, "backup_base_btg_raw", if_exists="append")

        # Renomeações e normalizações
        base.rename(columns={
            "nm_assessor":   "Assessor",
            "nr_conta":      "Conta",
            "pl_total":      "PL Total",
            "nome_completo": "Nome",
            "faixa_cliente": "Faixa Cliente"
        }, inplace=True)

        base["Conta"]    = base["Conta"].astype(str)
        base["Assessor"] = base["Assessor"].str.upper()

        # Agrupa faixas pequenas
        faixas_ate_300 = ["Ate 50K", "Entre 50k e 100k", "Entre 100k e 300k"]
        base.loc[base["Faixa Cliente"].isin(faixas_ate_300), "Faixa Cliente"] = "Ate 300k"

        # Merge com PL offshore
        engine = get_engine()
        try:
            with engine.connect() as conn:
                offshore = pd.read_sql("SELECT * FROM dbo.pl_offshore", conn)
            offshore["Conta"] = offshore["Conta"].astype(str)
            base = pd.concat([offshore, base], axis=0, ignore_index=True)
        except Exception:
            pass  # Tabela offshore ainda não existe — segue sem ela

        # Correções de nomes de assessores
        base.loc[base["Assessor"] == "MURILO LUIZ SILVA GINO", "Assessor"] = "IZADORA VILLELA FREITAS"
        nomes_rodrigo = ["RODRIGO DE MELLO D?ELIA", "RODRIGO DE MELLO DELIA"]
        base.loc[base["Assessor"].isin(nomes_rodrigo), "Assessor"] = "RODRIGO DE MELLO D'ELIA"

        base.drop_duplicates(subset="Conta", keep="first", inplace=True)

        salvar_df_otimizado(base, "base_btg", col_pk="Conta", if_exists="replace")

        # Histórico diário de PL
        hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        df_hist = base[["Conta", "Assessor", "PL Total"]].copy()
        df_hist["Data"] = hoje
        df_hist["Mês"]  = hoje.strftime("%Y-%m")
        salvar_df_otimizado(df_hist, "pl_historico_diario", if_exists="append")

        msg = f"Base e Histórico atualizados. Total: {len(base)}"
        print(f"[SUCESSO BASE_BTG] {msg}")
        registrar_log("BASE_BTG", "Sucesso", len(base), msg)
        return jsonify({"status": "Sucesso", "total": len(base)}), 200

    except Exception as e:
        print(f"[ERRO CRÍTICO BASE_BTG] {e}")
        registrar_log("BASE_BTG", "Erro", 0, str(e))
        return jsonify({"erro": str(e)}), 500


@app.route("/webhook/performance", methods=["POST"])
def webhook_performance():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    if not dados:
        return jsonify({"erro": "Payload vazio"}), 400

    conta_id = "Desconhecida"
    try:
        res        = dados.get("response") or dados.get("partnerResponse") or {}
        res        = res if isinstance(res, dict) else {}
        url_download = res.get("url")
        data_ref   = res.get("endDate")
        req_id     = dados.get("idPartnerRequest", "N/A")

        conta_raw  = res.get("accountNumber") or dados.get("cge") or dados.get("accountNumber")
        conta_id   = str(conta_raw) if (conta_raw and str(conta_raw).lower() != "null") else "Desconhecida"

        if not url_download:
            print(f"[AVISO PERFORMANCE] URL não encontrada. Conta: {conta_id} | ID: {req_id}")
            return jsonify({"erro": "URL não encontrada", "conta": conta_id}), 400

        r = requests.get(url_download, stream=True)
        r.raise_for_status()

        arquivos_salvos = 0
        conn   = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

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
                            arquivo_pdf    = ?,
                            nome_arquivo   = ?,
                            data_referencia = ?,
                            data_upload    = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (conta, arquivo_pdf, nome_arquivo, data_referencia, data_upload)
                        VALUES (?, ?, ?, ?, GETDATE());
                """, (conta_final, pdf_bytes, nome_arquivo, data_ref,
                      conta_final, pdf_bytes, nome_arquivo, data_ref))
                arquivos_salvos += 1

        conn.commit()
        conn.close()

        print(f"[SUCESSO PERFORMANCE] Conta: {conta_id} | Salvos: {arquivos_salvos} | Ref: {data_ref} | ID: {req_id}")
        return jsonify({"status": "Processado", "conta": conta_id}), 200

    except Exception as e:
        print(f"[ERRO CRÍTICO PERFORMANCE] Conta: {conta_id} | Erro: {str(e)[:200]}")
        return jsonify({"erro": str(e)}), 500


@app.route("/webhook/custodia", methods=["POST"])
def webhook_custodia():
    if not validar_token(request):
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    try:
        url_download = dados.get("response", {}).get("url")
        if not url_download:
            return jsonify({"status": "Recebido sem URL"}), 200

        r = requests.get(url_download)
        r.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            nome_csv = z.namelist()[0]
            print(f"[DEBUG CUSTODIA] Lendo: {nome_csv}", flush=True)
            with z.open(nome_csv) as f:
                df = pd.read_csv(f, sep=",", encoding="latin1", low_memory=False)

        # Backup raw (replace — custódia é snapshot diário completo)
        df_raw = df.copy()
        df_raw["data_recebimento_webhook"] = datetime.now()
        salvar_df_otimizado(df_raw, "backup_custodia_raw", if_exists="replace")

        # Tratamento de datas — mantidas como DATE para queries corretas no banco
        df_final = df.copy()
        for col in ["referenceDate", "dataInicio", "fixingDate", "dataKnockIn"]:
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(df_final[col], format="%d/%m/%Y", errors="coerce")

        df_final["data_upload"] = datetime.now()
        salvar_df_otimizado(df_final, "relatorios_custodia", if_exists="replace")

        msg = f"Importação Custódia concluída. Linhas: {len(df_final)}"
        print(f"[SUCESSO CUSTODIA] {msg}", flush=True)
        registrar_log("CUSTODIA", "Sucesso", len(df_final), msg)
        return jsonify({"status": "Sucesso", "linhas": len(df_final)}), 200

    except Exception as e:
        print(f"[ERRO CRÍTICO CUSTODIA] {e}", flush=True)
        registrar_log("CUSTODIA", "Erro", 0, str(e))
        return jsonify({"erro": str(e)}), 500


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