import os
import io
import uuid
import math
import requests
import pyodbc
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

# 1. CONFIGURACOES (Via Environment Variables no Render)
SERVER_NAME = os.getenv("SERVER_NAME")
DATABASE_NAME = os.getenv("DATABASE_NAME")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN") 
BTG_CLIENT_ID = os.getenv("BTG_CLIENT_ID")
BTG_CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET")

# Atualizado com os novos nomes definidos no Render
URL_REPORT_NNM = os.getenv("PARTNER_REPORT_URL_NNM")
URL_REPORT_BASE = os.getenv("PARTNER_REPORT_URL_BASEBTG")

CONN_STR = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes"

# 2. FUNCOES AUXILIARES E ENGINE DE CARGA

def registrar_log(atividade, status, linhas=0, mensagem=""):
    try:
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={CONN_STR}")
        with engine.begin() as conn:
            sql = text("""
                INSERT INTO dbo.logs_atividades (atividade, status, linhas_processadas, mensagem_detalhe)
                VALUES (:atv, :st, :ln, :msg)
            """)
            conn.execute(sql, {"atv": atividade, "st": status, "ln": linhas, "msg": str(mensagem)[:500]})
    except Exception as e:
        print(f"Falha ao gravar log no banco: {e}")

def salvar_df_otimizado(df, nome_tabela, col_pk=None, if_exists='append', schema='dbo'):
    if df.empty:
        return

    num_colunas = len(df.columns)
    if num_colunas > 0:
        limit_params = math.floor(2090 / num_colunas)
        limit_rows = 1000
        safe_chunksize = max(1, min(limit_params, limit_rows))
    else:
        safe_chunksize = 1000

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={CONN_STR}", fast_executemany=True)
    
    with engine.begin() as conn:
        df.to_sql(
            name=nome_tabela,
            con=conn,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=safe_chunksize,
            method='multi' 
        )
        
        if col_pk and if_exists == 'replace':
            try:
                conn.execute(text(f'ALTER TABLE {schema}."{nome_tabela}" ALTER COLUMN "{col_pk}" VARCHAR(450) NOT NULL'))
                conn.execute(text(f'ALTER TABLE {schema}."{nome_tabela}" ADD PRIMARY KEY ("{col_pk}")'))
            except Exception as e:
                print(f"Aviso ao criar PK em {nome_tabela}: {e}")

def get_btg_token():
    url = "https://api.btgpactual.com/iaas-auth/api/v1/authorization/oauth2/accesstoken"
    headers = {
        'x-id-partner-request': str(uuid.uuid4()),
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }
    payload = {'grant_type': 'client_credentials'}
    auth = (BTG_CLIENT_ID, BTG_CLIENT_SECRET)
    
    try:
        r = requests.post(url, data=payload, headers=headers, auth=auth)
        if r.status_code == 200:
            return r.headers.get('access_token')
        return None
    except Exception as e:
        return None

# 3. ROTAS DE GATILHO

@app.route('/trigger/nnm', methods=['GET'])
def trigger_nnm():
    if request.args.get('token') != WEBHOOK_TOKEN: return jsonify({"erro": "Acesso negado"}), 403

    access_token = get_btg_token()
    if not access_token:
        registrar_log('TRIGGER_NNM', 'Erro', 0, "Falha na geracao do token BTG")
        return jsonify({"erro": "Falha ao autenticar"}), 502

    headers = {'x-id-partner-request': str(uuid.uuid4()), 'access_token': access_token, 'Content-Type': 'application/json'}
    try:
        r = requests.get(URL_REPORT_NNM, headers=headers)
        if r.status_code == 202:
            registrar_log('TRIGGER_NNM', 'Sucesso', 0, "Solicitacao aceite")
            return jsonify({"status": "Solicitado", "http_code": 202}), 202
        return jsonify({"erro_btg": r.text}), r.status_code
    except Exception as e: return jsonify({"erro": str(e)}), 500

@app.route('/trigger/basebtg', methods=['GET'])
def trigger_basebtg():
    if request.args.get('token') != WEBHOOK_TOKEN: return jsonify({"erro": "Acesso negado"}), 403

    access_token = get_btg_token()
    if not access_token:
        registrar_log('TRIGGER_BASE', 'Erro', 0, "Falha na geracao do token BTG")
        return jsonify({"erro": "Falha ao autenticar"}), 502

    headers = {'x-id-partner-request': str(uuid.uuid4()), 'access_token': access_token, 'Content-Type': 'application/json'}
    try:
        r = requests.get(URL_REPORT_BASE, headers=headers)
        if r.status_code == 202:
            registrar_log('TRIGGER_BASE', 'Sucesso', 0, "Solicitacao aceite")
            return jsonify({"status": "Solicitado", "http_code": 202}), 202
        return jsonify({"erro_btg": r.text}), r.status_code
    except Exception as e: return jsonify({"erro": str(e)}), 500

# 4. ROTAS DE WEBHOOK

@app.route('/webhook/nnm', methods=['POST'])
def webhook_nnm():
    if request.args.get('token') != WEBHOOK_TOKEN: return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    try:
        url_download = dados.get('response', {}).get('url') or dados.get('url')
        if not url_download: return jsonify({"status": "Recebido sem URL"}), 200

        r = requests.get(url_download)
        r.raise_for_status()
        
        df = pd.read_csv(io.StringIO(r.content.decode('utf-8')), sep=';')
        
        # Backup Raw
        df_raw = df.copy()
        df_raw['data_recebimento_webhook'] = datetime.now()
        salvar_df_otimizado(df_raw, "backup_nnm_raw", if_exists="append")
        
        # Tratamento e filtro de colunas para relatorios_nnm_gerencial
        df.rename(columns={
            'dt_captacao': 'data_captacao',
            'captacao': 'valor_captacao'
        }, inplace=True)
        
        colunas_tabela = [
            'nr_conta', 'data_captacao', 'ativo', 'mercado', 'cge_officer', 
            'tipo_lancamento', 'descricao', 'qtd', 'valor_captacao', 
            'is_officer_nnm', 'is_partner_nnm', 'is_channel_nnm', 'is_bu_nnm', 
            'submercado', 'submercado_detalhado'
        ]
        
        colunas_presentes = [c for c in colunas_tabela if c in df.columns]
        df_final = df[colunas_presentes].copy()
        df_final['data_upload'] = datetime.now()
        
        salvar_df_otimizado(df_final, "relatorios_nnm_gerencial", if_exists="append")
            
        registrar_log('NNM', 'Sucesso', len(df_final), "Importacao NNM concluida")
        return jsonify({"status": "Sucesso", "linhas": len(df_final)}), 200

    except Exception as e:
        registrar_log('NNM', 'Erro', 0, str(e))
        return jsonify({"erro": str(e)}), 500

@app.route('/webhook/basebtg', methods=['POST'])
def webhook_base_btg():
    if request.args.get('token') != WEBHOOK_TOKEN: return jsonify({"erro": "Acesso negado"}), 403

    try:
        dados = request.json
        url_download = dados.get('response', {}).get('url') or dados.get('url')
        if not url_download: return jsonify({"erro": "URL nao encontrada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()
        base = pd.read_csv(io.BytesIO(r.content))

        # Backup Raw
        base_raw = base.copy()
        base_raw['data_recebimento_webhook'] = datetime.now()
        salvar_df_otimizado(base_raw, "backup_base_btg_raw", if_exists="append")

        # Tratamentos
        base.rename(columns={
            "nm_assessor": "Assessor", "nr_conta": "Conta", "pl_total": "PL Total",
            "nome_completo": "Nome", "faixa_cliente": "Faixa Cliente"
        }, inplace=True)
        
        base['Conta'] = base['Conta'].astype(str)
        base['Assessor'] = base['Assessor'].str.upper()

        faixas_ate_300 = ["Ate 50K", "Entre 50k e 100k", "Entre 100k e 300k"]
        base.loc[base['Faixa Cliente'].isin(faixas_ate_300), "Faixa Cliente"] = "Ate 300k"

        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={CONN_STR}")
        with engine.connect() as conn:
            try:
                offshore = pd.read_sql('SELECT * FROM dbo.pl_offshore', conn)
                offshore['Conta'] = offshore['Conta'].astype(str)
            except:
                offshore = pd.DataFrame()

        base = pd.concat([offshore, base], axis=0, ignore_index=True)
        
        # Ajustes de Assessores
        base.loc[base['Assessor'] == "MURILO LUIZ SILVA GINO", "Assessor"] = "IZADORA VILLELA FREITAS"
        base.loc[base['Assessor'].str.contains("GABRIEL GUERRERO TORRES FONSECA", na=False), "Assessor"] = "MARCOS SOARES PEREIRA FILHO"
        nomes_rodrigo = ["RODRIGO DE MELLO D?ELIA", "RODRIGO DE MELLO DELIA", "RODRIGO DE MELLO DELIA"]
        base.loc[base['Assessor'].isin(nomes_rodrigo), "Assessor"] = "RODRIGO DE MELLO D'ELIA"

        base.drop_duplicates(subset="Conta", keep='first', inplace=True)

        salvar_df_otimizado(base, "base_btg", col_pk="Conta", if_exists="replace")
        
        df_hist = pd.DataFrame()
        df_hist['Conta'] = base['Conta']
        df_hist['Assessor'] = base['Assessor']
        df_hist['PL Total'] = base['PL Total']
        hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        df_hist['Data'] = hoje
        df_hist['Mês'] = hoje.strftime("%Y-%m")
        
        salvar_df_otimizado(df_hist, "pl_historico_diario", if_exists="append")

        registrar_log('BASE_BTG', 'Sucesso', len(base), "Base e Historico atualizados")
        return jsonify({"status": "Sucesso", "total": len(base)}), 200

    except Exception as e:
        registrar_log('BASE_BTG', 'Erro', 0, str(e))
        return jsonify({"erro": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)