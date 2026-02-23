import os
import io
import csv
import uuid
import json
import requests
import pyodbc
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from flask import Flask, request, jsonify

app = Flask(__name__)

# 1. CONFIGURAÇÕES (Via Environment Variables no Render)
SERVER_NAME = os.getenv("SERVER_NAME")
DATABASE_NAME = os.getenv("DATABASE_NAME")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN") 
BTG_CLIENT_ID = os.getenv("BTG_CLIENT_ID")
BTG_CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET")
URL_REPORT_NNM = os.getenv("PARTNER_REPORT_URL")
URL_REPORT_BASE = os.getenv("PARTNER_REPORT_BASE_URL")

# String de Conexão
CONN_STR = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes"

# 2. FUNÇÕES AUXILIARES

def registrar_log(atividade, status, linhas=0, mensagem=""):
    """Grava o resultado da operação na tabela dbo.logs_atividades"""
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
        print(f"[ERRO AUTH] {str(e)}")
        return None

# 3. ROTAS DE GATILHO (Chamadas pelo GitHub Actions)

@app.route('/trigger/nnm', methods=['GET'])
def trigger_nnm():
    if request.args.get('token') != WEBHOOK_TOKEN:
        return jsonify({"erro": "Acesso negado"}), 403

    access_token = get_btg_token()
    if not access_token:
        registrar_log('TRIGGER_NNM', 'Erro', 0, "Falha na geração do token BTG")
        return jsonify({"erro": "Falha ao autenticar no BTG"}), 502

    headers = {'x-id-partner-request': str(uuid.uuid4()), 'access_token': access_token, 'Content-Type': 'application/json'}

    try:
        r = requests.get(URL_REPORT_NNM, headers=headers)
        if r.status_code == 202:
            registrar_log('TRIGGER_NNM', 'Sucesso', 0, "Solicitação aceita pelo BTG")
            return jsonify({"status": "Solicitado", "http_code": 202}), 202
        return jsonify({"erro_btg": r.text}), r.status_code
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/trigger/basebtg', methods=['GET'])
def trigger_basebtg():
    if request.args.get('token') != WEBHOOK_TOKEN:
        return jsonify({"erro": "Acesso negado"}), 403

    access_token = get_btg_token()
    if not access_token:
        registrar_log('TRIGGER_BASE', 'Erro', 0, "Falha na geração do token BTG")
        return jsonify({"erro": "Falha ao autenticar no BTG"}), 502

    headers = {'x-id-partner-request': str(uuid.uuid4()), 'access_token': access_token, 'Content-Type': 'application/json'}

    try:
        r = requests.get(URL_REPORT_BASE, headers=headers)
        if r.status_code == 202:
            registrar_log('TRIGGER_BASE', 'Sucesso', 0, "Solicitação de Base aceita pelo BTG")
            return jsonify({"status": "Solicitado", "http_code": 202}), 202
        return jsonify({"erro_btg": r.text}), r.status_code
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

# 4. ROTAS DE WEBHOOK (Recebem os dados do BTG)

@app.route('/webhook/nnm', methods=['POST'])
def webhook_nnm():
    if request.args.get('token') != WEBHOOK_TOKEN:
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    try:
        url_download = dados.get('response', {}).get('url') or dados.get('url')
        if not url_download:
            registrar_log('NNM', 'Aviso', 0, "Webhook recebido sem URL")
            return jsonify({"status": "Recebido sem URL"}), 200

        r = requests.get(url_download)
        r.raise_for_status()
        
        df = pd.read_csv(io.StringIO(r.content.decode('utf-8')), sep=';')
        
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={CONN_STR}")
        with engine.begin() as conn:
            df['data_upload'] = datetime.now()
            df.to_sql("relatorios_nnm_gerencial", con=conn, schema="dbo", if_exists="append", index=False)
            
        registrar_log('NNM', 'Sucesso', len(df), "Importação NNM concluída")
        return jsonify({"status": "Sucesso", "linhas": len(df)}), 200
    except Exception as e:
        registrar_log('NNM', 'Erro', 0, str(e))
        return jsonify({"erro": str(e)}), 500

@app.route('/webhook/basebtg', methods=['POST'])
def webhook_base_btg():
    if request.args.get('token') != WEBHOOK_TOKEN:
        return jsonify({"erro": "Acesso negado"}), 403

    try:
        dados = request.json
        url_download = dados.get('response', {}).get('url') or dados.get('url')
        if not url_download: return jsonify({"erro": "URL não encontrada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()
        base = pd.read_csv(io.BytesIO(r.content))

        # Tratamentos
        base.rename(columns={
            "nm_assessor": "Assessor", "nr_conta": "Conta", "pl_total": "PL Total",
            "nome_completo": "Nome", "faixa_cliente": "Faixa Cliente"
        }, inplace=True)
        
        base['Conta'] = base['Conta'].astype(str)
        base['Assessor'] = base['Assessor'].str.upper()

        faixas_ate_300 = ["Ate 50K", "Entre 50k e 100k", "Entre 100k e 300k"]
        base.loc[base['Faixa Cliente'].isin(faixas_ate_300), "Faixa Cliente"] = "Até 300k"

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
        base.loc[base['Assessor'].isin(nomes_rodrigo), "Assessor"] = "RODRIGO DE MELLO D’ELIA"

        base.drop_duplicates(subset="Conta", keep='first', inplace=True)

        with engine.begin() as conn:
            base.to_sql("base_btg", con=conn, schema="dbo", if_exists="replace", index=False,
                         dtype={"Conta": sqlalchemy.types.VARCHAR(255)})
            
            df_hist = pd.DataFrame()
            df_hist['Conta'] = base['Conta']
            df_hist['Assessor'] = base['Assessor']
            df_hist['PL Total'] = base['PL Total']
            hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            df_hist['Data'] = hoje
            df_hist['Mês'] = hoje.strftime("%Y-%m")
            
            df_hist.to_sql("pl_historico_diario", con=conn, schema="dbo", if_exists="append", index=False)

        registrar_log('BASE_BTG', 'Sucesso', len(base), "Base e Histórico PL atualizados")
        return jsonify({"status": "Sucesso", "total": len(base)}), 200
    except Exception as e:
        registrar_log('BASE_BTG', 'Erro', 0, str(e))
        return jsonify({"erro": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)