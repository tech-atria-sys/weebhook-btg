import os
import io
import csv
import uuid
import json
import requests
import pyodbc
from flask import Flask, request, jsonify

app = Flask(__name__)

# 1. CONFIGURAÇÕES (Preencher no Render > Environment)

# Banco de Dados
SERVER_NAME = os.getenv("SERVER_NAME")
DATABASE_NAME = os.getenv("DATABASE_NAME")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Segurança Interna 
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN") 

# Credenciais do BTG 
BTG_CLIENT_ID = os.getenv("BTG_CLIENT_ID")     # btg_client_id
BTG_CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET") # btg_client_secret

# URLs
# URL de Auth (F fixa pois é padrão do BTG)
URL_AUTH_BTG = "https://api.btgpactual.com/iaas-auth/api/v1/authorization/oauth2/accesstoken"

# URL do Relatório NNM (A que estava na doc inicial)
# (Ou a URL exata que estiver no Swagger deles)
URL_REPORT_NNM = os.getenv("PARTNER_REPORT_URL")

CONN_STR = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes"

# 2. FUNÇÕES AUXILIARES

def clean_decimal(valor):
    if not valor: return 0.0
    v = str(valor).strip()
    if '.' in v and ',' in v: v = v.replace('.', '').replace(',', '.')
    elif ',' in v: v = v.replace(',', '.')
    try: return float(v)
    except: return 0.0

def clean_bool(valor):
    return 1 if str(valor).lower() == 'true' else 0

def get_btg_token():
    """
    Gera o token
    """
    url = "https://api.btgpactual.com/iaas-auth/api/v1/authorization/oauth2/accesstoken"
    headers = {
        'x-id-partner-request': str(uuid.uuid4()),
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }
    payload = {'grant_type': 'client_credentials'}
    
    # Importante: Verifique se no Render o nome está exatamente "btg_client_id"
    auth = (os.getenv("BTG_CLIENT_ID"), os.getenv("BTG_CLIENT_SECRET"))
    
    try:
        print("Solicitando Token BTG (Via Headers)...")
        r = requests.post(url, data=payload, headers=headers, auth=auth)
        
        if r.status_code == 200:
            # Pega o token do CABEÇALHO, não do corpo.
            token = r.headers.get('access_token')
            return token
        else:
            print(f"[ERRO AUTH] Status: {r.status_code} - {r.text}")
            return None
    except Exception as e:
        print(f"[ERRO CRÍTICO AUTH] {str(e)}")
        return None

# 3. ROTA GATILHO (VOCÊ ACESSA PARA PEDIR O RELATÓRIO)

@app.route('/trigger/nnm', methods=['GET'])
def trigger_nnm():
    # 1. Verifica sua senha interna
    if request.args.get('token') != WEBHOOK_TOKEN:
        return jsonify({"erro": "Acesso negado"}), 403

    # 2. Pega o Token válido no BTG
    access_token = get_btg_token()
    if not access_token:
        return jsonify({"erro": "Falha ao autenticar no BTG"}), 502

    # 3. Monta a requisição para o Relatório NNM
    # A Doc diz que precisa do Header 'access_token' (igual ao seu script Position)
    headers = {
        'x-id-partner-request': str(uuid.uuid4()),
        'access_token': access_token, 
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        print(f"[TRIGGER] Pedindo relatório NNM em: {URL_REPORT_NNM}")
        # A doc inicial sugeria GET. Se der erro 405, troque para requests.post
        r = requests.get(URL_REPORT_NNM, headers=headers)
        
        # Status 202 = Aceito (Vai processar e mandar Webhook depois)
        if r.status_code == 202:
            return jsonify({
                "status": "Solicitado",
                "mensagem": "O BTG aceitou o pedido. Aguarde o Webhook.",
                "http_code": 202
            }), 202
            
        elif r.status_code == 200:
            # Caso raro onde eles devolvem na hora
            return jsonify({"status": "Retornado na hora (Inesperado)", "dados": r.json()}), 200

        else:
            return jsonify({"erro_btg": r.text, "status": r.status_code}), r.status_code

    except Exception as e:
        return jsonify({"erro": str(e)}), 500


# 4. Rota webhook que o BTG chama para enviar o relatório (Eles chamam essa URL com o JSON do relatório)

@app.route('/webhook/nnm', methods=['POST'])
def webhook_nnm():
    # Verifica sua senha interna
    if request.args.get('token') != WEBHOOK_TOKEN:
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    print(f"[WEBHOOK] Payload: {dados}")

    try:
        # Pega a URL de download do JSON
        url_download = dados.get('response', {}).get('url') or dados.get('url')
        
        if not url_download:
            # Pode ser mensagem de "Aguarde janela"
            msg = dados.get('message', 'Sem mensagem')
            print(f"[AVISO] Sem URL de download. Msg: {msg}")
            return jsonify({"status": "Recebido (Sem URL)"}), 200

        # Baixa o CSV
        print(f"[DOWNLOAD] Baixando de: {url_download}")
        r = requests.get(url_download)
        r.raise_for_status()

        # Lê o CSV
        f = io.StringIO(r.content.decode('utf-8'))
        reader = csv.DictReader(f, delimiter=';') # Confirme se é ; ou ,

        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

        sql = """
        INSERT INTO dbo.relatorios_nnm_gerencial 
        (nr_conta, dt_captacao, ativo, mercado, cge_officer, tipo_lancamento, descricao, 
         qtd, valor_captacao, is_officer_nnm, is_partner_nnm, is_channel_nnm, is_bu_nnm, 
         submercado, submercado_detalhado, data_upload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """

        linhas = 0
        for row in reader:
            cursor.execute(sql, (
                row.get('nr_conta'),
                row.get('dt_captacao'),
                row.get('ativo'),
                row.get('mercado'),
                row.get('cge_officer'),
                row.get('tipo_lancamento'),
                row.get('descricao'),
                clean_decimal(row.get('qtd')),
                clean_decimal(row.get('captacao') or row.get('valor_captacao')), 
                clean_bool(row.get('is_officer_nnm')),
                clean_bool(row.get('is_partner_nnm')),
                clean_bool(row.get('is_channel_nnm')),
                clean_bool(row.get('is_bu_nnm')),
                row.get('submercado'),
                row.get('submercado_detalhado')
            ))
            linhas += 1
            
        conn.commit()
        conn.close()
        print(f"[SUCESSO] {linhas} linhas importadas.")
        return jsonify({"status": "Sucesso", "linhas": linhas}), 200

    except Exception as e:
        print(f"[ERRO CRITICO] {e}")
        return jsonify({"erro": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)