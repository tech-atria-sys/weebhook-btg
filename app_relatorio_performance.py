import os
import io
import re
import requests
import zipfile
import pyodbc
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- CONFIGURAÇÕES (Lendo suas variáveis exatas) ---
SERVER_NAME = os.getenv("SERVER_NAME")       # Endereço do servidor
DATABASE_NAME = os.getenv("DATABASE_NAME")   # Nome do banco
USERNAME = os.getenv("USERNAME")             # Usuário
PASSWORD = os.getenv("PASSWORD")             # Senha

# Adicionei essa aqui para validar a segurança do Webhook (quem chama sua API)
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN")   

BTG_CLIENTE_ID = os.getenv("BTG_CLIENTE_ID")
BTG_CLIENTE_SECRET = os.getenv("BTG_CLIENTE_SECRET")

# String de Conexão Atualizada
CONN_STR = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes"

def extrair_conta_do_nome(nome_arquivo):
    match = re.search(r'(\d+)', nome_arquivo)
    if match:
        return match.group(1)
    return None

@app.route('/webhook', methods=['POST'])
def receber_webhook():
    # 1. Verifica se quem chamou tem a senha correta (Token na URL)
    token_recebido = request.args.get('token')
    
    # Se você não configurou o WEBHOOK_TOKEN no Render, ele avisa
    if not WEBHOOK_TOKEN:
        print("AVISO: Variável WEBHOOK_TOKEN não configurada no Render.")
    
    if token_recebido != WEBHOOK_TOKEN:
        return "Acesso Negado (Token Inválido)", 403

    dados = request.json
    
    if not dados or 'response' not in dados:
        return "Payload inválido", 400

    url_download = dados['response'].get('url')
    conta_payload = dados['response'].get('accountNumber') 
    data_ref = dados['response'].get('endDate')

    if not url_download:
        return "URL não encontrada", 400

    try:
        print(f"⬇️ Baixando arquivo da conta {conta_payload}...")
        r = requests.get(url_download, stream=True)
        r.raise_for_status()

        print("🔌 Conectando ao Banco...")
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            for nome_arquivo in z.namelist():
                if nome_arquivo.lower().endswith('.pdf'):
                    
                    conta_final = conta_payload if conta_payload else extrair_conta_do_nome(nome_arquivo)
                    
                    if conta_final:
                        pdf_bytes = z.read(nome_arquivo)
                        
                        # MERGE (Atualiza ou Insere)
                        sql_merge = """
                        MERGE dbo.relatorios_performance_atual AS Target
                        USING (SELECT ? AS ContaVal) AS Source
                        ON (Target.conta = Source.ContaVal)
                        WHEN MATCHED THEN
                            UPDATE SET arquivo_pdf = ?, nome_arquivo = ?, data_referencia = ?, data_upload = GETDATE()
                        WHEN NOT MATCHED THEN
                            INSERT (conta, arquivo_pdf, nome_arquivo, data_referencia, data_upload)
                            VALUES (?, ?, ?, ?, GETDATE());
                        """
                        params = (str(conta_final), pdf_bytes, nome_arquivo, data_ref, str(conta_final), pdf_bytes, nome_arquivo, data_ref)
                        cursor.execute(sql_merge, params)
                        print(f"✅ Conta {conta_final} salva com sucesso!")
                    
        conn.commit()
        conn.close()
        return "Processado", 200

    except Exception as e:
        print(f"Erro Crítico: {e}")
        return f"Erro: {str(e)}", 500

# Adicione isso no final do arquivo, antes do if __name__
@app.route('/meu-ip', methods=['GET'])
def get_ip():
    try:
        # Pergunta para um serviço externo qual é o meu IP público
        ip = requests.get('https://api.ipify.org').text
        return jsonify({'ip_render': ip})
    except Exception as e:
        return jsonify({'erro': str(e)}), 500


if __name__ == '__main__':
    # Porta 10000 é padrão do Render
    app.run(host='0.0.0.0', port=10000)