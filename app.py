import os
import io
import csv
import re
import requests
import zipfile
import pyodbc
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- 1. CONFIGURAÇÕES GERAIS E BANCO DE DADOS ---
# Essas variáveis valem para TODAS as rotas
SERVER_NAME = os.getenv("SERVER_NAME")
DATABASE_NAME = os.getenv("DATABASE_NAME")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN") 

CONN_STR = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes"

# --- 2. FUNÇÕES AUXILIARES (Compartilhadas) ---
def extrair_conta_do_nome(nome_arquivo):
    """Extrai números de strings (usado no PDF)"""
    match = re.search(r'(\d+)', nome_arquivo)
    return match.group(1) if match else None

def clean_decimal(valor):
    """Limpa moeda PT-BR ou EN-US para float (usado no CSV)"""
    if not valor: return 0.0
    v = str(valor).strip()
    if '.' in v and ',' in v: v = v.replace('.', '').replace(',', '.') # 1.000,00 -> 1000.00
    elif ',' in v: v = v.replace(',', '.') # 1000,00 -> 1000.00
    try: return float(v)
    except: return 0.0

def clean_bool(valor):
    """Converte 'true' texto para bit 1/0"""
    return 1 if str(valor).lower() == 'true' else 0

def validar_token():
    """Verifica se quem chamou tem a senha correta"""
    token = request.args.get('token')
    if token != WEBHOOK_TOKEN:
        return False
    return True

# ==========================================
# ROTA 1: RELATÓRIO DE PERFORMANCE (PDFs/ZIP)
# URL: https://seu-app.onrender.com/webhook/performance
# ==========================================
@app.route('/webhook/performance', methods=['POST'])
def rota_performance():
    if not validar_token(): return jsonify({"erro": "Token Invalido"}), 403

    dados = request.json
    print(f"[PERF] Recebido: {dados}")
    
    if not dados: return jsonify({"erro": "Payload vazio"}), 400

    try:
        # Lógica Específica dos PDFs (aquela antiga sua)
        res = dados.get('response') or dados.get('partnerResponse') or {}
        url_download = res.get('url')
        if not url_download: return jsonify({"erro": "Sem URL"}), 400
        
        # ... (Mantive resumido aqui, mas entra toda aquela sua lógica de ZIP e Merge)
        # Código de download do ZIP e inserção na tabela de performance...
        
        return jsonify({"status": "Processado Performance"}), 200
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

# ==========================================
# ROTA 2: RELATÓRIO NNM (CSV/Gerencial)
# URL: https://seu-app.onrender.com/webhook/nnm
# ==========================================
@app.route('/webhook/nnm', methods=['POST'])
def rota_nnm():
    if not validar_token(): return jsonify({"erro": "Token Invalido"}), 403

    dados = request.json
    print(f"[NNM] Recebido: {dados}")

    if not dados: return jsonify({"erro": "Payload vazio"}), 400

    try:
        # 1. Pega URL
        url = dados.get('response', {}).get('url')
        if not url:
            return jsonify({'status': 'Aguardando janela/Sem URL'}), 200

        # 2. Baixa CSV
        r = requests.get(url)
        r.raise_for_status()
        
        # 3. Processa CSV
        arquivo = io.StringIO(r.content.decode('utf-8'))
        reader = csv.DictReader(arquivo, delimiter=';') # Confirme se é ; ou ,

        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

        # Insere na tabela de STAGING ou na OFICIAL conforme definimos antes
        sql = """
        INSERT INTO dbo.relatorios_nnm_gerencial 
        (nr_conta, dt_captacao, ativo, mercado, tipo_lancamento, valor_captacao, data_upload)
        VALUES (?, ?, ?, ?, ?, ?, GETDATE())
        """
        
        for row in reader:
            # Tratamento específico para este relatório
            cursor.execute(sql, (
                row.get('nr_conta'),
                row.get('dt_captacao'),
                row.get('ativo'),
                row.get('mercado'),
                row.get('tipo_lancamento'),
                clean_decimal(row.get('captacao'))
            ))
        
        conn.commit()
        conn.close()
        return jsonify({"status": "Processado NNM"}), 200

    except Exception as e:
        print(f"[NNM ERRO] {e}")
        return jsonify({"erro": str(e)}), 500

# ==========================================
# ROTA 3: BASE BTG (Futuro)
# URL: https://seu-app.onrender.com/webhook/basebtg
# ==========================================
@app.route('/webhook/basebtg', methods=['POST'])
def rota_basebtg():
    if not validar_token(): return jsonify({"erro": "Token Invalido"}), 403
    
    dados = request.json
    print(f"[BASE BTG] Recebido: {dados}")
    
    # Aqui você colocará a lógica específica quando tiver a documentação dessa base
    # Ex: baixar JSON de clientes, atualizar cadastro, etc.
    
    return jsonify({"status": "Recebido (Ainda não implementado)"}), 200

# --- INICIALIZAÇÃO ---
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)