import os
import io
import re
import requests
import zipfile
import pyodbc
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- CONFIGURAÇÕES ---
SERVER_NAME = os.getenv("SERVER_NAME")
DATABASE_NAME = os.getenv("DATABASE_NAME")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN") 

CONN_STR = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes"

def extrair_conta_do_nome(nome_arquivo):
    match = re.search(r'(\d+)', nome_arquivo)
    return match.group(1) if match else None

@app.route('/webhook', methods=['POST'])
def receber_webhook():
    token_recebido = request.args.get('token')
    if token_recebido != WEBHOOK_TOKEN:
        return jsonify({"erro": "Token Invalido"}), 403

    dados = request.json
    if not dados:
        return jsonify({"erro": "Payload vazio"}), 400

    try:
        # Extração Robusta
        res = dados.get('response') or dados.get('partnerResponse') or {}
        url_download = res.get('url') if isinstance(res, dict) else None
        data_ref = res.get('endDate') if isinstance(res, dict) else None
        req_id = dados.get('idPartnerRequest', 'N/A')
        
        # Busca Identificador da Conta
        conta_raw = res.get('accountNumber') or dados.get('cge') or dados.get('accountNumber')
        conta_id = str(conta_raw) if (conta_raw and str(conta_raw).lower() != 'null') else "Desconhecida"

        # Caso 1: Sem URL (Relatório não gerado pelo BTG)
        if not url_download:
            print(f"[ERRO 400] Conta: {conta_id} | Status: URL nao encontrada | ID: {req_id}")
            return jsonify({"erro": "URL nao encontrada", "conta": conta_id}), 400

        # Caso 2: Processamento com Sucesso
        r = requests.get(url_download, stream=True)
        r.raise_for_status()

        arquivos_salvos = 0
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            for nome_arquivo in z.namelist():
                if nome_arquivo.lower().endswith('.pdf'):
                    conta_final = conta_id if conta_id != "Desconhecida" else extrair_conta_do_nome(nome_arquivo)
                    pdf_bytes = z.read(nome_arquivo)
                    
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
                    cursor.execute(sql_merge, (conta_final, pdf_bytes, nome_arquivo, data_ref, conta_final, pdf_bytes, nome_arquivo, data_ref))
                    arquivos_salvos += 1

        conn.commit()
        conn.close()
        
        # Log de Sucesso em Linha Única
        print(f"[SUCESSO] Conta: {conta_id} | Salvos: {arquivos_salvos} | Ref: {data_ref} | ID: {req_id}")
        return jsonify({"status": "Processado", "conta": conta_id}), 200

    except Exception as e:
        print(f"[CRITICO] Conta: {conta_id if 'conta_id' in locals() else 'N/A'} | Erro: {str(e)[:50]} | ID: {req_id if 'req_id' in locals() else 'N/A'}")
        return jsonify({"erro": str(e)}), 500

@app.route('/meu-ip', methods=['GET'])
def get_ip():
    try:
        return jsonify({'ip_render': requests.get('https://api.ipify.org').text})
    except:
        return jsonify({'erro': 'falha ao obter ip'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)