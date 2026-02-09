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

# String de Conexão
CONN_STR = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes"

def extrair_conta_do_nome(nome_arquivo):
    # Tenta achar numeros no nome do arquivo (ex: Relatorio_12345.pdf -> 12345)
    match = re.search(r'(\d+)', nome_arquivo)
    if match:
        return match.group(1)
    return None

@app.route('/webhook', methods=['POST'])
def receber_webhook():
    # 1. Segurança
    token_recebido = request.args.get('token')
    if token_recebido != WEBHOOK_TOKEN:
        return jsonify({"erro": "Token Invalido"}), 403

    dados = request.json
    if not dados or 'response' not in dados:
        return jsonify({"erro": "Payload invalido"}), 400

    # 2. Extração dos dados
    try:
        url_download = dados['response'].get('url')
        conta_payload = dados['response'].get('accountNumber') 
        data_ref = dados['response'].get('endDate')

        print(f"Recebido pedido para conta: {conta_payload}")

        if not url_download:
            return jsonify({"erro": "URL nao encontrada"}), 400

        # 3. Download
        print(f"Baixando arquivo...")
        r = requests.get(url_download, stream=True)
        r.raise_for_status()

        # 4. Processamento do ZIP
        arquivos_salvos = 0
        
        print("Conectando ao Banco de Dados...")
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            print(f"Arquivos dentro do ZIP: {z.namelist()}") # <--- LOG IMPORTANTE
            
            for nome_arquivo in z.namelist():
                # Verifica se é PDF (ignore case)
                if nome_arquivo.lower().endswith('.pdf'):
                    
                    print(f"📄 Processando PDF: {nome_arquivo}")
                    
                    # Prioriza a conta que veio no JSON, se não tiver, tenta pegar do nome do arquivo
                    conta_final = conta_payload if conta_payload else extrair_conta_do_nome(nome_arquivo)
                    
                    if conta_final:
                        pdf_bytes = z.read(nome_arquivo)
                        
                        # QUERY DE MERGE (Salva ou Atualiza)
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
                        
                        # Parametros mapeados para os ? na ordem exata
                        params = (
                            str(conta_final),               # SELECT ? (Source)
                            pdf_bytes, nome_arquivo, data_ref, # UPDATE SET (?, ?, ?)
                            str(conta_final), pdf_bytes, nome_arquivo, data_ref # INSERT VALUES (?, ?, ?, ?)
                        )
                        
                        cursor.execute(sql_merge, params)
                        arquivos_salvos += 1
                        print(f"Conta {conta_final} salva no banco!")
                    else:
                        print(f"Arquivo {nome_arquivo} ignorado: Não achei número da conta.")

        # 5. Finalização
        if arquivos_salvos > 0:
            conn.commit()
            print(f"COMMIT REALIZADO. Total salvos: {arquivos_salvos}")
            conn.close()
            return jsonify({"status": "Processado", "arquivos_salvos": arquivos_salvos}), 200
        else:
            print("NENHUM PDF ENCONTRADO NO ZIP.")
            conn.close()
            return jsonify({"status": "Alerta", "mensagem": "ZIP baixado, mas nenhum PDF encontrado dentro dele."}), 200

    except Exception as e:
        print(f"ERRO CRÍTICO: {e}")
        return jsonify({"erro": str(e)}), 500

@app.route('/meu-ip', methods=['GET'])
def get_ip():
    try:
        ip = requests.get('https://api.ipify.org').text
        return jsonify({'ip_render': ip})
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)