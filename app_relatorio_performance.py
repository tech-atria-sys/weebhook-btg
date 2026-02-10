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

    # 2. Extração dos dados com fallback para identificar a conta
    try:
        res = dados.get('response', {})
        url_download = res.get('url')
        data_ref = res.get('endDate')
        
        # Tenta capturar a conta de múltiplas origens para evitar o "None"
        conta_payload = (
            res.get('accountNumber') or  # Opção 1: Dentro do response
            dados.get('cge') or          # Opção 2: Raiz do JSON (CGE)
            dados.get('accountNumber')   # Opção 3: Raiz do JSON
        )

        print(f"--- NOVA REQUISIÇÃO RECEBIDA ---")
        print(f"Conta Identificada: {conta_payload}")
        print(f"ID da Requisição: {dados.get('idPartnerRequest')}")

        if not url_download:
            print(f"ERRO: URL de download não enviada para a conta {conta_payload}")
            return jsonify({"erro": "URL nao encontrada", "conta": conta_payload}), 400

        # 3. Download
        print(f"Baixando arquivo para conta {conta_payload}...")
        r = requests.get(url_download, stream=True)
        r.raise_for_status()

        # 4. Processamento do ZIP
        arquivos_salvos = 0
        
        print("Conectando ao Banco de Dados...")
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            print(f"Arquivos dentro do ZIP: {z.namelist()}")
            
            for nome_arquivo in z.namelist():
                if nome_arquivo.lower().endswith('.pdf'):
                    print(f"📄 Processando PDF: {nome_arquivo}")
                    
                    # Usa a conta do payload ou extrai do nome do arquivo se falhar
                    conta_final = conta_payload if conta_payload else extrair_conta_do_nome(nome_arquivo)
                    
                    if conta_final:
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
                        
                        params = (
                            str(conta_final), 
                            pdf_bytes, nome_arquivo, data_ref, 
                            str(conta_final), pdf_bytes, nome_arquivo, data_ref
                        )
                        
                        cursor.execute(sql_merge, params)
                        arquivos_salvos += 1
                        print(f"Conta {conta_final} salva no banco!")
                    else:
                        print(f"Arquivo {nome_arquivo} ignorado: ID da conta não identificado.")

        # 5. Finalização
        if arquivos_salvos > 0:
            conn.commit()
            print(f"COMMIT REALIZADO. Total salvos: {arquivos_salvos}")
            conn.close()
            return jsonify({"status": "Processado", "arquivos_salvos": arquivos_salvos}), 200
        else:
            print("NENHUM PDF ENCONTRADO NO ZIP.")
            conn.close()
            return jsonify({"status": "Alerta", "mensagem": "Nenhum PDF processado."}), 200

    except Exception as e:
        print(f"ERRO CRÍTICO no processamento: {e}")
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