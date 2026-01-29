# %%
from flask import Flask, request, send_file
import requests
import os

# %%
app = Flask(__name__)

# Configura a pasta onde os arquivos vão cair
PASTA_DESTINO = "relatorios_baixados"
if not os.path.exists(PASTA_DESTINO):
    os.makedirs(PASTA_DESTINO)

# Variável global para lembrar o nome do último arquivo (memória curta do servidor)
ultimo_arquivo_gerado = None

@app.route('/webhook', methods=['POST'])
def receber_webhook():
    global ultimo_arquivo_gerado
    print("Recebendo dados...")
    
    dados = request.json
    
    # Verifica se veio a URL de download
    if dados and 'response' in dados and 'url' in dados['response']:
        url_download = dados['response']['url']
        data_fim = dados['response'].get('endDate', 'data_desconhecida')
        
        print(f"Baixando relatório de {data_fim}...")
        
        try:
            # Baixa o arquivo do link
            r = requests.get(url_download)
            nome_arquivo_completo = f"{PASTA_DESTINO}/performance_{data_fim}.zip"
            
            with open(nome_arquivo_completo, 'wb') as f:
                f.write(r.content)
            
            # Atualiza a memória do servidor
            ultimo_arquivo_gerado = nome_arquivo_completo
                
            print(f"✅ SUCESSO! Arquivo salvo: {nome_arquivo_completo}")
            
        except Exception as e:
            print(f"Erro ao baixar o arquivo: {e}")
            return "Erro no download interno", 500
            
    else:
        print("Recebi webhook, mas sem URL de download.")

    return "OK", 200

# --- NOVA ROTA MÁGICA ---
@app.route('/meus-dados', methods=['GET'])
def baixar_para_pc():
    global ultimo_arquivo_gerado
    
    if ultimo_arquivo_gerado and os.path.exists(ultimo_arquivo_gerado):
        print(f"Enviando arquivo {ultimo_arquivo_gerado} para o João...")
        return send_file(ultimo_arquivo_gerado, as_attachment=True)
    else:
        return "Nenhum arquivo foi gerado recentemente ou o servidor reiniciou.", 404

if __name__ == '__main__':
    # Configuração para rodar na nuvem
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
