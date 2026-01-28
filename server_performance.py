# %%
from flask import Flask, request
import requests
import os

# %%
app = Flask(__name__)

# Configura a pasta onde os arquivos vão cair
PASTA_DESTINO = "relatorios_baixados"
if not os.path.exists(PASTA_DESTINO):
    os.makedirs(PASTA_DESTINO)

@app.route('/webhook', methods=['POST'])
def receber_webhook():
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
            nome_arquivo = f"{PASTA_DESTINO}/performance_{data_fim}.zip"
            
            with open(nome_arquivo, 'wb') as f:
                f.write(r.content)
                
            print(f"✅ SUCESSO! Arquivo salvo na pasta: {nome_arquivo}")
            
        except Exception as e:
            print(f"Erro ao baixar o arquivo: {e}")
            
    else:
        # Se veio erro ou formato estranho
        print("Recebi algo, mas não tinha link de download.")
        if 'errors' in dados:
            print(f"Erro reportado pelo BTG: {dados['errors']}")
        else:
            print(dados)

    return "OK", 200

if __name__ == '__main__':
    # O Render escolhe a porta sozinho, precisamos pegar ela
    port = int(os.environ.get("PORT", 10000))
    # host='0.0.0.0' é OBRIGATÓRIO na nuvem para receber conexões de fora
    print(f"Rodando na porta {port}...")
    app.run(host='0.0.0.0', port=port)
