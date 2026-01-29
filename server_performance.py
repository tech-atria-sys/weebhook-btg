# %%
from flask import Flask, request, send_file
import requests
import os
import zipfile # Biblioteca para criar o pacotão

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
        
        # Cria um nome único usando a data e um pedaço da URL para não substituir arquivos iguais
        # Ex: performance_2025-01-01_xyz123.zip
        id_unico = url_download.split('/')[-1][-5:] 
        nome_arquivo = f"performance_{data_fim}_{id_unico}.zip"
        caminho_completo = os.path.join(PASTA_DESTINO, nome_arquivo)
        
        print(f"Baixando relatório de {data_fim}...")
        
        try:
            r = requests.get(url_download)
            with open(caminho_completo, 'wb') as f:
                f.write(r.content)
            
            print(f"Arquivo salvo: {nome_arquivo}")
            
        except Exception as e:
            print(f"Erro ao baixar o arquivo: {e}")
            return "Erro interno", 500
            
    else:
        print("Recebi webhook, mas sem URL de download.")

    return "OK", 200

# --- ROTA DO PACOTÃO ---
@app.route('/meus-dados', methods=['GET'])
def baixar_tudo_de_uma_vez():
    # Nome do arquivo final que você vai baixar
    nome_pacote = "TODOS_RELATORIOS.zip"
    
    # Lista todos os arquivos que estão na pasta
    arquivos_na_pasta = os.listdir(PASTA_DESTINO)
    
    if not arquivos_na_pasta:
        return "A pasta está vazia! Rode o script de solicitar primeiro.", 404

    print(f"📦 Empacotando {len(arquivos_na_pasta)} arquivos...")

    # Cria o ZIPÃO
    with zipfile.ZipFile(nome_pacote, 'w') as zipf:
        for arquivo in arquivos_na_pasta:
            caminho_arquivo = os.path.join(PASTA_DESTINO, arquivo)
            # Adiciona o arquivo dentro do zip
            zipf.write(caminho_arquivo, arcname=arquivo)
            
    print("Enviando...")
    return send_file(nome_pacote, as_attachment=True)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
