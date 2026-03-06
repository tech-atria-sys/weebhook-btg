import os
import io
import re
import uuid
import math
import requests
import zipfile
import pyodbc
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

# 1. CONFIGURACOES (Via Environment Variables no Render)
SERVER_NAME = os.getenv("SERVER_NAME")
DATABASE_NAME = os.getenv("DATABASE_NAME")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN") 
BTG_CLIENT_ID = os.getenv("BTG_CLIENT_ID")
BTG_CLIENT_SECRET = os.getenv("BTG_CLIENT_SECRET")

# URLs dos relatorios (Lembre-se de criar a variavel PARTNER_REPORT_URL_CUSTODIA no Render)
URL_REPORT_NNM = os.getenv("PARTNER_REPORT_URL_NNM")
URL_REPORT_BASE = os.getenv("PARTNER_REPORT_URL_BASEBTG")
URL_REPORT_CUSTODIA = os.getenv("PARTNER_REPORT_URL_CUSTODIA")

CONN_STR = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes"

# 2. FUNCOES AUXILIARES E ENGINE DE CARGA

def registrar_log(atividade, status, linhas=0, mensagem=""):
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

def salvar_df_otimizado(df, nome_tabela, col_pk=None, if_exists='append', schema='dbo'):
    if df.empty:
        return

    num_colunas = len(df.columns)
    if num_colunas > 0:
        limit_params = math.floor(2090 / num_colunas)
        limit_rows = 1000
        safe_chunksize = max(1, min(limit_params, limit_rows))
    else:
        safe_chunksize = 1000

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={CONN_STR}", fast_executemany=True)
    
    with engine.begin() as conn:
        df.to_sql(
            name=nome_tabela,
            con=conn,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=safe_chunksize,
            method='multi' 
        )
        
        if col_pk and if_exists == 'replace':
            try:
                conn.execute(text(f'ALTER TABLE {schema}."{nome_tabela}" ALTER COLUMN "{col_pk}" VARCHAR(450) NOT NULL'))
                conn.execute(text(f'ALTER TABLE {schema}."{nome_tabela}" ADD PRIMARY KEY ("{col_pk}")'))
            except Exception as e:
                print(f"Aviso ao criar PK em {nome_tabela}: {e}")

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
        return None

def extrair_conta_do_nome(nome_arquivo):
    match = re.search(r'(\d+)', nome_arquivo)
    return match.group(1) if match else None


# 3. ROTAS DE GATILHO

@app.route('/trigger/nnm', methods=['GET'])
def trigger_nnm():
    if request.args.get('token') != WEBHOOK_TOKEN: return jsonify({"erro": "Acesso negado"}), 403

    access_token = get_btg_token()
    if not access_token:
        registrar_log('TRIGGER_NNM', 'Erro', 0, "Falha na geracao do token BTG")
        return jsonify({"erro": "Falha ao autenticar"}), 502

    headers = {'x-id-partner-request': str(uuid.uuid4()), 'access_token': access_token, 'Content-Type': 'application/json'}
    try:
        r = requests.get(URL_REPORT_NNM, headers=headers)
        if r.status_code == 202:
            registrar_log('TRIGGER_NNM', 'Sucesso', 0, "Solicitacao aceita")
            return jsonify({"status": "Solicitado", "http_code": 202}), 202
        return jsonify({"erro_btg": r.text}), r.status_code
    except Exception as e: return jsonify({"erro": str(e)}), 500

@app.route('/trigger/basebtg', methods=['GET'])
def trigger_basebtg():
    if request.args.get('token') != WEBHOOK_TOKEN: return jsonify({"erro": "Acesso negado"}), 403

    access_token = get_btg_token()
    if not access_token:
        registrar_log('TRIGGER_BASE', 'Erro', 0, "Falha na geracao do token BTG")
        return jsonify({"erro": "Falha ao autenticar"}), 502

    headers = {'x-id-partner-request': str(uuid.uuid4()), 'access_token': access_token, 'Content-Type': 'application/json'}
    try:
        r = requests.get(URL_REPORT_BASE, headers=headers)
        if r.status_code == 202:
            registrar_log('TRIGGER_BASE', 'Sucesso', 0, "Solicitacao aceita")
            return jsonify({"status": "Solicitado", "http_code": 202}), 202
        return jsonify({"erro_btg": r.text}), r.status_code
    except Exception as e: return jsonify({"erro": str(e)}), 500

@app.route('/trigger/custodia', methods=['GET'])
def trigger_custodia():
    if request.args.get('token') != WEBHOOK_TOKEN: return jsonify({"erro": "Acesso negado"}), 403

    access_token = get_btg_token()
    if not access_token:
        registrar_log('TRIGGER_CUSTODIA', 'Erro', 0, "Falha na geracao do token BTG")
        return jsonify({"erro": "Falha ao autenticar"}), 502

    headers = {'x-id-partner-request': str(uuid.uuid4()), 'access_token': access_token, 'Content-Type': 'application/json'}
    try:
        r = requests.get(URL_REPORT_CUSTODIA, headers=headers)
        if r.status_code == 202:
            registrar_log('TRIGGER_CUSTODIA', 'Sucesso', 0, "Solicitacao aceita")
            return jsonify({"status": "Solicitado", "http_code": 202}), 202
        return jsonify({"erro_btg": r.text}), r.status_code
    except Exception as e: return jsonify({"erro": str(e)}), 500

@app.route('/trigger/carteiras-recomendadas', methods=['GET'])
def trigger_carteiras_recomendadas():
    # 1. Valida o token da sua API
    if request.args.get('token') != WEBHOOK_TOKEN: 
        return jsonify({"erro": "Acesso negado"}), 403

    # 2. Obtem o token do BTG
    access_token = get_btg_token()
    if not access_token:
        registrar_log('CARTEIRAS_RECOM', 'Erro', 0, "Falha na geracao do token BTG")
        return jsonify({"erro": "Falha ao autenticar no BTG"}), 502

    headers = {
        'x-id-partner-request': str(uuid.uuid4()), 
        'access_token': access_token, 
        'Content-Type': 'application/json'
    }
    
    try:
        # URL declarada diretamente dentro da funcao para evitar erros de escopo
        url_api_btg = "https://api.btgpactual.com/iaas-recommended-equities/api/v1/recommended-equities-allocation"
        r = requests.get(url_api_btg, headers=headers)
        
        if r.status_code == 200:
            dados = r.json()
            
            if not dados:
                return jsonify({"status": "Sucesso", "mensagem": "Nenhuma carteira retornada"}), 200

            # Achatar o JSON para um formato tabular (DataFrame)
            linhas_tabela = []
            for carteira in dados:
                carteira_base = {
                    "tipo_carteira": carteira.get("typeInitial"),
                    "descricao": carteira.get("description"),
                    "nome_carteira": carteira.get("name"),
                    "link_pdf": carteira.get("fileName"),
                    "rentabilidade_anterior": carteira.get("previousProfitability"),
                    "rentabilidade_acumulada": carteira.get("accumulatedProfitability"),
                    "inicio_validade": carteira.get("validityStart"),
                    "fim_validade": carteira.get("validityEnd"),
                    "data_extracao": datetime.now()
                }
                
                ativos = carteira.get("assets", [])
                
                if ativos:
                    for item in ativos:
                        linha = carteira_base.copy()
                        ativo_info = item.get("asset", {})
                        setor_info = ativo_info.get("sector", {})
                        
                        linha["ticker"] = ativo_info.get("ticker")
                        linha["empresa"] = ativo_info.get("company")
                        linha["setor"] = setor_info.get("name")
                        linha["peso"] = item.get("weight")
                        
                        linhas_tabela.append(linha)
                else:
                    linhas_tabela.append(carteira_base)
            
            df_carteiras = pd.DataFrame(linhas_tabela)
            
            # Converter colunas de data para o formato correto
            df_carteiras['inicio_validade'] = pd.to_datetime(df_carteiras['inicio_validade'], errors='coerce')
            df_carteiras['fim_validade'] = pd.to_datetime(df_carteiras['fim_validade'], errors='coerce')

            salvar_df_otimizado(df_carteiras, "carteiras_recomendadas_btg", if_exists="replace")
            
            registrar_log('CARTEIRAS_RECOM', 'Sucesso', len(df_carteiras), "Carteiras recomendadas importadas com sucesso")
            return jsonify({"status": "Sucesso", "linhas_salvas": len(df_carteiras)}), 200
            
        else:
            erro_msg = f"Erro BTG: {r.status_code} - {r.text}"
            registrar_log('CARTEIRAS_RECOM', 'Erro', 0, erro_msg)
            return jsonify({"erro": erro_msg}), r.status_code
            
    except Exception as e:
        registrar_log('CARTEIRAS_RECOM', 'Erro', 0, str(e))
        return jsonify({"erro": str(e)}), 500

# 4. ROTAS DE WEBHOOK

@app.route('/webhook/nnm', methods=['POST'])
def webhook_nnm():
    if request.args.get('token') != WEBHOOK_TOKEN: 
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    try:
        url_download = dados.get('response', {}).get('url') or dados.get('url')
        if not url_download: 
            return jsonify({"status": "Recebido sem URL"}), 200

        r = requests.get(url_download)
        r.raise_for_status()
        
        df = pd.read_csv(io.StringIO(r.content.decode('utf-8')), sep=';')
        
        # 1. Padronizacao inicial da data
        df.rename(columns={'dt_captacao': 'data_captacao'}, inplace=True)
        df['data_captacao'] = pd.to_datetime(df['data_captacao'], errors='coerce')
        
        # 2. Definicao das Janelas Moveis
        from datetime import timedelta
        DIAS_FATO = 3
        DIAS_BACKUP = 20
        
        hoje = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        data_corte_fato = hoje - timedelta(days=DIAS_FATO)
        data_corte_backup = hoje - timedelta(days=DIAS_BACKUP)
        
        str_corte_fato = data_corte_fato.strftime('%Y-%m-%d')
        str_corte_backup = data_corte_backup.strftime('%Y-%m-%d')

        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={CONN_STR}")

        # --- 3. TRATAMENTO DO BACKUP RAW (20 DIAS) ---
        df_raw = df[df['data_captacao'] >= data_corte_backup].copy()
        df_raw['data_recebimento_webhook'] = datetime.now()
        
        with engine.begin() as conn:
            try:
                # Remove os ultimos 20 dias para nao duplicar com a carga de hoje
                conn.execute(text(f"DELETE FROM dbo.backup_nnm_raw WHERE data_captacao >= '{str_corte_backup}'"))
            except Exception as e:
                print(f"Aviso Backup NNM: Falha ao deletar (pode ser a primeira execucao). Erro: {e}")
        
        salvar_df_otimizado(df_raw, "backup_nnm_raw", if_exists="append")


        # Filtra a base original apenas para a janela curta do painel (3 dias)
        df_fato = df[df['data_captacao'] >= data_corte_fato].copy()
        
        colunas_tabela = [
            'nr_conta', 'data_captacao', 'ativo', 'mercado', 'cge_officer', 
            'tipo_lancamento', 'descricao', 'qtd', 'captacao', 
            'is_officer_nnm', 'is_partner_nnm', 'is_channel_nnm', 'is_bu_nnm', 
            'submercado', 'submercado_detalhado'
        ]
        
        colunas_presentes = [c for c in colunas_tabela if c in df_fato.columns]
        df_final = df_fato[colunas_presentes].copy()
        
        # Converte booleanos
        colunas_booleanas = ['is_officer_nnm', 'is_partner_nnm', 'is_channel_nnm', 'is_bu_nnm']
        for col in colunas_booleanas:
            if col in df_final.columns:
                df_final[col] = df_final[col].map({'t': 1, 'f': 0, 'True': 1, 'False': 0}).fillna(0).astype(int)

        df_final['data_upload'] = datetime.now()
        
        with engine.begin() as conn:
            try:
                # Remove os ultimos 3 dias da tabela gerencial exposta
                conn.execute(text(f"DELETE FROM dbo.relatorios_nnm_gerencial WHERE data_captacao >= '{str_corte_fato}'"))
            except Exception as e:
                pass
        
        salvar_df_otimizado(df_final, "relatorios_nnm_gerencial", if_exists="append")
            
        msg_sucesso = f"Fato: {DIAS_FATO} dias ({len(df_final)} linhas) | Backup: {DIAS_BACKUP} dias ({len(df_raw)} linhas)"
        print(f"[SUCESSO NNM] {msg_sucesso}")
        registrar_log('NNM', 'Sucesso', len(df_final), f"Importacao concluida. {msg_sucesso}")
        
        return jsonify({"status": "Sucesso", "linhas_fato": len(df_final), "linhas_backup": len(df_raw)}), 200

    except Exception as e:
        print(f"[ERRO CRITICO NNM] Falha no processamento: {str(e)}") 
        registrar_log('NNM', 'Erro', 0, str(e))
        return jsonify({"erro": str(e)}), 500

@app.route('/webhook/basebtg', methods=['POST'])
def webhook_base_btg():
    if request.args.get('token') != WEBHOOK_TOKEN: return jsonify({"erro": "Acesso negado"}), 403

    try:
        dados = request.json
        url_download = dados.get('response', {}).get('url') or dados.get('url')
        if not url_download: return jsonify({"erro": "URL nao encontrada"}), 400

        r = requests.get(url_download)
        r.raise_for_status()
        
        # Adicionado sep=';' e encoding para evitar erro de tokenizacao
        base = pd.read_csv(io.BytesIO(r.content), sep=';', encoding='utf-8')

        # Backup Raw
        base_raw = base.copy()
        base_raw['data_recebimento_webhook'] = datetime.now()
        salvar_df_otimizado(base_raw, "backup_base_btg_raw", if_exists="append")

        # Tratamentos
        base.rename(columns={
            "nm_assessor": "Assessor", "nr_conta": "Conta", "pl_total": "PL Total",
            "nome_completo": "Nome", "faixa_cliente": "Faixa Cliente"
        }, inplace=True)
        
        base['Conta'] = base['Conta'].astype(str)
        base['Assessor'] = base['Assessor'].str.upper()

        faixas_ate_300 = ["Ate 50K", "Entre 50k e 100k", "Entre 100k e 300k"]
        base.loc[base['Faixa Cliente'].isin(faixas_ate_300), "Faixa Cliente"] = "Ate 300k"

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
        base.loc[base['Assessor'].isin(nomes_rodrigo), "Assessor"] = "RODRIGO DE MELLO D'ELIA"

        base.drop_duplicates(subset="Conta", keep='first', inplace=True)

        salvar_df_otimizado(base, "base_btg", col_pk="Conta", if_exists="replace")
        
        df_hist = pd.DataFrame()
        df_hist['Conta'] = base['Conta']
        df_hist['Assessor'] = base['Assessor']
        df_hist['PL Total'] = base['PL Total']
        hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        df_hist['Data'] = hoje
        df_hist['Mês'] = hoje.strftime("%Y-%m")
        
        salvar_df_otimizado(df_hist, "pl_historico_diario", if_exists="append")

        print(f"[SUCESSO BASE_BTG] Base e Historico atualizados. Total: {len(base)}")
        registrar_log('BASE_BTG', 'Sucesso', len(base), "Base e Historico atualizados")
        return jsonify({"status": "Sucesso", "total": len(base)}), 200

    except Exception as e:
        print(f"[ERRO CRITICO BASE_BTG] Falha no processamento: {str(e)}")
        registrar_log('BASE_BTG', 'Erro', 0, str(e))
        return jsonify({"erro": str(e)}), 500

@app.route('/webhook/performance', methods=['POST'])
def webhook_performance():
    token_recebido = request.args.get('token')
    if token_recebido != WEBHOOK_TOKEN:
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    if not dados:
        return jsonify({"erro": "Payload vazio"}), 400

    try:
        res = dados.get('response') or dados.get('partnerResponse') or {}
        url_download = res.get('url') if isinstance(res, dict) else None
        data_ref = res.get('endDate') if isinstance(res, dict) else None
        req_id = dados.get('idPartnerRequest', 'N/A')
        
        conta_raw = res.get('accountNumber') or dados.get('cge') or dados.get('accountNumber')
        conta_id = str(conta_raw) if (conta_raw and str(conta_raw).lower() != 'null') else "Desconhecida"

        if not url_download:
            print(f"[AVISO PERFORMANCE] URL nao encontrada. Conta: {conta_id} | ID: {req_id}")
            return jsonify({"erro": "URL nao encontrada", "conta": conta_id}), 400

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
        
        # Log de Sucesso exclusivo no console do Render (evita spam na tabela de logs)
        print(f"[SUCESSO PERFORMANCE] Conta: {conta_id} | Salvos: {arquivos_salvos} | Ref: {data_ref} | ID: {req_id}")
        
        return jsonify({"status": "Processado", "conta": conta_id}), 200

    except Exception as e:
        conta_falha = conta_id if 'conta_id' in locals() else 'N/A'
        print(f"[ERRO CRITICO PERFORMANCE] Conta: {conta_falha} | Erro: {str(e)[:100]}")
        return jsonify({"erro": str(e)}), 500

@app.route('/webhook/custodia', methods=['POST'])
def webhook_custodia():
    if request.args.get('token') != WEBHOOK_TOKEN: 
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.json
    try:
        res = dados.get('response', {})
        url_download = res.get('url')
        
        if not url_download: 
            return jsonify({"status": "Recebido sem URL"}), 200

        r = requests.get(url_download)
        r.raise_for_status()
        
        # 1. Abre o arquivo ZIP baixado em memoria
        import zipfile
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            # 2. Pega o nome do arquivo CSV que esta la dentro
            nome_arquivo_csv = z.namelist()[0]
            print(f"[DEBUG CUSTODIA] Lendo arquivo descompactado: {nome_arquivo_csv}", flush=True)
            
            # 3. Abre o CSV interno e passa para o Pandas ler
            with z.open(nome_arquivo_csv) as f:
                df = pd.read_csv(f, sep=',', encoding='latin1', low_memory=False)

        # Backup Raw
        df_raw = df.copy()
        df_raw['data_recebimento_webhook'] = datetime.now()
        salvar_df_otimizado(df_raw, "backup_custodia_raw", if_exists="replace")
        
        # Tratamento das datas
        df_final = df.copy()
        colunas_de_data = ['referenceDate', 'dataInicio', 'fixingDate', 'dataKnockIn']
        for col in colunas_de_data:
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(df_final[col], format='%d/%m/%Y', errors='coerce')

        # Salva na tabela principal
        df_final['data_upload'] = datetime.now()
        salvar_df_otimizado(df_final, "relatorios_custodia", if_exists="replace")
            
        print(f"[SUCESSO CUSTODIA] Importacao concluida. Linhas: {len(df_final)}", flush=True)
        registrar_log('CUSTODIA', 'Sucesso', len(df_final), "Importacao Custodia concluida")
        
        return jsonify({"status": "Sucesso", "linhas": len(df_final)}), 200

    except Exception as e:
        print(f"[ERRO CRITICO CUSTODIA] Falha: {str(e)}", flush=True) 
        registrar_log('CUSTODIA', 'Erro', 0, str(e))
        return jsonify({"erro": str(e)}), 500


# 5. UTILITARIOS

@app.route('/meu-ip', methods=['GET'])
def get_ip():
    try:
        return jsonify({'ip_render': requests.get('https://api.ipify.org').text})
    except:
        return jsonify({'erro': 'falha ao obter ip'}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)