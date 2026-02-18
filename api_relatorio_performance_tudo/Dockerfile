# Usamos a versao "bullseye" (Debian 11) para garantir compatibilidade com o driver da Microsoft
FROM python:3.9-slim-bullseye

WORKDIR /app

# 1. Instala os utilitários básicos (curl e gnupg)
RUN apt-get update && apt-get install -y curl gnupg2 unixodbc-dev

# 2. Adiciona a chave de segurança da Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

# 3. Adiciona o repositório oficial do SQL Server (Debian 11)
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

# 4. Atualiza a lista e instala o Driver ODBC 18
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18

# 5. Instala as dependências do Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copia o código (se certifique que o nome do arquivo aqui bate com o seu CMD abaixo)
COPY . .

# 7. Configura o servidor
EXPOSE 10000

# IMPORTANTE: Confira se o nome do arquivo antes de :app é o seu arquivo real
CMD ["gunicorn", "--bind", "0.0.0.0:10000", "--workers", "2", "--timeout", "600", "app_relatorio_performance:app"]