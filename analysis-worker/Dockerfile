# Python 3.9 の軽量イメージをベースにする
FROM python:3.9-slim

# 作業ディレクトリを設定
WORKDIR /app

# 必要なライブラリのリストをコンテナにコピー
COPY requirements.txt .

# ライブラリをインストール
RUN pip install --no-cache-dir -r requirements.txt

# 分析スクリプトをコンテナにコピー
COPY main.py .

# コンテナ実行時のデフォルトコマンドを設定
ENTRYPOINT ["python", "main.py"]