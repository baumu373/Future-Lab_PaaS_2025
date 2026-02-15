# ベースとなるOSイメージを指定
FROM ubuntu:22.04

# 環境変数を設定（apt-get実行時にプロンプトで止まらないようにするおまじない）
ENV DEBIAN_FRONTEND=noninteractive

# OSを最新の状態にし、分析でよく使うツールをインストール
RUN apt-get update && apt-get install -y \
    python3 \
    pip \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# ログメッセージ
RUN echo "PythonとPostgreSQLクライアントを含むカスタムイメージへようこそ！"

# コンテナが起動したときにデフォルトで実行されるコマンド
# この設定により、コンテナは起動し続けます（すぐに終了しない）
CMD ["tail", "-f", "/dev/null"]
