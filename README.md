# PaaS型未来実験室 (PaaS-based Future Laboratory)

**教育・研究活動を支援する、即時構築可能なデータ分析プラットフォーム**

2025年度 中部大学 工学部 情報工学科 卒業研究成果物
(作成者: 豊島 蓮 / 指導教員: 木村 秀明 教授)

## 📖 概要 (Overview)

「PaaS型未来実験室」は、ITインフラの専門知識を持たない学生や研究者が、Webブラウザ上の操作だけで即座に**自分専用のデータ分析環境（JupyterLab + PostgreSQL）**を構築できるプラットフォームです。

Kubernetesを基盤とし、Go言語で実装された**独自のコントローラー**が、ユーザー管理・リソース分離・自己修復を自律的に行います。また、研究データの**「環境スナップショット（DBのホットバックアップ）」**機能を備え、研究成果の引継ぎや再現性の確保を強力に支援します。

### ✨ 主な機能

* **ワンクリック環境構築**: ログイン後、ボタン一つでJupyterLabとDBが立ち上がります。
* **マルチテナント**: K8s NamespaceとNetworkPolicyによる安全なユーザー分離。
* **自己修復機能**: 誤ってリソースを削除しても、30秒以内に自動復旧します。
* **環境スナップショット**: 稼働中のDBとファイルを丸ごと保存し、他者へ共有・復元が可能。

## 🛠 技術スタック (Tech Stack)

* **Backend / Controller**: Go (using `client-go`)
* **Frontend**: React (HTML/JS)
* **Infrastructure**: Kubernetes, Docker
* **Database**: PostgreSQL (User/Meta data & User's Analysis DB)
* **Storage**: MinIO (S3 Compatible Object Storage)
* **Auth**: Keycloak (OIDC)

## 🚀 初回セットアップ (Setup)

このシステムを初めて動かすPC（Windows + WSL2）で行う設定です。

### 1. ファイアウォールの開放
管理者権限の PowerShell で以下を実行し、各ポートへの通信を許可します。

```powershell
New-NetFirewallRule -DisplayName "Future Lab Ports" -Direction Inbound -LocalPort 8000,8090,8080 -Protocol TCP -Action Allow
```

### **2\. NodePort範囲の許可**

K8sが払い出すポート（30000番台）も許可しておきます。

```
New-NetFirewallRule \-DisplayName "K8s NodePorts" \-Direction Inbound \-LocalPort 30000-32767 \-Protocol TCP \-Action Allow
```

### **3\. Keycloakの設定**

ブラウザで http://localhost:8080 (Keycloak管理画面) にアクセスし、クライアント future-lab-frontend の設定を変更します。

* **Valid Redirect URIs**: http://\<WindowsのIP\>:8000/\* を追加  
* **Web Origins**: http://\<WindowsのIP\>:8000 を追加

## **▶️ 運用マニュアル (Daily Operation)**

PCを再起動した際や、日々の実験開始時に行う手順です。

**WSL2のIPアドレスは再起動ごとに変わる**ため、毎回確認が必要です。

### **1\. IPアドレスの確認**

* **Windows側 (スマホからのアクセス先)**  
  PowerShellで確認:
  ```
  ipconfig  
  \# \-\> IPv4 アドレス をメモ (例: 192.168.0.103)
  ```

  ※以降、これを \<WindowsIP\> と呼びます。  
* **Ubuntu側 (転送先)**  
  Ubuntuターミナルで確認:
  ```
  hostname \-I  
  \# \-\> 先頭のIPをメモ (例: 172.25.x.x)
  ```

  ※以降、これを \<UbuntuIP\> と呼びます。

### **2\. 転送設定の更新 (Windows側)**

管理者権限の PowerShell で以下を実行し、WindowsへのアクセスをUbuntuへ転送します。

\<UbuntuIP\> は手順1で確認したものに書き換えてください。

```
\# 古い設定の確認  
netsh interface portproxy show all

\# 設定の更新 (上書き登録)  
netsh interface portproxy add v4tov4 listenport=8000 connectport=8000 connectaddress=\<UbuntuIP\>  
netsh interface portproxy add v4tov4 listenport=8090 connectport=8090 connectaddress=\<UbuntuIP\>  
netsh interface portproxy add v4tov4 listenport=8080 connectport=8080 connectaddress=\<UbuntuIP\>
```

### **3\. サーバーの起動 (Ubuntu側)**

エラーを防ぐため、必ずIPアドレスを明示して起動します。

**バックエンド (main.go):**
```
\# リポジトリのクローン  
git clone https://github.com/baumu373/Future-Lab_PaaS_2025.git)  
cd Future-Lab_PaaS_2025/future-lab-backend

\# 環境変数の設定 (\<WindowsIP\> を書き換える)  
export KEYCLOAK\_URL="http://\<WindowsIP\>:8080"  
export EXTERNAL\_IP="\<WindowsIP\>"

\# MinIOポートの取得 (重要: K8s上のポートを動的に取得)  
export MINIO\_PORT=$(kubectl get svc public-minio-service \-o jsonpath='{.spec.ports\[0\].nodePort}')  
export MINIO\_ENDPOINT="${EXTERNAL\_IP}:${MINIO\_PORT}"

\# 起動  
go run main.go
```

**フロントエンド:**
```
cd ../future-lab-frontend  
python3 \-m http.server 8000
```

### **4\. ユーザーサービスの公開 (Jupyter等)**

実験を開始し、JupyterLabなどが立ち上がったら、そのポートも転送設定に追加します。

1. **ポート確認**: kubectl get svc またはスマホ画面でポート番号を確認 (例: 31567\)  
2. **転送追加 (Windows PowerShell)**:

   ```
   netsh interface portproxy add v4tov4 listenport=31567 connectport=31567 connectaddress=\<UbuntuIP\>
   ```

これで、スマホから http://\<WindowsIP\>:31567 でJupyterLabにアクセスできます。

## **⚠️ トラブルシューティング**

### **Q. "token issued by a different provider" エラーが出る**

* **原因**: main.go が認識しているKeycloakのURL (localhost) と、実際にアクセスしたURL (IPアドレス) が一致していないため。  
* **解決策**: 起動時に export KEYCLOAK\_URL="http://\<WindowsIP\>:8080" を正しく設定し、バックエンドを再起動してください。

### **Q. スマホから繋がらない**

* **確認1**: スマホとPCは同じWi-Fiに繋がっていますか？  
* **確認2**: Windowsファイアウォールでポート (8000, 8090, 8080\) が許可されていますか？  
* **確認3**: netsh interface portproxy show all で、\<UbuntuIP\> が現在の正しいUbuntuのIPになっていますか？ (WSL再起動で変わることがあります)

## **📚 ドキュメント**

* 👉 [**引継ぎ資料 & 研究テーマ案 (handover\_material.md)**](https://www.google.com/search?q=./handover_material.md)  
  * 開発・運用時の詳細なハマりポイント  
  * 残された課題

## **📜 ライセンス**

This project is licensed under the MIT License \- see the [LICENSE](https://www.google.com/search?q=LICENSE) file for details.
