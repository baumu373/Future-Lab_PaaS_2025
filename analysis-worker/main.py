import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest

def main():
    if len(sys.argv) != 3:
        print("Usage: python main.py <input_csv_path> <output_image_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    if not os.path.exists(input_path):
        print(f"Error: Input file not found at {input_path}")
        sys.exit(1)

    print(f"Reading data from {input_path}...")
    try:
        # CSVファイルを読み込む
        df = pd.read_csv(input_path, index_col=0, header=0, parse_dates=True)
        series_to_plot = df.iloc[:, 0]

        # --- ▼▼▼ ここからが異常検知のロジック ▼▼▼ ---
        print("Performing anomaly detection...")
        
        # Isolation Forestモデルを準備
        # contamination='auto' は、アルゴリズムがデータに基づいて異常値の割合を自動で推定する設定
        model = IsolationForest(contamination='auto', random_state=42)
        
        # モデルを学習させ、各データ点が異常(-1)か正常(1)かを予測
        # series_to_plot.values.reshape(-1, 1) は、データをモデルが扱える形式に変換しています
        predictions = model.fit_predict(series_to_plot.values.reshape(-1, 1))
        
        # 予測結果をデータフレームに追加
        df['anomaly'] = predictions
        
        # 異常と判定されたデータ点のみを抽出
        anomalies = df[df['anomaly'] == -1]
        print(f"Found {len(anomalies)} anomalies.")
        # --- ▲▲▲ ここまでが異常検知のロジック ▲▲▲ ---

        print("Generating plot with anomalies...")
        # グラフの作成
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # 1. 元々の時系列データをプロット
        series_to_plot.plot(ax=ax, label='Measured Data', color='darkblue')
        
        # 2. 異常検知されたデータ点を赤色の点でプロット
        ax.scatter(anomalies.index, anomalies.iloc[:, 0], color='red', label='Anomaly Detected', s=50, zorder=5)
        
        # グラフの装飾
        ax.set_title('Time Series Anomaly Detection', fontsize=16)
        ax.set_xlabel('Time', fontsize=12)
        ax.set_ylabel(series_to_plot.name, fontsize=12)
        ax.grid(True, linestyle='--', alpha=0.6)
        ax.legend()
        plt.tight_layout()

        # グラフを画像ファイルとして保存
        plt.savefig(output_path)
        print(f"Plot saved to {output_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

