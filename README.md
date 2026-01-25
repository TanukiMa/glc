# glc

## セイバーメトリクス機能 (Sabermetrics Feature)

このリポジトリには、セイバーメトリクスにおける時代調整統計を計算するためのモジュールが含まれています。

### 概要

野球のセイバーメトリクスにおいて、生の統計（打率など）は時代背景を考慮して評価する必要があります。
**打高投低の年の打率0.280と投高打低の年の打率0.280は同じではありません。**

例えば：
- 投高打低の年（1968年のMLBなど）では、リーグ平均打率が.237と非常に低かった
- 打高投低の年（2000年のMLBなど）では、リーグ平均打率が.270と高かった
- 同じ.280の打率でも、前者の方が価値が高い

### 使用方法

#### 基本的な例

```python
from sabermetrics import EraAdjustedStats

stats = EraAdjustedStats()

# BA+（Batting Average Plus）を計算
# 100 = リーグ平均、100より大きい = 平均以上、100より小さい = 平均以下
player_ba = 0.280
league_avg = 0.260
ba_plus = stats.calculate_ba_plus(player_ba, league_avg)
print(f"BA+: {ba_plus:.1f}")  # BA+: 107.7
```

#### 時代間の比較

```python
# 同じ打率を異なる時代で比較
comparison = stats.compare_eras(
    player_ba=0.280,
    high_offense_league_avg=0.275,  # 打高投低の年
    low_offense_league_avg=0.245     # 投高打低の年
)

print(f"打高投低の年のBA+: {comparison['high_offense_era']['ba_plus']:.1f}")
print(f"投高打低の年のBA+: {comparison['low_offense_era']['ba_plus']:.1f}")
print(f"差: {comparison['difference']:.1f}ポイント")
```

#### デモンストレーション

```bash
# デモスクリプトを実行
python3 sabermetrics_example.py

# または、メインモジュールを直接実行
python3 sabermetrics.py
```

### テスト

```bash
python3 test_sabermetrics.py -v
```

### ファイル

- `sabermetrics.py` - コアモジュール（EraAdjustedStatsクラスと関数を含む）
- `sabermetrics_example.py` - 使用例とデモンストレーション
- `test_sabermetrics.py` - ユニットテスト

## Web Scraping Monitoring (Original GLC功能)

このリポジトリのオリジナル機能は、Webページの更新を監視し、変更があればデータベースに記録するツールです。
