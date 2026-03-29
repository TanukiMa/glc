# セイバーメトリクスの実装: 時代調整打率

## 問題提起

> セイバーメトリクスにおいて、打高投低の年の打率0.280と投高打低の年の打率0.280は同じでは無いですよね？

## 回答

**正解です。同じではありません。**

打率0.280という同じ数値でも、リーグの打撃環境によってその価値は大きく異なります。

## 実装内容

このリポジトリでは、この問題を解決するために以下を実装しました：

### 1. BA+（Batting Average Plus）の計算

BA+は、選手の打率をリーグ平均で正規化した指標です：

```
BA+ = (選手の打率 / リーグ平均打率) × 100
```

- BA+ = 100: リーグ平均
- BA+ > 100: リーグ平均以上
- BA+ < 100: リーグ平均以下

### 2. 具体例

#### 投高打低の年（1968年MLBの例）
- リーグ平均打率: .237
- 選手の打率: .280
- **BA+: 118.1** ← 非常に優秀

#### 打高投低の年（2000年MLBの例）
- リーグ平均打率: .270
- 選手の打率: .280
- **BA+: 103.7** ← まあまあ

#### 差異
同じ打率.280でも、**BA+では14.4ポイントの差**があります。

## 使用方法

```python
from sabermetrics import EraAdjustedStats

stats = EraAdjustedStats()

# 投高打低の年（リーグ平均.245）
ba_plus_low_offense = stats.calculate_ba_plus(0.280, 0.245)
print(f"投高打低の年: BA+ {ba_plus_low_offense:.1f}")  # 114.3

# 打高投低の年（リーグ平均.275）
ba_plus_high_offense = stats.calculate_ba_plus(0.280, 0.275)
print(f"打高投低の年: BA+ {ba_plus_high_offense:.1f}")  # 101.8

print(f"差: {ba_plus_low_offense - ba_plus_high_offense:.1f}ポイント")  # 12.5ポイント
```

## デモンストレーション

```bash
# 基本的なデモ
python3 sabermetrics.py

# 詳細な例
python3 sabermetrics_example.py

# テストの実行
python3 test_sabermetrics.py
```

## 結論

セイバーメトリクスにおいて、生の打率は時代背景を考慮して評価する必要があります。
BA+のような正規化された指標を使用することで、異なる時代の選手を公平に比較できます。

**打高投低の年の打率0.280と投高打低の年の打率0.280は、確かに同じではありません。**
