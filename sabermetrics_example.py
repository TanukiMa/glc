#!/usr/bin/env python3
# sabermetrics_example.py
# Example script demonstrating era-adjusted batting statistics

from sabermetrics import EraAdjustedStats, demonstrate_era_adjustment


def main():
    """
    Main function demonstrating various scenarios of era-adjusted statistics.
    """
    
    # Run the main demonstration
    print("=" * 70)
    print("セイバーメトリクスの例: 打率の時代調整")
    print("Era-Adjusted Batting Statistics Example")
    print("=" * 70)
    print()
    
    # Basic demonstration
    demonstrate_era_adjustment()
    
    print("\n\n")
    print("=" * 70)
    print("追加の例: 複数の選手の比較")
    print("Additional Example: Comparing Multiple Players")
    print("=" * 70)
    print()
    
    stats = EraAdjustedStats()
    
    # Example scenarios
    scenarios = [
        {
            'year': '1968年 (投高打低)',
            'player': '選手A',
            'ba': 0.280,
            'league_avg': 0.237,
            'context': 'ピッチャー有利の年'
        },
        {
            'year': '2000年 (打高投低)',
            'player': '選手B',
            'ba': 0.280,
            'league_avg': 0.270,
            'context': 'バッター有利の年'
        },
        {
            'year': '2023年 (通常)',
            'player': '選手C',
            'ba': 0.280,
            'league_avg': 0.248,
            'context': '通常の年'
        }
    ]
    
    print("同じ打率0.280を持つ3人の選手の時代調整後の評価:\n")
    
    for scenario in scenarios:
        ba_plus = stats.calculate_ba_plus(scenario['ba'], scenario['league_avg'])
        print(f"{scenario['year']} - {scenario['player']}")
        print(f"  打率: {scenario['ba']:.3f}")
        print(f"  リーグ平均: {scenario['league_avg']:.3f}")
        print(f"  BA+: {ba_plus:.1f}")
        print(f"  状況: {scenario['context']}")
        print()
    
    print("結論:")
    print("  選手Aの打率0.280（投高打低の1968年）は BA+ 118.1")
    print("  選手Bの打率0.280（打高投低の2000年）は BA+ 103.7")
    print("  選手Cの打率0.280（通常の2023年）は BA+ 112.9")
    print()
    print("  → 同じ打率でも、時代によって15ポイント近くの差が生じる")
    print("  → セイバーメトリクスでは、時代調整が不可欠である")
    print("=" * 70)
    
    print("\n\n")
    print("=" * 70)
    print("実際の使用例: NPBの歴史的データ")
    print("Real Example: Historical NPB Data")
    print("=" * 70)
    print()
    
    # Historical NPB examples
    npb_examples = [
        {
            'year': 1980,
            'league': 'セントラル・リーグ',
            'league_avg': 0.262,
            'note': '比較的高打率の時代'
        },
        {
            'year': 2005,
            'league': 'パシフィック・リーグ',
            'league_avg': 0.265,
            'note': '打高投低の時代'
        },
        {
            'year': 2019,
            'league': 'NPB全体',
            'league_avg': 0.248,
            'note': '投高打低の時代（飛ばないボール）'
        }
    ]
    
    test_batting_average = 0.300
    
    print(f"仮定: 打率{test_batting_average:.3f}を記録した選手の時代調整評価\n")
    
    for example in npb_examples:
        ba_plus = stats.calculate_ba_plus(test_batting_average, example['league_avg'])
        print(f"{example['year']}年 {example['league']}")
        print(f"  リーグ平均: {example['league_avg']:.3f}")
        print(f"  BA+: {ba_plus:.1f}")
        print(f"  備考: {example['note']}")
        print()
    
    print("このように、同じ打率でも時代背景を考慮することで、")
    print("より公平な選手評価が可能になります。")
    print("=" * 70)


if __name__ == "__main__":
    main()
