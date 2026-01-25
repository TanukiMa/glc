#!/usr/bin/env python3
# sabermetrics.py
# Module for calculating era-adjusted batting statistics
# Addresses the concept that a .280 batting average in different eras has different values

# Small epsilon value for floating-point comparisons
EPSILON = 1e-10

class EraAdjustedStats:
    """
    Class to calculate era-adjusted batting statistics.
    
    In sabermetrics, raw statistics like batting average need to be adjusted
    for the league environment. A .280 batting average in a pitcher-dominant
    era (投高打低) is more impressive than .280 in a hitter-dominant era (打高投低).
    """
    
    def __init__(self, league_avg_ba=0.260):
        """
        Initialize with league average batting average.
        
        Args:
            league_avg_ba (float): The league average batting average for normalization
        """
        self.league_avg_ba = league_avg_ba
    
    def calculate_ba_plus(self, player_ba, league_avg_ba=None):
        """
        Calculate BA+ (Batting Average Plus), similar to OPS+ or ERA+.
        
        BA+ is normalized to 100, where:
        - 100 = league average
        - > 100 = better than league average
        - < 100 = worse than league average
        
        Formula: BA+ = (player_ba / league_avg_ba) * 100
        
        Args:
            player_ba (float): Player's batting average
            league_avg_ba (float): League average batting average for that year
        
        Returns:
            float: BA+ value
        """
        if league_avg_ba is None:
            league_avg_ba = self.league_avg_ba
        
        if abs(league_avg_ba) < EPSILON:
            raise ValueError("League average batting average cannot be 0")
        
        return (player_ba / league_avg_ba) * 100
    
    def compare_eras(self, player_ba, high_offense_league_avg, low_offense_league_avg):
        """
        Compare the same batting average in different league environments.
        
        This demonstrates that a .280 batting average in a pitcher's era
        is more valuable than .280 in a hitter's era.
        
        Args:
            player_ba (float): Player's batting average (e.g., 0.280)
            high_offense_league_avg (float): League average in high-offense era (打高投低)
            low_offense_league_avg (float): League average in low-offense era (投高打低)
        
        Returns:
            dict: Comparison results with BA+ for both eras
        """
        high_offense_ba_plus = self.calculate_ba_plus(player_ba, high_offense_league_avg)
        low_offense_ba_plus = self.calculate_ba_plus(player_ba, low_offense_league_avg)
        
        return {
            'player_ba': player_ba,
            'high_offense_era': {
                'league_avg': high_offense_league_avg,
                'ba_plus': high_offense_ba_plus,
                'interpretation': '打高投低の年' if high_offense_league_avg > low_offense_league_avg else '投高打低の年'
            },
            'low_offense_era': {
                'league_avg': low_offense_league_avg,
                'ba_plus': low_offense_ba_plus,
                'interpretation': '投高打低の年' if low_offense_league_avg < high_offense_league_avg else '打高投低の年'
            },
            'difference': abs(high_offense_ba_plus - low_offense_ba_plus),
            'conclusion': f'同じ打率{player_ba:.3f}でも、BA+では{abs(high_offense_ba_plus - low_offense_ba_plus):.1f}ポイントの差がある'
        }
    
    def normalize_stat(self, player_stat, league_avg_stat):
        """
        Generic function to normalize any stat to league average.
        
        Args:
            player_stat (float): Player's statistic
            league_avg_stat (float): League average for that statistic
        
        Returns:
            float: Normalized stat (Plus statistic)
        """
        if abs(league_avg_stat) < EPSILON:
            raise ValueError("League average stat cannot be 0")
        
        return (player_stat / league_avg_stat) * 100


def demonstrate_era_adjustment():
    """
    Demonstration function showing how the same batting average
    has different values in different eras.
    """
    stats = EraAdjustedStats()
    
    # Example: .280 batting average in two different eras
    player_ba = 0.280
    
    # High offense era (打高投低): league average is .275
    high_offense_league_avg = 0.275
    
    # Low offense era (投高打低): league average is .245
    low_offense_league_avg = 0.245
    
    comparison = stats.compare_eras(player_ba, high_offense_league_avg, low_offense_league_avg)
    
    print("=" * 70)
    print("セイバーメトリクスにおける打率の時代調整")
    print("=" * 70)
    print(f"\n選手の打率: {comparison['player_ba']:.3f}\n")
    
    print(f"【{comparison['high_offense_era']['interpretation']}】")
    print(f"  リーグ平均打率: {comparison['high_offense_era']['league_avg']:.3f}")
    print(f"  BA+: {comparison['high_offense_era']['ba_plus']:.1f}")
    print()
    
    print(f"【{comparison['low_offense_era']['interpretation']}】")
    print(f"  リーグ平均打率: {comparison['low_offense_era']['league_avg']:.3f}")
    print(f"  BA+: {comparison['low_offense_era']['ba_plus']:.1f}")
    print()
    
    print(f"結論: {comparison['conclusion']}")
    print()
    print("解説: 投高打低の年の打率0.280は、BA+で{:.1f}となり、".format(
        comparison['low_offense_era']['ba_plus']))
    print("      打高投低の年の打率0.280（BA+ {:.1f}）よりも".format(
        comparison['high_offense_era']['ba_plus']))
    print("      {:.1f}ポイント高く評価されます。".format(comparison['difference']))
    print("\n同じ打率0.280でも、時代背景によって価値が異なることが分かります。")
    print("=" * 70)
    
    return comparison


if __name__ == "__main__":
    demonstrate_era_adjustment()
