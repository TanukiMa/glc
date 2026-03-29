#!/usr/bin/env python3
# test_sabermetrics.py
# Unit tests for the sabermetrics module

import unittest
from sabermetrics import EraAdjustedStats, demonstrate_era_adjustment


class TestEraAdjustedStats(unittest.TestCase):
    """Test cases for the EraAdjustedStats class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.stats = EraAdjustedStats(league_avg_ba=0.260)
    
    def test_ba_plus_league_average(self):
        """Test that league average player has BA+ of 100."""
        ba_plus = self.stats.calculate_ba_plus(0.260, 0.260)
        self.assertAlmostEqual(ba_plus, 100.0, places=1)
    
    def test_ba_plus_above_average(self):
        """Test that above-average player has BA+ > 100."""
        ba_plus = self.stats.calculate_ba_plus(0.300, 0.260)
        self.assertGreater(ba_plus, 100.0)
        self.assertAlmostEqual(ba_plus, 115.38, places=1)
    
    def test_ba_plus_below_average(self):
        """Test that below-average player has BA+ < 100."""
        ba_plus = self.stats.calculate_ba_plus(0.220, 0.260)
        self.assertLess(ba_plus, 100.0)
        self.assertAlmostEqual(ba_plus, 84.62, places=1)
    
    def test_ba_plus_zero_league_average(self):
        """Test that zero league average raises ValueError."""
        with self.assertRaises(ValueError):
            self.stats.calculate_ba_plus(0.280, 0.0)
    
    def test_compare_eras_same_ba_different_value(self):
        """
        Test the core concept: same BA has different values in different eras.
        This is the key point from the problem statement.
        """
        player_ba = 0.280
        high_offense_avg = 0.275  # 打高投低
        low_offense_avg = 0.245   # 投高打低
        
        comparison = self.stats.compare_eras(
            player_ba, 
            high_offense_avg, 
            low_offense_avg
        )
        
        # BA+ should be higher in the low-offense era
        self.assertGreater(
            comparison['low_offense_era']['ba_plus'],
            comparison['high_offense_era']['ba_plus']
        )
        
        # Check that the difference is significant
        self.assertGreater(comparison['difference'], 10.0)
        
        # Verify basic structure
        self.assertEqual(comparison['player_ba'], player_ba)
        self.assertIn('ba_plus', comparison['high_offense_era'])
        self.assertIn('ba_plus', comparison['low_offense_era'])
    
    def test_compare_eras_specific_values(self):
        """Test specific BA+ values for known scenarios."""
        player_ba = 0.280
        
        # Scenario 1: High offense era (league avg .275)
        ba_plus_high = self.stats.calculate_ba_plus(player_ba, 0.275)
        self.assertAlmostEqual(ba_plus_high, 101.82, places=1)
        
        # Scenario 2: Low offense era (league avg .245)
        ba_plus_low = self.stats.calculate_ba_plus(player_ba, 0.245)
        self.assertAlmostEqual(ba_plus_low, 114.29, places=1)
        
        # The difference should be about 12-13 points
        difference = ba_plus_low - ba_plus_high
        self.assertAlmostEqual(difference, 12.47, places=1)
    
    def test_normalize_stat_generic(self):
        """Test the generic stat normalization function."""
        # Test with batting average
        normalized = self.stats.normalize_stat(0.280, 0.260)
        self.assertAlmostEqual(normalized, 107.69, places=1)
        
        # Test with another stat (e.g., OPS)
        normalized_ops = self.stats.normalize_stat(0.850, 0.750)
        self.assertAlmostEqual(normalized_ops, 113.33, places=1)
    
    def test_normalize_stat_zero_league_average(self):
        """Test that zero league average raises ValueError."""
        with self.assertRaises(ValueError):
            self.stats.normalize_stat(0.300, 0.0)
    
    def test_pitcher_era_more_valuable(self):
        """
        Test that in a pitcher's era (投高打低), the same BA is more valuable.
        This directly addresses the problem statement.
        """
        player_ba = 0.280
        
        # Pitcher's era: league average is low
        pitchers_era_avg = 0.240
        
        # Hitter's era: league average is high
        hitters_era_avg = 0.270
        
        ba_plus_pitchers = self.stats.calculate_ba_plus(player_ba, pitchers_era_avg)
        ba_plus_hitters = self.stats.calculate_ba_plus(player_ba, hitters_era_avg)
        
        # BA+ should be significantly higher in pitcher's era
        self.assertGreater(ba_plus_pitchers, ba_plus_hitters)
        
        # Should be at least 10 points different
        self.assertGreater(ba_plus_pitchers - ba_plus_hitters, 10.0)
    
    def test_real_world_scenario_1968(self):
        """
        Test real-world scenario: 1968 "Year of the Pitcher" in MLB.
        League average BA was .237 (extremely low).
        """
        player_ba = 0.280
        league_avg_1968 = 0.237  # Historically accurate
        
        ba_plus = self.stats.calculate_ba_plus(player_ba, league_avg_1968)
        
        # BA+ should be very high (around 118)
        self.assertGreater(ba_plus, 115.0)
        self.assertAlmostEqual(ba_plus, 118.14, places=1)
    
    def test_real_world_scenario_2000(self):
        """
        Test real-world scenario: 2000 "Year of the Hitter" in MLB.
        League average BA was .270 (very high).
        """
        player_ba = 0.280
        league_avg_2000 = 0.270  # Historically accurate
        
        ba_plus = self.stats.calculate_ba_plus(player_ba, league_avg_2000)
        
        # BA+ should be closer to 100 (around 103-104)
        self.assertLess(ba_plus, 110.0)
        self.assertAlmostEqual(ba_plus, 103.70, places=1)
    
    def test_comparison_shows_not_equal(self):
        """
        Test that demonstrates the answer to the problem statement:
        "A .280 BA in a pitcher's year and a .280 BA in a hitter's year are NOT the same."
        """
        player_ba = 0.280
        high_offense_avg = 0.275
        low_offense_avg = 0.245
        
        comparison = self.stats.compare_eras(
            player_ba,
            high_offense_avg,
            low_offense_avg
        )
        
        # The BA+ values should NOT be equal
        self.assertNotEqual(
            comparison['high_offense_era']['ba_plus'],
            comparison['low_offense_era']['ba_plus']
        )
        
        # There should be a meaningful conclusion about the difference
        self.assertIn('conclusion', comparison)
        self.assertIn('difference', comparison)
        self.assertGreater(comparison['difference'], 0.0)


class TestDemonstrateFunction(unittest.TestCase):
    """Test the demonstrate_era_adjustment function."""
    
    def test_demonstrate_returns_valid_comparison(self):
        """Test that the demonstration function returns valid data."""
        # Should not raise an exception
        comparison = demonstrate_era_adjustment()
        
        # Should return a dictionary with expected keys
        self.assertIsInstance(comparison, dict)
        self.assertIn('player_ba', comparison)
        self.assertIn('high_offense_era', comparison)
        self.assertIn('low_offense_era', comparison)
        self.assertIn('difference', comparison)
        self.assertIn('conclusion', comparison)


if __name__ == '__main__':
    unittest.main()
