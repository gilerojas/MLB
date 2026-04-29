"""Live in-game event detection for Mallitalytics MLB.

Data source is the MLB Stats API (`statsapi.mlb.com`), which is near real-time
(~10-30s behind live). Baseball Savant parquets land after the game and are not
usable for live detection.
"""
