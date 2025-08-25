from enum import Enum


class Settings(Enum):
    NEWS_MIN_DURATION = "NewsMinDuration"
    NEWS_MAX_DURATION = "NewsMaxDuration"
    STOCK_MAX_PERCENT_LOSS = "StockMaxPercentLoss"
    STOCK_PURCHASE_MIN_INCREASE = "StockPurchaseMinIncrease"
    STOCK_PURCHASE_MAX_INCREASE = "StockPurchaseMaxIncrease"
    STOCK_NO_PURCHASE_MIN_LOSS = "StockNoPurchaseMinLoss"
    STOCK_NO_PURCHASE_MAX_LOSS = "StockNoPurchaseMaxLoss"
    STOCK_NO_PURCHASE_LOSS_TIME = "StockNoPurchaseLossTime"
    MARKET_CRASH_LOSS = "MarketCrashLoss"


class StockPriceChangeReason(Enum):
    NO_PURCHASE = 'No purchase'
    PURCHASE = 'Purchase'
    EVENT = 'Event'
