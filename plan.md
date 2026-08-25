crypto-lakehouse-prod/
├── bronze/
│   ├── binance/trades/year=2026/month=08/day=20/
│   ├── fred/rates/
│   └── sentiment/news/
├── silver/
│   ├── trades_cleaned/
│   ├── macro_indicators/
│   └── sentiment_scored/
├── gold/
│   ├── vwap_5min/
│   ├── volatility/
│   ├── support_resistance/
│   └── llm_training_pairs/
├── checkpoints/          # Spark streaming checkpoints
└── ml_models/            # LLaMA 3.1 fine-tuned artifacts