commands


rpk group seek governance-layer --to start



python -m src.governance.consumer
rpk topic consume agent-safe-events --num 2



### audit test
# Terminal 1
rpk topic consume audit-log --offset start

# Terminal 2
python -m src.producer.producer

# Terminal 3
python -m src.governance.consumer

