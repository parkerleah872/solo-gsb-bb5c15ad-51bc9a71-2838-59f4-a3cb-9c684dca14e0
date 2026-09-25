from __future__ import annotations

import os
from pathlib import Path

# SQLite 数据库文件路径,容器内默认 /data/samples.db,本地默认仓库下 data/samples.db
_DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "samples.db"
DB_PATH: str = os.getenv("SAMPLE_DB_PATH", str(_DEFAULT_DB))

# 备份根目录:容器内默认 /backups(docker-compose 挂载持久化卷),
# 本地默认仓库下 backups/;每次备份在其中原子发布为唯一子目录
_DEFAULT_BACKUPS = Path(__file__).resolve().parent.parent / "backups"
BACKUP_DIR: str = os.getenv("SAMPLE_BACKUP_DIR", str(_DEFAULT_BACKUPS))

# 余量审计时允许的浮点误差(绝对值)
BALANCE_TOLERANCE: float = float(os.getenv("BALANCE_TOLERANCE", "1e-6"))
