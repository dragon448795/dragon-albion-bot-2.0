# rpg/__init__.py
"""
RPG 系統主模組
"""

from .player import RPGPlayerSystem
from .inventory import RPGInventorySystem
from .combat import RPGCombatSystem
from .exploration import RPGExplorationSystem
from .items import RPGItemSystem

class RPGSYSTEM:
    def __init__(self, db):
        self.db = db
        self.player = RPGPlayerSystem(db)
        self.inventory = RPGInventorySystem(db)
        self.combat = RPGCombatSystem(db)
        self.exploration = RPGExplorationSystem(db)
        self.items = RPGItemSystem(db)
    
    async def initialize(self):
        """初始化 RPG 系統"""
        print("🎮 初始化 RPG 系統...")
        # 這裡可以添加系統初始化邏輯
        return True
