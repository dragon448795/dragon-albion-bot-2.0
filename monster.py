class MonsterSystem:
    def __init__(self, db):
        self.db = db
        self.init_monsters()
        
    def init_monsters(self):
        """初始化100+種怪物"""
        # 這裡我會建立基礎的怪物模板
        base_monsters = []
        
        # 地圖1：幽暗森林 (1-10層)
        forest_monsters = [
            {"id": "f1_green", "name": "綠史萊姆", "color": "green", "level": 1, "hp": 50, "attack": 5, "defense": 2, "speed": 3},
            {"id": "f1_blue", "name": "藍史萊姆", "color": "blue", "level": 3, "hp": 80, "attack": 8, "defense": 4, "speed": 4},
            {"id": "f1_purple", "name": "紫史萊姆", "color": "purple", "level": 5, "hp": 120, "attack": 12, "defense": 6, "speed": 5},
            {"id": "f1_gold", "name": "黃金史萊姆", "color": "gold", "level": 7, "hp": 200, "attack": 18, "defense": 8, "speed": 6},
        ]
        
        # 添加更多怪物...
        
    def get_monster_for_floor(self, map_id: str, floor: int, party_level: int = 1):
        """根據樓層取得適合的怪物"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        # 根據隊伍等級調整怪物強度
        level_range = max(1, floor * 3)
        
        cursor.execute('''
        SELECT * FROM monsters 
        WHERE map_id = ? AND floor = ? AND level <= ?
        ORDER BY RANDOM() LIMIT 1
        ''', (map_id, floor, level_range))
        
        monster = cursor.fetchone()
        conn.close()
        
        return dict(monster) if monster else None
