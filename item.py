class ItemSystem:
    def __init__(self, db):
        self.db = db
        self.init_items()
        
    def init_items(self):
        """初始化400+種素材和裝備"""
        # 素材類別
        materials = [
            # 綠色素材
            {"id": "mat_green_1", "name": "粗糙獸皮", "type": "material", "subtype": "leather", "rarity": "green", "level": 1, "value": 5},
            {"id": "mat_green_2", "name": "初級藥草", "type": "material", "subtype": "herb", "rarity": "green", "level": 1, "value": 3},
            
            # 藍色素材
            {"id": "mat_blue_1", "name": "堅韌獸皮", "type": "material", "subtype": "leather", "rarity": "blue", "level": 10, "value": 25},
            
            # 紫色素材
            {"id": "mat_purple_1", "name": "魔法獸皮", "type": "material", "subtype": "leather", "rarity": "purple", "level": 30, "value": 100},
            
            # 金色素材
            {"id": "mat_gold_1", "name": "龍鱗", "type": "material", "subtype": "scale", "rarity": "gold", "level": 50, "value": 500},
        ]
        
        # 裝備詞條庫
        self.affixes = {
            "明鏡止水": {"crit_chance": 0.15, "tier": "legendary"},
            "一心二用": {"crit_chance": 0.10, "tier": "epic"},
            "靈光一觸": {"crit_chance": 0.05, "tier": "rare"},
            "會心滅世": {"crit_damage": 0.50, "tier": "legendary"},
            "會心之魂": {"crit_damage": 0.30, "tier": "epic"},
            "會心一擊": {"crit_damage": 0.15, "tier": "rare"},
            "絕對領域": {"defense": 30, "tier": "legendary"},
            "絕對鐵壁": {"defense": 20, "tier": "epic"},
            "絕對防禦": {"defense": 10, "tier": "rare"},
        }
        
    def generate_equipment(self, rarity: str, slot: str, base_level: int):
        """隨機生成裝備"""
        equipment = {
            "id": f"equip_{random.randint(10000, 99999)}",
            "type": "equipment",
            "subtype": slot,
            "rarity": rarity,
            "level": base_level,
        }
        
        # 根據稀有度決定詞條數量
        affix_counts = {"green": 2, "blue": 3, "purple": 4, "gold": 6}
        
        # 添加基礎屬性
        stats = ["stamina", "speed", "strength", "intelligence"]
        selected_stats = random.sample(stats, affix_counts[rarity])
        
        for stat in selected_stats:
            # 根據稀有度決定數值範圍
            value_ranges = {
                "green": (1, 5),
                "blue": (3, 8),
                "purple": (5, 12),
                "gold": (8, 20)
            }
            min_val, max_val = value_ranges[rarity]
            equipment[f"{stat}_bonus"] = random.randint(min_val, max_val)
            
        # 添加特殊詞條（只有藍裝以上）
        if rarity in ["blue", "purple", "gold"]:
            available_affixes = [k for k, v in self.affixes.items() if v["tier"] in self.get_affix_tiers_for_rarity(rarity)]
            if available_affixes:
                affix = random.choice(available_affixes)
                equipment["special_affix"] = affix
                
        return equipment
    
    def get_affix_tiers_for_rarity(self, rarity: str):
        """根據裝備稀有度決定可用的詞條等級"""
        rarity_to_tiers = {
            "green": ["common"],
            "blue": ["common", "rare"],
            "purple": ["common", "rare", "epic"],
            "gold": ["common", "rare", "epic", "legendary"]
        }
        return rarity_to_tiers.get(rarity, ["common"])
