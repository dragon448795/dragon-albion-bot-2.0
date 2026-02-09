# rpg_core.py
"""
小雲 RPG 核心整合系統
將五個階段的模組整合在一起
"""
import sqlite3
from typing import Dict, List, Optional, Tuple
import random
from datetime import datetime

class RPGCore:
    """RPG 核心系統 - 整合所有模組"""
    
    def __init__(self, db_path="rpg_game.db"):
        # 初始化所有系統
        from database import RPGDatabase
        from monsters import MonsterSystem
        from items import ItemSystem
        from combat import CombatSystem
        from party import PartySystem
        
        print("🎮 初始化 RPG 核心系統...")
        
        # 核心資料庫
        self.db = RPGDatabase(db_path)
        
        # 各系統模組
        self.monsters = MonsterSystem(self.db)
        self.items = ItemSystem(self.db)
        self.combat = CombatSystem(self.db)
        self.party = PartySystem(self.db)
        
        # 遊戲狀態
        self.active_parties = {}  # 活動中的隊伍
        self.active_battles = {}  # 進行中的戰鬥
        self.player_sessions = {}  # 玩家會話狀態
        
        print("✅ RPG 核心系統初始化完成")
    
    # ========== 玩家系統方法 ==========
    def create_player(self, user_id: str, name: str) -> Dict:
        """創建新玩家"""
        success = self.db.create_player(user_id, name)
        if success:
            player = self.db.get_player(user_id)
            return {
                "success": True,
                "player": player,
                "message": f"角色 {name} 創建成功！"
            }
        return {
            "success": False,
            "message": "創建角色失敗"
        }
    
    def get_player_status(self, user_id: str) -> Dict:
        """取得玩家完整狀態"""
        player = self.db.get_player(user_id)
        if not player:
            return None
        
        # 獲取裝備
        equipment = self.db.get_player_equipment(user_id)
        
        # 獲取背包
        inventory = self.db.get_player_inventory(user_id)
        
        # 計算總屬性
        total_stats = self._calculate_total_stats(player, equipment)
        
        return {
            "player": player,
            "equipment": equipment,
            "inventory": inventory,
            "total_stats": total_stats,
            "party": self.party.get_user_party(user_id)
        }
    
    def _calculate_total_stats(self, player: Dict, equipment: List) -> Dict:
        """計算總屬性（基礎+裝備）"""
        stats = {
            "max_hp": player["max_hp"],
            "max_mp": player["max_mp"],
            "attack": 0,
            "defense": 0,
            "stamina": player["stamina"],
            "speed": player["speed"],
            "strength": player["strength"],
            "intelligence": player["intelligence"],
            "carry_capacity": player["carry_capacity"],
            "crit_chance": 0.05,  # 基礎5%
            "crit_damage": 1.5,   # 基礎150%
        }
        
        # 添加裝備加成
        for item in equipment:
            if item:
                stats["max_hp"] += item.get("hp_bonus", 0)
                stats["max_mp"] += item.get("mp_bonus", 0)
                stats["attack"] += item.get("attack_bonus", 0)
                stats["defense"] += item.get("defense_bonus", 0)
                stats["stamina"] += item.get("stamina_bonus", 0)
                stats["speed"] += item.get("speed_bonus", 0)
                stats["strength"] += item.get("strength_bonus", 0)
                stats["intelligence"] += item.get("intelligence_bonus", 0)
                stats["carry_capacity"] += item.get("capacity_bonus", 0)
                stats["crit_chance"] += item.get("critical_chance_bonus", 0)
                stats["crit_damage"] += item.get("critical_damage_bonus", 0)
        
        return stats
    
    # ========== 冒險系統方法 ==========
    def start_adventure(self, user_id: str, map_id: str = "forest") -> Dict:
        """開始冒險"""
        player = self.db.get_player(user_id)
        if not player:
            return {"success": False, "message": "找不到角色"}
        
        # 檢查是否在隊伍中
        party_info = self.party.get_user_party(user_id)
        
        if party_info:
            # 隊伍冒險
            return self._start_party_adventure(party_info["party_id"], map_id)
        else:
            # 單人冒險
            return self._start_solo_adventure(user_id, map_id)
    
    def _start_solo_adventure(self, user_id: str, map_id: str) -> Dict:
        """開始單人冒險"""
        # 隨機遭遇怪物
        monster = self.monsters.get_monster_for_floor(map_id, 1)
        
        if not monster:
            return {"success": False, "message": "該區域沒有怪物"}
        
        # 創建戰鬥
        battle_id = f"battle_solo_{user_id}_{int(datetime.now().timestamp())}"
        
        self.active_battles[battle_id] = {
            "battle_id": battle_id,
            "type": "solo",
            "player_id": user_id,
            "monster": monster,
            "monster_hp": monster["hp"],
            "player_hp": self.db.get_player(user_id)["hp"],
            "turn": 0,
            "status": "active",
            "actions": {},
            "start_time": datetime.now()
        }
        
        return {
            "success": True,
            "battle_id": battle_id,
            "message": f"遭遇 {monster['name']}！",
            "monster": monster,
            "type": "solo"
        }
    
    def _start_party_adventure(self, party_id: str, map_id: str) -> Dict:
        """開始隊伍冒險"""
        # 檢查隊伍是否準備好
        if not self.party.is_party_ready(party_id):
            return {"success": False, "message": "隊伍成員未準備好"}
        
        # 隨機遭遇怪物（根據隊伍等級）
        party_level = self.party.get_party_average_level(party_id)
        monster = self.monsters.get_monster_for_floor(map_id, 1, party_level)
        
        if not monster:
            return {"success": False, "message": "該區域沒有怪物"}
        
        # 創建隊伍戰鬥
        battle_id = f"battle_party_{party_id}_{int(datetime.now().timestamp())}"
        
        # 獲取所有隊伍成員
        members = self.party.get_party_members(party_id)
        
        self.active_battles[battle_id] = {
            "battle_id": battle_id,
            "type": "party",
            "party_id": party_id,
            "members": [m["user_id"] for m in members],
            "monster": monster,
            "monster_hp": monster["hp"],
            "player_hp": {m["user_id"]: self.db.get_player(m["user_id"])["hp"] for m in members},
            "turn": 0,
            "status": "waiting",
            "actions": {},
            "start_time": datetime.now()
        }
        
        return {
            "success": True,
            "battle_id": battle_id,
            "message": f"隊伍遭遇 {monster['name']}！",
            "monster": monster,
            "type": "party",
            "members": members
        }
    
    # ========== 戰鬥系統方法 ==========
    def battle_action(self, battle_id: str, user_id: str, action: str, target: str = None) -> Dict:
        """戰鬥行動"""
        if battle_id not in self.active_battles:
            return {"success": False, "message": "戰鬥不存在或已結束"}
        
        battle = self.active_battles[battle_id]
        
        # 檢查玩家是否在戰鬥中
        if battle["type"] == "solo":
            if battle["player_id"] != user_id:
                return {"success": False, "message": "你不在這場戰鬥中"}
        else:  # party
            if user_id not in battle["members"]:
                return {"success": False, "message": "你不在這場戰鬥中"}
        
        # 記錄行動
        battle["actions"][user_id] = {
            "action": action,
            "target": target,
            "time": datetime.now()
        }
        
        # 檢查是否所有玩家都已行動
        if battle["type"] == "party":
            all_acted = all(member in battle["actions"] for member in battle["members"])
            if all_acted:
                return self._resolve_battle_turn(battle_id)
        
        return {"success": True, "message": "行動已記錄", "waiting": battle["type"] == "party"}
    
    def _resolve_battle_turn(self, battle_id: str) -> Dict:
        """處理戰鬥回合"""
        battle = self.active_battles[battle_id]
        
        # 這裡實現戰鬥邏輯
        # 1. 計算傷害
        # 2. 更新HP
        # 3. 檢查戰鬥是否結束
        
        # 簡化版：直接結束戰鬥
        result = {
            "success": True,
            "battle_id": battle_id,
            "message": "戰鬥回合結束",
            "battle_over": True,
            "victory": True,
            "rewards": {
                "exp": 50,
                "gold": 30,
                "items": []
            }
        }
        
        # 刪除戰鬥記錄
        del self.active_battles[battle_id]
        
        return result
    
    # ========== 組隊系統方法 ==========
    def create_party(self, leader_id: str) -> Dict:
        """創建隊伍"""
        party_id = self.party.create_party(leader_id)
        return {
            "success": True,
            "party_id": party_id,
            "message": "隊伍創建成功",
            "invite_code": party_id[-6:]  # 邀請碼
        }
    
    def join_party(self, party_id: str, user_id: str) -> Dict:
        """加入隊伍"""
        success, message = self.party.join_party(party_id, user_id)
        return {
            "success": success,
            "message": message,
            "party_id": party_id if success else None
        }
    
    def get_party_info(self, party_id: str) -> Dict:
        """取得隊伍信息"""
        info = self.party.get_party_info(party_id)
        if not info:
            return {"success": False, "message": "隊伍不存在"}
        
        # 添加成員詳細信息
        members = []
        for member_id in info["members"]:
            player = self.db.get_player(member_id)
            if player:
                members.append({
                    "user_id": member_id,
                    "name": player["name"],
                    "level": player["level"],
                    "hp": player["hp"],
                    "max_hp": player["max_hp"]
                })
        
        info["member_details"] = members
        info["success"] = True
        
        return info
    
    # ========== 物品系統方法 ==========
    def get_player_inventory(self, user_id: str) -> List:
        """取得玩家背包"""
        return self.db.get_player_inventory(user_id)
    
    def use_item(self, user_id: str, item_id: str) -> Dict:
        """使用物品"""
        # 這裡實現物品使用邏輯
        return {"success": False, "message": "功能開發中"}
    
    def craft_item(self, user_id: str, recipe_id: str, materials: List) -> Dict:
        """製作物品"""
        # 這裡實現製作邏輯
        return {"success": False, "message": "功能開發中"}
