class CombatSystem:
    def __init__(self, db):
        self.db = db
        self.active_battles = {}  # {party_id: battle_data}
        
    def start_battle(self, party_id: str, monster_data: dict):
        """開始戰鬥"""
        battle_id = f"battle_{party_id}_{datetime.now().timestamp()}"
        
        battle_data = {
            "battle_id": battle_id,
            "party_id": party_id,
            "monster": monster_data,
            "turn": 0,
            "actions": {},
            "status": "waiting",
            "start_time": datetime.now(),
            "timeout": 120  # 2分鐘超時
        }
        
        self.active_battles[party_id] = battle_data
        return battle_data
    
    def process_turn(self, party_id: str):
        """處理回合行動"""
        battle = self.active_battles.get(party_id)
        if not battle:
            return None
            
        # 檢查是否所有玩家都已行動或超時
        current_time = datetime.now()
        time_passed = (current_time - battle["start_time"]).seconds
        
        if time_passed > battle["timeout"]:
            # 處理超時（當作防禦）
            return self.resolve_timeout(battle)
            
        # 執行戰鬥計算
        return self.resolve_actions(battle)
    
    def player_action(self, party_id: str, user_id: str, action: str, target: str = None):
        """玩家行動"""
        battle = self.active_battles.get(party_id)
        if not battle:
            return False
            
        battle["actions"][user_id] = {
            "action": action,
            "target": target,
            "time": datetime.now()
        }
        
        return True
    
    def calculate_damage(self, attacker_stats, defender_stats, skill_multiplier=1.0):
        """計算傷害"""
        base_damage = attacker_stats.get("attack", 0) * skill_multiplier
        defense = defender_stats.get("defense", 0)
        
        # 減傷計算
        damage_reduction = defense / (defense + 100)
        actual_damage = base_damage * (1 - damage_reduction)
        
        # 暴擊檢查
        crit_chance = attacker_stats.get("crit_chance", 0.05)
        if random.random() < crit_chance:
            crit_damage = attacker_stats.get("crit_damage", 1.5)
            actual_damage *= crit_damage
            
        return max(1, int(actual_damage))
